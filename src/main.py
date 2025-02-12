import asyncio
from collections.abc import Iterable
from typing import Dict

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.models import FileIndexingRequest, MyFile
from src.pdf_utils.pdf_utils import pdf_blob_to_pdfplumber_doc

from .globals import clients, configs, objects

router = APIRouter()

# Shared results store (use a more robust storage mechanism in production)
background_results = {}


async def send_webhook_notification(
    username: str, file_name: str, status: str, result: Dict = None
):
    """Send webhook notification about file processing status."""

    WEBHOOK_URL = configs["app_config"].WEBHOOK_URL
    if not WEBHOOK_URL:
        logger.warning("WEBHOOK_URL not configured, skipping notification")
        return

    payload = {
        "preferredUsername": username,
        "blobName": file_name,
        "status": status,
        "departmentId": 0,
    }

    logger.info(f"Sending payload\n{payload}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.put(WEBHOOK_URL, json=payload)
            logger.debug(response)
            response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send webhook notification for {file_name}: {str(e)}\n")


@router.post("/api/exec/reindex/")
async def reindex_file(
    indexing_request: FileIndexingRequest,
    background_tasks: BackgroundTasks,
):
    """
    Reindex a single file from a specified Azure Blob Storage container in the background.

    Args:
        container_name: The name of the Azure Blob Storage container.
        file_name: The name of the file to reindex.
        background_tasks: FastAPI BackgroundTasks instance.

    Returns:
        A message indicating the background task has started.
    """
    blob_container_client = AzureContainerClient(
        client=clients["blob_service_client"],
        container_name=indexing_request.blob_container_name,
    )

    # Add the reindex task to background tasks
    background_tasks.add_task(
        reindex_file_background,
        indexing_request.blob_container_name,
        indexing_request.file_name,
        indexing_request.uploader,
        indexing_request.dept_name,
        blob_container_client,
        objects["pipeline"],
    )

    await send_webhook_notification(
        username="default",
        file_name=indexing_request.file_name,
        status="IN_PROGRESS",
    )
    return {
        "message": f"Reindexing of file '{indexing_request.file_name}' in container '{indexing_request.blob_container_name}' started."
    }


async def reindex_file_background(
    container_name: str,
    file_name: str,
    uploader: str,
    dept_name: str,
    blob_container_client,
    pipeline,
):
    """
    Background task to reindex a single file from an Azure Blob Storage container.

    Args:
        container_name: Name of the Azure Blob Storage container.
        file_name: Name of the file to reindex.
        blob_container_client: Azure container client instance.
        pipeline: The processing pipeline instance.
    """
    try:

        # Download file content asynchronously
        file_content = await asyncio.to_thread(
            blob_container_client.download_file, file_name
        )

        # Create a MyFile instance
        file = MyFile(
            file_name=file_name,
            file_content=file_content,
            dept_name=dept_name,
            uploader=uploader,
        )

        # Process the file
        result = await pipeline.process_file(file)

        # Log success
        logger.info(
            f"Reindexing complete for file '{file_name}' in container '{container_name}': {result}"
        )
        await send_webhook_notification(
            username="default", file_name=file_name, status="READY", result=result
        )
    except Exception as e:
        # Log the error
        logger.error(
            f"Error during reindexing of file '{file_name}' in container '{container_name}': {str(e)}"
        )
        raise


async def search_client_filter_file(file_name: str, search_client) -> Iterable:
    """ """
    # Get file name without extension for title matching
    title = file_name
    filter_expr = f"title eq '{title}'"
    search_results = await asyncio.to_thread(
        search_client.search,
        search_text="*",  # Get all documents
        filter=filter_expr,  # Exact match using OData filter
        select=["chunk_id", "chunk", "metadata"],  # Only get chunk_ids for efficiency
    )

    return list(search_results)


async def remove_file(
    file_name: str, search_client, use_parent_id: bool = False
) -> dict:
    """
    Remove all documents from Azure Search where either:
    - title exactly matches the file name (without extension), or
    - parent_id exactly matches the file name (with extension)

    Args:
        file_name: Name of the file to remove (with extension)
        search_client: Azure Search client instance
        use_parent_id: If True, filter by parent_id instead of title

    Returns:
        dict: Result of the removal operation including number of documents removed
    """
    try:
        if use_parent_id:
            # Use the full file name as parent_id
            filter_expr = f"parent_id eq '{file_name}'"
            search_term = file_name
        else:
            # Get file name without extension for title matching
            title = file_name
            filter_expr = f"title eq '{title}'"
            search_term = title

        # Search for documents with exact match using OData filter
        search_results = await asyncio.to_thread(
            search_client.search,
            search_text="*",  # Get all documents
            filter=filter_expr,  # Exact match using OData filter
            select=["chunk_id"],  # Only get chunk_ids for efficiency
        )

        # Collect all chunk_ids
        chunk_ids = []
        for result in search_results:
            chunk_ids.append(result["chunk_id"])

        if not chunk_ids:
            field_type = "parent_id" if use_parent_id else "title"
            logger.warning(f"No documents found with {field_type} '{search_term}'")
            return {
                "file_name": file_name,
                "status": "no_documents_found",
                "documents_removed": 0,
                "filter_type": field_type,
            }

        # Delete documents in batches
        batch_size = 1000  # Azure Search limitation
        for i in range(0, len(chunk_ids), batch_size):
            batch = chunk_ids[i : i + batch_size]
            await asyncio.to_thread(
                search_client.delete_documents,
                documents=[
                    {"@search.action": "delete", "chunk_id": chunk_id}
                    for chunk_id in batch
                ],
            )

        field_type = "parent_id" if use_parent_id else "title"
        logger.info(
            f"Successfully removed {len(chunk_ids)} documents for file '{file_name}' using {field_type} filter"
        )
        return {
            "file_name": file_name,
            "status": "success",
            "documents_removed": len(chunk_ids),
            "filter_type": field_type,
        }

    except Exception as e:
        error_msg = f"Error removing documents for file '{file_name}': {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/api/exec/remove_file/")
async def remove_file_endpoint(
    file_name: str,
    use_parent_id: bool = False,
):
    """
    Remove all documents associated with a file from multiple Azure Search clients
    and delete associated image files if present.

    Args:
        file_name: Name of the file whose documents should be removed
        use_parent_id: If True, filter by parent_id instead of title (here title and file name is the same)

    Returns:
        Result of the removal operation from all search clients and blob storage
    """
    search_clients = [
        clients["text-azure-ai-search"],
        clients["image-azure-ai-search"],
        clients["summary-azure-ai-search"],
    ]

    results = []
    total_removed = 0
    deleted_blobs = []

    # First handle the image files for the image search client
    image_search_client = clients["image-azure-ai-search"]
    image_container_client = clients["image_container_client"]

    try:
        # Get all chunk_ids from the image search results before any deletion
        search_results = await asyncio.to_thread(
            image_search_client.search,
            search_text="*",
            filter=(
                f"parent_id eq '{file_name}'"
                if use_parent_id
                else f"title eq '{file_name}'"
            ),
            select=["chunk_id"],
        )

        # Delete all associated image files first
        chunk_ids = []
        for doc in search_results:
            chunk_ids.append(doc["chunk_id"])
            blob_name = f"{doc['chunk_id']}"  # Assuming blob name matches chunk_id

            if await asyncio.to_thread(image_container_client.delete_file, blob_name):
                deleted_blobs.append(blob_name)
            else:
                logger.warning(f"Failed to delete image file: {blob_name}")

        logger.info(
            f"Deleted {len(deleted_blobs)} image files out of {len(chunk_ids)} found"
        )

    except Exception as e:
        error_msg = f"Error handling image files: {str(e)}"
        logger.error(error_msg)
        return {
            "file_name": file_name,
            "overall_status": "error",
            "error": error_msg,
            "stage": "image_deletion",
        }

    # Then process each search client
    for client in search_clients:
        try:
            result = await remove_file(file_name, client, use_parent_id)
            results.append(result)
            total_removed += result["documents_removed"]

        except Exception as e:
            logger.error(f"Error with client {client._index_name}: {str(e)}")
            results.append(
                {
                    "client": client._index_name,
                    "file_name": file_name,
                    "status": "error",
                    "error": str(e),
                    "documents_removed": 0,
                }
            )

    return {
        "file_name": file_name,
        "overall_status": "completed",
        "total_documents_removed": total_removed,
        "client_results": results,
        "deleted_image_files": {"count": len(deleted_blobs), "files": deleted_blobs},
    }


@router.get("/api/exec/get_file_entries/")
async def run_retrieve_by_file_name(file_name: str):
    tasks = {}
    # Define clients
    for client_name in [
        "text-azure-ai-search",
        "image-azure-ai-search",
        "summary-azure-ai-search",
    ]:

        logger.debug(f"Starting task for client: {client_name}")

        tasks[client_name] = asyncio.create_task(
            search_client_filter_file(file_name, clients[client_name])
        )

    # return await tasks["summary-azure-ai-search"]
    # Await all tasks and collect results
    completed_results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    # Create a result dictionary mapping client_name to task result
    result = {
        client_name: completed_results[idx]
        for idx, client_name in enumerate(tasks.keys())
    }

    for i, v in result.items():
        logger.info(f"Client {i}: Found {len(v)} results")

    return result


@router.get("/api/exec/get_pdf_file_metadata/")
async def get_file_metadata(container_name: str, file_name: str):
    # Define clients
    blob_container_client = AzureContainerClient(
        client=clients["blob_service_client"], container_name=container_name
    )

    # Download file content asynchronously
    file_content = await asyncio.to_thread(
        blob_container_client.download_file, file_name
    )
    return pdf_blob_to_pdfplumber_doc(file_content).metadata
