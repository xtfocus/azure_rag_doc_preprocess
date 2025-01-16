import asyncio
import os
from typing import Dict

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.models import MyFile, UserRemoveRequest, UserUploadRequest

from .globals import clients, objects

router = APIRouter()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")


async def send_webhook_notification(
    username: str, file_name: str, status: str, result: Dict = None
):
    """Send webhook notification about file processing status."""
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


async def process_user_file_background(
    username: str, file_name: str, file_content: bytes, pipeline
):
    """Background task to process a single file."""
    try:
        my_file = MyFile(
            file_name=file_name, file_content=file_content, uploader=username
        )

        # Process the file using the pipeline
        result = await pipeline.process_file(my_file)

        # Send completion webhook
        await send_webhook_notification(
            username=username, file_name=file_name, status="READY", result=result
        )

        return {"file_name": file_name, "result": result}

    except Exception as e:
        error = f"Error processing file '{file_name}': {str(e)}"
        await send_webhook_notification(
            username=username,
            file_name=file_name,
            status="ERROR",
            result={"error": str(e)},
        )
        logger.error(error)
        raise
        return {"file_name": file_name, "error": str(e)}


@router.post("/api/exec/user_upload/")
async def process_user_file(
    user_upload_request: UserUploadRequest, background_tasks: BackgroundTasks
):
    """
    Process a user upload file request
    """

    await send_webhook_notification(
        username=user_upload_request.username,
        file_name=user_upload_request.blob_name,
        status="IN_PROGRESS",
    )

    response = []
    try:
        # Define client
        blob_container_client = AzureContainerClient(
            client=clients["blob_service_client"],
            container_name=user_upload_request.container_name,
        )
        file_content = await asyncio.to_thread(
            blob_container_client.download_file, user_upload_request.blob_name
        )

        pipeline = objects["pipeline"]

        # Add background task for processing
        background_tasks.add_task(
            process_user_file_background,
            user_upload_request.username,
            user_upload_request.blob_name,
            file_content,
            pipeline,
        )

        response.append(
            {
                "username": user_upload_request.username,
                "file_name": user_upload_request.blob_name,
                "status": "processing_initiated",
            }
        )

    except Exception as e:
        error = f"Error initiating processing for file '{user_upload_request.blob_name}': {str(e)}"
        logger.error(error)
        response.append(
            {
                "file_name": user_upload_request.blob_name,
                "status": "initiation_failed",
                "error": str(e),
            }
        )

    return {"tasks": response}


async def remove_file(search_client, filter_expr: str) -> dict:
    """
    Remove all documents from Azure Search using a filter
    """
    try:
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
            logger.warning(f"No documents found with filter {filter_expr}")
            return {
                "index": search_client._index_name,
                "status": "COMPLETED",
                "documents_removed": 0,
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

        logger.info(
            f"Successfully removed {len(chunk_ids)} documents with filter {filter_expr}"
        )
        return {
            "index": search_client._index_name,
            "status": "COMPLETED",
            "documents_removed": len(chunk_ids),
        }

    except Exception as e:
        error_msg = f"Error removing documents for file '{file_name}': {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/api/exec/user_remove/")
async def remove_user_file(user_remove_request: UserRemoveRequest):
    """
    Remove user's file in the following order:
    - remove images associated with file
    - remove Azure Search entries associated with file
    - remove file from Blob Storage

    Do not use webhook here
    """
    search_clients = [
        clients["text-azure-ai-search"],
        clients["image-azure-ai-search"],
        clients["summary-azure-ai-search"],
    ]

    results = []
    total_removed = 0
    deleted_blobs = []

    filter_expr = f"(uploader eq '{user_remove_request.username}') and (title eq '{user_remove_request.blob_name}')"

    # First handle the image files for the image search client
    image_search_client = clients["image-azure-ai-search"]
    image_container_client = clients["image_container_client"]

    try:
        # Get all chunk_ids from the image search results before any deletion
        image_search_results = await asyncio.to_thread(
            image_search_client.search,
            search_text="*",
            filter=filter_expr,
            select=["chunk_id"],
        )
        chunk_ids = []
        for doc in image_search_results:
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
            "file_name": user_remove_request.blob_name,
            "overall_status": "error",
            "error": error_msg,
            "stage": "image_deletion",
        }

    # Then process each search client
    for client in search_clients:
        try:
            result = await remove_file(client, filter_expr)
            results.append(result)
            total_removed += result["documents_removed"]

        except Exception as e:
            logger.error(f"Error with client {client._index_name}: {str(e)}")
            results.append(
                {
                    "index": client._index_name,
                    "filter_expr": filter_expr,
                    "status": "ERROR",
                    "error": str(e),
                    "documents_removed": 0,
                }
            )

    return {
        "username": user_remove_request.username,
        "file_name": user_remove_request.blob_name,
        "overall_status": "COMPLETED",
        "total_documents_removed": total_removed,
        "client_results": results,
        "deleted_image_files": {"count": len(deleted_blobs), "files": deleted_blobs},
    }
