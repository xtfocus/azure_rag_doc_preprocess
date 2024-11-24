import asyncio
from typing import Any, Callable, List

from fastapi import (APIRouter, BackgroundTasks, Depends, HTTPException,
                     UploadFile)
from loguru import logger

from src import task_counter
from src.azure_container_client import AzureContainerClient
from src.pipeline import MyFile
from src.task_counter import TaskCounter

from .globals import clients, objects

router = APIRouter()

# Shared results store (use a more robust storage mechanism in production)
background_results = {}

task_counter = TaskCounter()


async def ensure_no_active_tasks():
    """
    Dependency that checks if there are any active background tasks.
    """

    if task_counter.is_busy:
        raise HTTPException(
            status_code=409,
            detail=f"There are {task_counter.active_tasks} background tasks still running. Please try again later.",
        )
    yield


def run_with_task_counter(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to wrap a function with task counter increment and decrement logic.
    """

    async def wrapper(*args, **kwargs):
        task_counter.increment()
        try:
            return await func(*args, **kwargs)
        finally:
            task_counter.decrement()

    return wrapper


@router.post("/api/exec/uploads/")
async def process_files(
    files: List[UploadFile], _: None = Depends(ensure_no_active_tasks)
):
    """
    Process multiple uploaded files asynchronously.

    Args:
        files: List of uploaded files from the client.

    Returns:
        List of results for each processed file.
    """

    pipeline = objects["pipeline"]

    objects["duplicate-checker"]._ensure_container_exists()

    @run_with_task_counter
    async def process_single_file(file: UploadFile):
        try:
            if not objects["duplicate-checker"].duplicate_by_file_name(file.filename):
                # Read file content asynchronously
                file_content = await file.read()

                my_file = MyFile(file_name=file.filename, file_content=file_content)

                # Process the file using the pipeline
                result = await pipeline.process_file(my_file)
                objects["duplicate-checker"].update(file_name=file.filename)

                return {"file_name": file.filename, "result": result}
            else:
                raise ValueError(f"{file.filename} already processed. Skipping...")

        except Exception as e:
            # Raise HTTPException for any errors
            raise HTTPException(
                status_code=500,
                detail=f"Error processing file '{file.filename}': {str(e)}",
            )

    # Create tasks for processing each file
    tasks = [process_single_file(file) for file in files]

    # Gather results for all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle results and exceptions
    final_results = []
    for index, result in enumerate(results):
        if isinstance(result, Exception):
            # Log or return the error for this file
            final_results.append(
                {"file_name": files[index].filename, "error": str(result)}
            )
        else:
            final_results.append(result)

    objects["duplicate-checker"].save()
    return final_results


@router.post("/api/exec/reindex/")
async def reindex_file(
    container_name: str,
    file_name: str,
    background_tasks: BackgroundTasks,
    _: None = Depends(ensure_no_active_tasks),
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
        client=clients["blob_service_client"], container_name=container_name
    )

    # Add the reindex task to background tasks
    background_tasks.add_task(
        reindex_file_background,
        container_name,
        file_name,
        blob_container_client,
        objects["pipeline"],
    )

    return {
        "message": f"Reindexing of file '{file_name}' in container '{container_name}' started."
    }


@run_with_task_counter
async def reindex_file_background(
    container_name: str,
    file_name: str,
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
        file = MyFile(file_name=file_name, file_content=file_content)

        # Process the file
        result = await pipeline.process_file(file)

        # Log success
        logger.info(
            f"Reindexing complete for file '{file_name}' in container '{container_name}': {result}"
        )
    except Exception as e:
        # Log the error
        logger.error(
            f"Error during reindexing of file '{file_name}' in container '{container_name}': {str(e)}"
        )


@router.post("/api/exec/blob_container/")
async def process_container(
    container_name: str,
    background_tasks: BackgroundTasks,
    _: None = Depends(ensure_no_active_tasks),
):
    """
    Start processing all files in an Azure blob container in the background.

    Args:
        container_name: Name of the container to process.

    Returns:
        A message indicating the task has been started.
    """
    pipeline = objects["pipeline"]
    blob_container_client = AzureContainerClient(
        client=clients["blob_service_client"], container_name=container_name
    )
    duplicate_checker = objects["duplicate-checker"]

    duplicate_checker._ensure_container_exists()

    # Launch the background task
    background_tasks.add_task(
        process_all_blobs,
        container_name,
        blob_container_client,
        pipeline,
        duplicate_checker,
    )

    return {"message": f"Processing of container '{container_name}' started."}


@run_with_task_counter
async def process_all_blobs(
    container_name: str,
    blob_container_client,
    pipeline,
    duplicate_checker,
):
    blob_names = blob_container_client.list_blob_names()
    results = []
    for blob_name in blob_names:
        res = await process_blob(
            blob_name, blob_container_client, pipeline, duplicate_checker
        )
        results.append(res)

    background_results[container_name] = results

    logger.info(f"Processed all documents in {container_name}:\n {results}")


@run_with_task_counter
async def process_blob(
    blob_name: str,
    blob_container_client,
    pipeline,
    duplicate_checker,
):
    try:
        task_counter.increment()
        if not duplicate_checker.duplicate_by_file_name(blob_name):

            # Download file content asynchronously
            file_content = await asyncio.to_thread(
                blob_container_client.download_file, blob_name
            )
            file = MyFile(file_name=blob_name, file_content=file_content)

            # Process the file
            result = await pipeline.process_file(file)

            duplicate_checker.update(file_name=blob_name)
            duplicate_checker.save()
            task_counter.decrement()
            return {"blob_name": blob_name, "result": result}

        else:
            error = f"{blob_name} already processed. SKIPPING ... "
            raise ValueError(error)

    except Exception as e:
        error = f"Error processing {blob_name}: {str(e)}"
        logger.error(f"Error processing blob '{blob_name}': {str(e)}")
        task_counter.decrement()

        return {"blob_name": blob_name, "error": error}
