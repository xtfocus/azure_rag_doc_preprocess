import asyncio
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile
from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.pipeline import MyFile

from .globals import clients, objects

router = APIRouter()

# Shared results store (use a more robust storage mechanism in production)
background_results = {}


@router.post("/api/exec/uploads/")
async def process_files(files: List[UploadFile]):
    """
    Process multiple uploaded files asynchronously.

    Args:
        files: List of uploaded files from the client.

    Returns:
        List of results for each processed file.
    """

    pipeline = objects["pipeline"]

    objects["duplicate-checker"]._ensure_container_exists()

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


@router.post("/api/exec/blob_container/")
async def process_container(
    container_name: str,
    background_tasks: BackgroundTasks,
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


async def process_blob(
    blob_name: str,
    blob_container_client,
    pipeline,
    duplicate_checker,
):
    try:
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
            return {"blob_name": blob_name, "result": result}

        else:
            logger.error(f"{blob_name} already processed. SKIPPING ... ")
            return {"blob_name": blob_name, "error": f"{blob_name} already processed."}
    except Exception as e:
        logger.error(f"Error processing blob '{blob_name}': {str(e)}")
        return {"blob_name": blob_name, "error": str(e)}
