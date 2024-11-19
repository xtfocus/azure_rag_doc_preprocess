import asyncio
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, UploadFile
from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.pipeline import MyFile

from .globals import clients, objects

router = APIRouter()


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
async def process_container(container_name: str):
    """
    Process all files in an Azure blob container asynchronously.

    Args:
        container_name: Name of the container to process

    Returns:
        List of processing results for each file
    """

    pipeline = objects["pipeline"]

    blob_container_client = AzureContainerClient(
        client=clients["blob_service_client"], container_name=container_name
    )

    # List all blobs in the container
    blob_names = blob_container_client.list_blob_names()

    # blob_names = ["36pact_ooizumi.pdf"]

    logger.info(f"Known: {objects['duplicate-checker'].known_dict}")

    async def process_blob(name: str):
        try:

            if not objects["duplicate-checker"].duplicate_by_file_name(name):
                # Download file content asynchronously using asyncio.to_thread for a blocking IO
                file_content = await asyncio.to_thread(
                    blob_container_client.download_file, name
                )

                # Create MyFile object
                file = MyFile(file_name=name, file_content=file_content)

                # Process the file using the pipeline
                result = await pipeline.process_file(file)

                objects["duplicate-checker"].update(file_name=name)

                objects["duplicate-checker"].save()
                return result

            else:
                raise ValueError(f"{name} already processed. Skipping...")

        except Exception as e:
            # Raise an HTTPException with details about the failed blob
            raise HTTPException(
                status_code=500, detail=f"Error processing blob '{name}': {str(e)}"
            )

    result: List = []
    for name in blob_names:
        res = await process_blob(name)
        result.append(res)
    # # Process blobs concurrently with semaphore to limit concurrency
    # semaphore = asyncio.Semaphore(5)  # Limit concurrent processing
    #
    # async def bounded_process_blob(name: str) -> Dict[str, Any]:
    #     """Process blob with concurrency limit"""
    #     async with semaphore:
    #         return await process_blob(name)
    #
    # # Create and gather tasks
    # tasks = [bounded_process_blob(name) for name in blob_names]
    # results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out and log exceptions
    final_results = []
    for index, result in enumerate(results):
        if isinstance(result, Exception):
            # Log the error here if needed
            logger.error(f"Error processing blob '{blob_names[index]}': {result}")
            final_results.append({"blob_name": blob_names[index], "error": str(result)})
        else:
            final_results.append(
                {"blob_name": blob_names[index], "result": str(result)}
            )

    return final_results
