import asyncio
import os
from collections.abc import Iterable
from typing import Dict, List

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.file_utils import pdf_blob_to_pymupdf_doc
from src.models import UserUploadRequest
from src.pipeline import MyFile

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
        return {"file_name": file_name, "error": str(e)}


@router.post("/api/exec/user_upload/")
async def process_user_file(
    user_upload_request: UserUploadRequest, background_tasks: BackgroundTasks
):
    """
    Process multiple uploaded files asynchronously in the background.

    Args:
        files: List of uploaded files from the client.
        background_tasks: FastAPI BackgroundTasks instance.

    Returns:
        Dictionary confirming task initiation for each file.
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
