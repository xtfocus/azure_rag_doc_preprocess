"""
File: azure_container_client.py
Desc: handling I/O tasks with Blob Storage for a specfic container

"""

import base64
from abc import ABC
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from loguru import logger


class BaseAzureContainerClient(ABC):
    """
    Abstract base class defining interface for Azure Blob Storage container operations
    """

    def __init__(
        self,
        client: BlobServiceClient,
        container_name: str = "default_container",
    ):
        """
        Initialize the base Azure container client.

        Args:
            client (BlobServiceClient): Azure Blob Storage service client
            container_name (str): Name of the container to manage
        """
        self.client: BlobServiceClient = client
        self.container_name: str = container_name
        logger.info(f"Making sure container {container_name} exists ...")
        self._ensure_container_exists()

    def list_blob_names(self) -> List[str]:
        return list(
            self.client.get_container_client(self.container_name).list_blob_names()
        )

    def _ensure_container_exists(self) -> None:
        """Check if the container exists and create it if not."""
        container_client: ContainerClient = self.client.get_container_client(
            self.container_name
        )
        logger.info(f"Check on {self.container_name}")
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"Container '{self.container_name}' created.")
        else:
            logger.info(f"Container '{self.container_name}' already exists.")

    def download_file(self, blob_name: str) -> Optional[bytes]:
        """Download a file from the container.

        Args:
            blob_name (str): The name of the blob to download.

        Returns:
            Optional[bytes]: The content of the blob if found, otherwise None.
        """
        try:
            container_client: ContainerClient = self.client.get_container_client(
                self.container_name
            )
            blob_client: BlobClient = container_client.get_blob_client(blob_name)
            result = blob_client.download_blob().readall()
            logger.info(f"Successfully downloaded blob {blob_name}")
            return result
        except Exception as e:
            logger.error(f"Error downloading blob '{blob_name}': {e}")
            return None

    # Add this method to BaseAzureContainerClient class
    def delete_file(self, blob_name: str) -> bool:
        """
        Delete a file from the container.

        Args:
            blob_name (str): The name of the blob to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        try:
            container_client: ContainerClient = self.client.get_container_client(
                self.container_name
            )
            blob_client: BlobClient = container_client.get_blob_client(blob_name)

            if blob_client.exists():
                blob_client.delete_blob()
                logger.info(f"Successfully deleted blob {blob_name}")
                return True
            else:
                logger.warning(f"Blob {blob_name} does not exist")
                return False

        except Exception as e:
            logger.error(f"Error deleting blob '{blob_name}': {e}")
            return False


class AzureContainerClient(BaseAzureContainerClient):
    """
    Wrapper for BlobServiceClient to work on a single container
    """

    def __init__(
        self,
        client: BlobServiceClient,
        container_name: str = "default_container",
    ):
        """
        Initialize the Azure container client with a specified container name.

        Args:
            container_name (str): Name of the container to manage. Defaults to "default_container".
        """
        self.client: BlobServiceClient = client
        self.container_name: str = container_name
        logger.info(f"Making sure container {container_name} exists ...")
        self._ensure_container_exists()

    def list_pdf_files(self) -> List[str]:
        """List all PDF files in the container."""
        container_client: ContainerClient = self.client.get_container_client(
            self.container_name
        )
        return [
            blob.name
            for blob in container_client.list_blobs()
            if blob.name.endswith(".pdf")
        ]

    async def upload_base64_image_to_blob(
        self,
        blob_names: Iterable[str],
        base64_images: Iterable[str],
        metadata: Dict[str, str] = dict(),
    ):
        container_client = self.client.get_container_client(self.container_name)

        if metadata:
            # URL encode both keys and values in metadata
            encoded_metadata = {
                quote(k): quote(v) if isinstance(v, str) else str(v)
                for k, v in metadata.items()
            }
        else:
            encoded_metadata = None

        # logger.debug(encoded_metadata)
        try:
            count = 0

            for blob_name, base64_image in zip(blob_names, base64_images):

                image_data = base64.b64decode(base64_image)
                # URL encode the blob name
                encoded_blob_name = quote(blob_name)

                # Get blob client with encoded name
                blob_client: BlobClient = container_client.get_blob_client(
                    encoded_blob_name
                )

                # Upload with encoded metadata
                blob_client.upload_blob(
                    image_data,
                    overwrite=True,
                    content_type="image/png",
                    metadata=encoded_metadata,
                )
                count += 1

                # logger.debug(f"Successfully uploaded blob: {blob_name}")

            logger.debug(f"Successfully uploaded all {count} image blobs")

        except Exception as e:
            logger.error(f"Upload images error: {str(e)}")
            raise

        return
