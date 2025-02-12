import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from loguru import logger
from pydantic import BaseModel, Field


class MyFile(BaseModel):
    file_name: str
    file_content: bytes
    uploader: str = "default"
    dept_name: str = "default"


class FileText(BaseModel):
    """
    Represents a page of text
    """

    page_no: int
    text: str


class FileIndexingRequest(BaseModel):
    file_name: str
    blob_container_name: str
    uploader: str = "default"
    dept_name: str = "default"


class FileDeleteRequest(BaseModel):
    file_name: str
    blob_container_name: str
    uploader: str = "default"
    dept_name: str = "default"


class FileImage(BaseModel):
    """
    Represent an image
    """

    page_no: int
    image_no: int
    image_base64: str


@dataclass
class PageStats:
    """
    Document statistics on the number of pages grouped by
        whether they contain or not contain any texts or images
    """

    text_yes_image_yes: int = 0
    text_yes_image_no: int = 0
    text_no_image_yes: int = 0
    text_no_image_no: int = 0

    def update(self, has_text: bool, has_images: bool) -> None:
        if has_text and has_images:
            self.text_yes_image_yes += 1
        elif has_text:
            self.text_yes_image_no += 1
        elif has_images:
            self.text_no_image_yes += 1
        else:
            self.text_no_image_no += 1

    def log_summary(self, doc_metadata: dict) -> None:
        logger.info(f"File metadata: {doc_metadata}")
        logger.info(
            "\n"
            "|                    | Has Images         | No Images          |\n"
            "|--------------------|--------------------|--------------------|\n"
            f"| **Has Text**       | {self.text_yes_image_yes:>18} | {self.text_yes_image_no:>18} |\n"
            f"| **No Text**        | {self.text_no_image_yes:>18} | {self.text_no_image_no:>18} |"
        )


class CustomSkillException(Exception):
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        logger.error(
            f"CustomSkillException raised: {self.message}"
        )  # Log exception on creation
        super().__init__(self.message)


class RequestData(BaseModel):
    values: List[Dict]


class PageRange(BaseModel):
    """Represents the page range information for a document chunk"""

    start_page: int
    end_page: int


class BaseChunk(BaseModel):
    """Represents a single chunk of text with its metadata"""

    chunk_no: str
    chunk: str
    page_range: PageRange


class MyFileMetaData(BaseModel):
    """Represents the metadata for a file in Azure Search Vector DB"""

    file_hash: str
    title: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    uploader: str
    dept_name: str


class AzureSearchDocMetaData(BaseModel):
    """
    Represents a document in Azure Search index with all required fields
    """

    chunk_id: str = Field(description="Unique identifier for the chunk")
    metadata: str = Field(description="JSON serialized metadata")
    parent_id: str = Field(description="ID of the parent document")
    title: str = Field(description="Title of the document")
    uploader: str = Field(description="Uploader of the document")
    dept_name: str = Field(description="Department of the document")

    @classmethod
    def from_chunk(
        cls, chunk: BaseChunk, file_metadata: MyFileMetaData, prefix: str
    ) -> "AzureSearchDocMetaData":
        """
        Creates an AzureSearchDoc from a chunk and file metadata
        """
        try:
            return cls(
                chunk_id=f"{prefix}_{file_metadata.file_hash}_chunk_{chunk.chunk_no}",
                metadata=json.dumps({"page_range": chunk.page_range.dict()}),
                parent_id=file_metadata.file_hash,
                title=file_metadata.title,
                uploader=file_metadata.uploader,
                dept_name=file_metadata.dept_name,
            )
        except Exception as e:
            raise


class UserUploadRequest(BaseModel):
    username: str
    blob_name: str
    container_name: str


class UserRemoveRequest(BaseModel):
    username: str
    blob_name: str
    container_name: str
