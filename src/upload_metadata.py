"""
Define file metadata
"""

from pydantic import BaseModel

from src.file_utils import create_file_metadata_from_bytes


class MyFile(BaseModel):
    file_name: str
    file_content: bytes
    uploader: str = "default"


def create_file_upload_metadata(file: MyFile) -> dict:
    file_metadata = create_file_metadata_from_bytes(
        file_bytes=file.file_content,
        file_name=file.file_name,
    )
    file_metadata.update(dict(uploader=file.uploader))
    return file_metadata
