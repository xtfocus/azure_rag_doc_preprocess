"""
Helpers to define file metadata
"""

from src.file_utils import create_file_metadata_from_bytes
from src.models import MyFile, MyFileMetaData


def create_file_upload_metadata(file: MyFile) -> MyFileMetaData:
    """
    Create basic metadata
    Add uploader as part of metadata
    """
    file_metadata = create_file_metadata_from_bytes(
        file_bytes=file.file_content,
        file_name=file.file_name,
    )
    file_metadata.update(dict(uploader=file.uploader))
    return MyFileMetaData.model_validate(file_metadata)
