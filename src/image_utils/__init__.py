"""
image_file_parsing.py
Handle converting jpg and png files to {"images":List[FileImage]} format
"""

import base64
from typing import Dict, List

from src.models import FileImage


def image_file_extract(file_content: bytes) -> Dict[str, List]:
    """
    Convert the file content of a JPG/JPEG/PNG image to a Base64-encoded string.

    Args:
        file_content (bytes): The image file content in bytes.

    Returns:
        str: Base64-encoded string of the image.
    """
    # Encode the bytes to a Base64 string
    return {
        "images": [
            FileImage(
                image_no=0,
                page_no=0,
                image_base64=base64.b64encode(file_content).decode("utf-8"),
            )
        ],
        "texts": [],
    }
