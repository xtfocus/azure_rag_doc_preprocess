from typing import Dict, List

from src.models import FileText


def txt_extract_texts(file_content: bytes, encoding: str = "utf-8") -> Dict[str, List]:
    """
    Convert file_content bytes of a .txt file to a plain text string.

    Args:
        file_content (bytes): The content of the .txt file in bytes.
        encoding (str): The character encoding to decode the bytes. Defaults to 'utf-8'.

    Returns:
        str: The decoded text content of the file.
    """
    try:
        # Decode the bytes to a string using the specified encoding
        return {
            "texts": [FileText(text=file_content.decode(encoding), page_no=0)],
            "images": [],
        }
    except UnicodeDecodeError as e:
        raise ValueError(
            f"Failed to decode file_content. Ensure the encoding is correct. {e}"
        )
