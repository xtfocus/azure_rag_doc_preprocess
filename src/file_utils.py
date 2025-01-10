import base64
import hashlib
import io
import os
from typing import List, Optional

import pdfplumber
from pdfplumber.page import Page
from PIL import Image


def page_extract_tables_md(
    page: pdfplumber.page.Page, preserve_linebreaks: bool = False
) -> list[str]:
    """
    Extract tables from a PDF page and convert them to markdown format.

    Args:
        page: A pdfplumber Page object
        preserve_linebreaks: If True, converts newlines to HTML <br> tags.
                           If False, replaces newlines with spaces.

    Returns:
        list[str]: List of tables in markdown format
    """
    markdown_tables = []

    # Extract tables from the page
    tables = page.extract_tables()

    for table in tables:
        if not table:  # Skip empty tables
            continue

        # Clean and normalize the data
        cleaned_table = []
        for row in table:
            cleaned_row = []
            for cell in row:
                if cell is None:
                    cleaned_cell = ""
                else:
                    # Convert to string and split into lines
                    lines = [line.strip() for line in str(cell).split("\n")]
                    # Remove empty lines
                    lines = [line for line in lines if line]

                    if preserve_linebreaks:
                        # Join with HTML line breaks
                        cleaned_cell = "<br>".join(lines)
                    else:
                        # Join with spaces
                        cleaned_cell = " ".join(lines)
                cleaned_row.append(cleaned_cell)
            cleaned_table.append(cleaned_row)

        # Calculate maximum width for each column
        col_widths = []
        for col in range(len(cleaned_table[0])):
            width = max(len(row[col]) for row in cleaned_table)
            col_widths.append(max(3, width))  # Minimum width of 3 for markdown syntax

        # Build the markdown table
        markdown = []

        # Header row
        header = (
            "|"
            + "|".join(
                cleaned_table[0][i].ljust(col_widths[i])
                for i in range(len(cleaned_table[0]))
            )
            + "|"
        )
        markdown.append(header)

        # Separator row
        separator = (
            "|"
            + "|".join("-" * col_widths[i] for i in range(len(cleaned_table[0])))
            + "|"
        )
        markdown.append(separator)

        # Data rows
        for row in cleaned_table[1:]:
            data_row = (
                "|"
                + "|".join(row[i].ljust(col_widths[i]) for i in range(len(row)))
                + "|"
            )
            markdown.append(data_row)

        markdown_tables.append("\n".join(markdown))

    return markdown_tables


def pdf_blob_to_pdfplumber_doc(blob: bytes) -> pdfplumber.PDF:
    """
    Converts a PDF byte blob into a pdfplumber PDF object.

    Args:
        blob (bytes): A byte blob representing a PDF file.

    Returns:
        pdfplumber.PDF: The pdfplumber PDF object created from the byte blob.
    """
    return pdfplumber.open(io.BytesIO(blob))


def extract_single_image(page: Page, image_obj: dict) -> Optional[Image.Image]:
    """
    Extracts a single image from the page given its image object.
    Converts it to RGB format if necessary and returns a PIL Image.
    Args:
        page (pdfplumber.page.Page): The page containing the image
        image_obj (dict): The image object dictionary from pdfplumber
    Returns:
        Optional[PIL.Image.Image]: A PIL Image object of the extracted image, or None if extraction fails
    """
    try:
        # Get raw image data
        raw_image = image_obj["stream"].get_data()

        # Create PIL Image from bytes
        img = Image.open(io.BytesIO(raw_image))

        # Convert to RGB if necessary
        if img.mode in ["RGBA", "LA"]:
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1])
            img = background
        elif img.mode not in ["RGB"]:
            img = img.convert("RGB")

        return img
    except Exception as e:
        logger.warning(f"Failed to extract image: {str(e)}")
        return None


def page_extract_images(page: Page) -> List[Image.Image]:
    """
    Extracts all images on a given page as PIL Image objects.
    Args:
        page (pdfplumber.page.Page): A single page of a pdfplumber document.
    Returns:
        List[PIL.Image.Image]: A list of PIL Image objects for each image on the page.
    """
    images = []

    # Extract images from the page
    for img_obj in page.images:
        pil_image = extract_single_image(page, img_obj)
        if pil_image is not None:
            images.append(pil_image)

    return images


def get_images_as_base64(page: Page) -> List[str]:
    """
    Converts all images on a given page to base64-encoded strings.
    Args:
        page (pdfplumber.page.Page): A single page of a pdfplumber document.
    Returns:
        List[str]: A list of base64-encoded strings, each representing an image on the page.
    """
    images_base64 = []
    images = page_extract_images(page)

    for img in images:
        # Convert PIL Image to PNG format in-memory
        img_buffer = io.BytesIO()
        img.save(img_buffer, format="PNG")

        # Encode PNG binary data as base64 string
        img_base64 = base64.b64encode(img_buffer.getvalue()).decode("utf-8")
        images_base64.append(img_base64)

    return images_base64


def create_file_metadata_from_path(file_path):
    """
    Create metadata for a document file.

    Parameters:
    - file_path (str): The file path to the PDF document.

    Returns:
    - dict: Metadata dictionary containing the document title, file name, and SHA-256 hash.
    """
    # Extract the file name without the directory path and extension
    title = os.path.splitext(os.path.basename(file_path))[0]
    file_name = os.path.basename(file_path)

    # Calculate SHA-256 hash to uniquely identify the file
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read the file in chunks to avoid memory overload with large files
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)

    # Generate the hash in hexadecimal format
    file_hash = sha256_hash.hexdigest()

    return {"title": title, "file": file_name, "file_hash": file_hash}


def create_file_metadata_from_bytes(file_bytes: bytes, file_name: str, title=None):
    """
    Create metadata for a document file using the file contents in bytes.

    Parameters:
    - file_bytes (bytes): The bytes content of the document file.
    - file_name (str): The file name of the document.
    - title (str, optional): The title of the document. If not provided, it will be inferred from the file_name.

    Returns:
    - dict: Metadata dictionary containing the document title, file name, and SHA-256 hash.
    """
    # If title is not provided, infer it from the file_name
    if title is None:
        title = os.path.splitext(file_name)[0]

    # Calculate SHA-256 hash to uniquely identify the file
    sha256_hash = hashlib.sha256()
    sha256_hash.update(file_bytes)
    file_hash = sha256_hash.hexdigest()

    return {"title": title, "file": file_name, "file_hash": file_hash}
