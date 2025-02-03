import base64
import io
from typing import Dict, List

import pdfplumber
from loguru import logger
from pdfplumber.page import Page
from pdfplumber.pdf import PDF as Doc


def get_page_drawings_stats(page: Page) -> Dict[str, int]:
    """Count drawings by type: curve, line, quad, rectangle"""

    lines = page.lines
    hlines = [l for l in lines if l["y0"] == l["y1"]]
    vlines = [l for l in lines if l["x0"] == l["x1"]]
    return {
        "c": len(page.curves),
        "hl": len(hlines),
        "vl": len(vlines),
        "re": len(page.rects),
    }


def is_infographic_page(page: Page) -> bool:
    """Check if page contains multiple visual components"""
    stats = get_page_drawings_stats(page)
    n_elements = sum(v for k, v in stats.items() if k in ("vl", "c"))
    n_elements += len(page.images)
    return n_elements >= 9


def doc_exported_from_ppt(pdf: Doc) -> bool:
    """Return True if pdf document is a PowerPoint export"""
    metadata = pdf.metadata
    return any(
        "PowerPoint" in metadata.get(field, "") for field in ["Creator", "Producer"]
    )


def page_to_base64(page: Page, format: str = "PNG", scale: int = 2) -> str:
    """Convert whole page to base64 image"""
    # Convert page to image using pdfplumber's native method
    img = page.to_image(resolution=72 * scale)

    # Get the image as bytes
    img_buffer = io.BytesIO()
    img.original.save(img_buffer, format=format)

    return base64.b64encode(img_buffer.getvalue()).decode()


def pdf_page_is_landscape(page: Page, ratio=1.2) -> bool:
    """
    Determines if a given pdfplumber.page.Page is in landscape layout.

    Args:
        page (Page): The PDF page to check.

    Returns:
        bool: True if the page layout is landscape, False otherwise.
    """
    # Retrieve the page width and height
    width, height = page.width, page.height

    # Check if width is greater than height multiplied by ratio
    return width > (height * ratio)


def page_extract_tables_md(page: Page, preserve_linebreaks: bool = False) -> list[str]:
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
        if (not table) or (len(table) == 1):  # Skip empty tables or single row table
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

        markdown_tables.append("```Markdown\n" + "\n".join(markdown) + "\n```")

    return markdown_tables


def pdf_blob_to_pdfplumber_doc(blob: bytes) -> Doc:
    """
    Converts a PDF byte blob into a pdfplumber PDF object.

    Args:
        blob (bytes): A byte blob representing a PDF file.

    Returns:
        pdfplumber.PDF: The pdfplumber PDF object created from the byte blob.
    """
    return pdfplumber.open(io.BytesIO(blob))


def insignificant_image(image_bbox: tuple):
    """
    If height or length of 'image' is smaller than 1, flagged as insignificant
    """
    min_dimension = 1
    # Calculate width and height
    x0, y0, x1, y1 = image_bbox
    width, height = x1 - x0, y1 - y0
    # Filter out small images based on dimensions
    if width < min_dimension or height < min_dimension:
        return 1
    return 0


def get_images_as_base64(page: Page) -> List[str]:
    """
    Converts all images on a given page to base64-encoded strings with high quality.

    Args:
        page (pdfplumber.page.Page): A single page of a pdfplumber document.

    Returns:
        List[str]: A list of base64-encoded strings, each representing a high-quality image on the page.
    """
    base64_images = []
    for k, image in enumerate(page.images):
        # Extract the bounding box of the image
        bbox = (image["x0"], image["top"], image["x1"], image["bottom"])

        if insignificant_image(bbox):
            logger.info(f"Ignoring {k+1}th image in {page} due to insignificant size")
            continue
        # Crop the image from the page
        cropped_page = page.within_bbox(bbox)
        if cropped_page:
            # Render a high-quality rasterized version of the cropped page
            pil_image = cropped_page.to_image(
                resolution=250
            ).original  # Use high resolution

            # Save as PNG into a BytesIO buffer for lossless compression
            buffer = io.BytesIO()
            pil_image.save(buffer, format="PNG")

            # Encode the image to base64
            base64_image = base64.b64encode(buffer.getvalue()).decode("utf-8")
            base64_images.append(base64_image)

    return base64_images
