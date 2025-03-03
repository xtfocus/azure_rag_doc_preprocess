"""
Module for parsing docx and doc
"""

import base64
import datetime
import hashlib
import os
import subprocess
from io import BytesIO
from pathlib import Path

from docx import Document
from docx.table import Table
from docx.text.paragraph import Paragraph
from loguru import logger

from src.models import FileImage, FileText


def iter_block_items(parent):
    """
    Yield each paragraph and table child within the document in order.
    """
    for child in parent.element.body.iterchildren():
        if child.tag.endswith("p"):
            yield Paragraph(child, parent)
        elif child.tag.endswith("tbl"):
            yield Table(child, parent)


def table_to_markdown(table):
    """
    Convert a docx Table object to Markdown format.
    """
    table_data = []
    max_cols = max(len(row.cells) for row in table.rows)

    for row in table.rows:
        row_data = [cell.text.replace("\n", "<br>").strip() for cell in row.cells]
        table_data.append(row_data)

    if not table_data:
        return ""

    table_markdown = []
    header = table_data[0]
    table_markdown.append("| " + " | ".join(header) + " |")
    table_markdown.append("| " + " | ".join(["---"] * max_cols) + " |")

    for row in table_data[1:]:
        table_markdown.append("| " + " | ".join(row) + " |")

    return "\n".join(table_markdown)


def docx_extract_texts_and_images(file_content: bytes):
    """
    Parse a .docx file to extract texts, images, tables, and drawings.

    Args:
        file_content (bytes): Bytes of the file.

    Returns:
        dict: A dictionary with extracted texts, images, tables, and drawings.
    """
    doc = Document(BytesIO(file_content))
    markdown_content = ""
    markdown_tables = []
    images = []
    drawings = []

    # Iterate over paragraphs and tables in document order
    for block in iter_block_items(doc):
        if isinstance(block, Paragraph):
            text = block.text.strip()
            if text:
                markdown_content += "\n" + text
        elif isinstance(block, Table):
            table_md = table_to_markdown(block)
            if table_md:
                markdown_content += "\n" + table_md
                markdown_tables.append(FileText(text=table_md, page_no=0))

    # Extract images and drawings
    image_counter = 0
    for rel in doc.part.rels.values():
        if "image" in rel.target_ref:
            image_data = rel.target_part.blob
            image_base64 = base64.b64encode(image_data).decode("utf-8")
            images.append(
                FileImage(
                    page_no=0,  # Assuming rendering is dynamic
                    image_no=image_counter,
                    image_base64=image_base64,
                )
            )
            image_counter += 1
        elif "drawing" in rel.target_ref:
            drawings.append(rel.target_ref)

    return {
        "texts": [FileText(text=markdown_content, page_no=0)],
        "images": images,
        "tables": markdown_tables,
        "drawings": drawings,
    }


def doc_extract_texts_and_images(file_content: bytes):
    temp_indir = os.getenv("TEMP_INDIR", "temp_indir")
    temp_outdir = os.getenv("TEMP_OUTDIR", "temp_outdir")

    # Ensure temp directories exist
    os.makedirs(temp_indir, exist_ok=True)
    os.makedirs(temp_outdir, exist_ok=True)

    # Generate a unique false filename
    file_hash = hashlib.md5(file_content).hexdigest()[
        :8
    ]  # Shorten hash for readability
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    false_name = f"doc_{file_hash}_{timestamp}.doc"

    input_path = Path(temp_indir) / false_name
    output_path = Path(temp_outdir) / (false_name + "x")  # Convert to .docx extension

    try:
        # Write input file
        with open(input_path, "wb") as f:
            f.write(file_content)

        logger.debug("Start conversion doc --> docx")

        # Convert to docx using LibreOffice (lowriter)
        subprocess.run(
            [
                "lowriter",
                "--convert-to",
                "docx",
                str(input_path),
                "--outdir",
                temp_outdir,
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        logger.debug("Finish conversion doc --> docx")

        # Read the converted docx file
        with open(output_path, "rb") as f:
            docx_binary = f.read()

        # Extract text and images
        result = docx_extract_texts_and_images(docx_binary)

    finally:
        # Cleanup temporary files
        try:
            os.remove(input_path)
        except FileNotFoundError:
            pass
        try:
            os.remove(output_path)
        except FileNotFoundError:
            pass

        logger.debug("Cleaned up after conversion doc --> docx")

    return result
