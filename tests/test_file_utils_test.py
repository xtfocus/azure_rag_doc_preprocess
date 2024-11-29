import base64
import os
from pathlib import Path

import fitz
import pytest

from src.file_utils import (create_file_metadata_from_bytes,
                            create_file_metadata_from_path,
                            extract_single_image, get_images_as_base64,
                            page_extract_images, pdf_blob_to_pymupdf_doc)


# Fixture for test files directory
@pytest.fixture
def test_files_dir():
    current_dir = Path(__file__).parent
    test_files_dir = current_dir / "test_files"
    test_files_dir.mkdir(exist_ok=True)
    return test_files_dir


# Fixture for a simple text PDF
@pytest.fixture
def text_pdf_path(test_files_dir):
    pdf_path = test_files_dir / "text_only.pdf"
    if not pdf_path.exists():
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 50), "This is a test document")
        doc.save(str(pdf_path))
        doc.close()
    return pdf_path


# Fixture for a PDF with an image
@pytest.fixture
def image_pdf_path(test_files_dir):
    pdf_path = test_files_dir / "with_image.pdf"
    if not pdf_path.exists():
        error_msg = f"Please add pdf test file with images in {pdf_path} "
        raise ValueError(error_msg)
    return pdf_path


# Fixture for an empty PDF
@pytest.fixture
def empty_pdf_path(test_files_dir):
    pdf_path = test_files_dir / "empty.pdf"
    if not pdf_path.exists():
        doc = fitz.open()
        doc.new_page()
        doc.save(str(pdf_path))
        doc.close()
    return pdf_path


def test_pdf_blob_to_pymupdf_doc(text_pdf_path):
    # Read PDF as bytes
    with open(text_pdf_path, "rb") as f:
        pdf_blob = f.read()

    # Convert blob to document
    doc = pdf_blob_to_pymupdf_doc(pdf_blob)
    assert isinstance(doc, fitz.Document)
    assert doc.page_count > 0
    doc.close()


def test_page_extract_images(image_pdf_path):
    doc = fitz.open(str(image_pdf_path))
    images = page_extract_images(doc[0])
    assert len(images) > 0  # Should contain at least one image
    assert all(isinstance(img, fitz.Pixmap) for img in images)
    doc.close()


def test_get_images_as_base64(image_pdf_path):
    doc = fitz.open(str(image_pdf_path))
    base64_images = get_images_as_base64(doc[0])
    assert len(base64_images) > 0
    # Verify that each string is valid base64
    for img_str in base64_images:
        try:
            base64.b64decode(img_str)
        except Exception:
            pytest.fail("Invalid base64 string")
    doc.close()


def test_create_file_metadata_from_path(empty_pdf_path):

    try:
        metadata = create_file_metadata_from_path(empty_pdf_path)
    except Exception:
        pytest.fail("Function not functioning")

    assert metadata["title"] == "empty"
    assert metadata["file"] == "empty.pdf"
    assert len(metadata["file_hash"]) == 64  # Verify the length of the SHA-256 hash


def test_create_file_metadata_from_bytes(text_pdf_path):
    with open(text_pdf_path, "rb") as f:
        file_bytes = f.read()
    metadata = create_file_metadata_from_bytes(file_bytes, "text_only.pdf")
    assert metadata["title"] == "text_only"
    assert metadata["file"] == "text_only.pdf"
    assert len(metadata["file_hash"]) == 64  # Verify the length of the SHA-256 hash


def test_create_file_metadata_from_bytes_with_title(text_pdf_path):
    with open(text_pdf_path, "rb") as f:
        file_bytes = f.read()
    metadata = create_file_metadata_from_bytes(
        file_bytes, "text_only.pdf", title="My Document"
    )
    assert metadata["title"] == "My Document"
    assert metadata["file"] == "text_only.pdf"
    assert len(metadata["file_hash"]) == 64  # Verify the length of the SHA-256 hash
