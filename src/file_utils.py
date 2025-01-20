import hashlib
from io import BytesIO
from zipfile import ZipFile

import chardet


def create_file_metadata_from_bytes(file_bytes: bytes, file_name: str) -> dict:
    """
    Create metadata for a document file using the file contents in bytes.

    Parameters:
    - file_bytes (bytes): The bytes content of the document file.
    - file_name (str): The file name of the document.

    Returns:
    - dict: Metadata dictionary containing the document title, file name, and SHA-256 hash.
    """
    # If title is not provided, infer it from the file_name
    title = file_name

    # Calculate SHA-256 hash to uniquely identify the file
    sha256_hash = hashlib.sha256()
    sha256_hash.update(file_bytes)
    file_hash = sha256_hash.hexdigest()

    return {
        "title": title,
        "file": file_name,
        "file_hash": file_hash,
    }


def detect_file_type(file_bytes: bytes) -> str:
    """
    Detect the file type (PDF, DOC, DOCX, JPG, PNG, CSV, XLSX, TXT) with enhanced validation.

    Args:
        file_bytes (bytes): File content as bytes.

    Returns:
        str: 'pdf', 'doc', 'docx', 'jpg', 'png', 'csv', 'xlsx', 'txt', or 'unknown'.
    """
    # File signatures
    PDF_SIGNATURE = bytes([0x25, 0x50, 0x44, 0x46, 0x2D])  # %PDF-
    DOCX_SIGNATURE = bytes([0x50, 0x4B, 0x03, 0x04])  # ZIP
    JPG_SIGNATURE = bytes([0xFF, 0xD8, 0xFF, 0xE0])  # Standard JPEG
    JPG_SIGNATURE_EXIF = bytes([0xFF, 0xD8, 0xFF, 0xE1])  # JPEG with EXIF
    PNG_SIGNATURE = bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A])  # Full PNG
    DOC_SIGNATURES = [
        bytes(
            [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1]
        ),  # Compound File Binary Format
        bytes([0x0D, 0x44, 0x4F, 0x43]),  # Older DOC format
        bytes([0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, 0x00]),  # Alternative CFBF
    ]

    # Ensure the input has enough bytes for signature detection
    if len(file_bytes) < 8:
        return "unknown"

    header = file_bytes[:8]

    # Check signatures
    if header.startswith(PDF_SIGNATURE):
        return "pdf"

    if header.startswith(JPG_SIGNATURE) or header.startswith(JPG_SIGNATURE_EXIF):
        return "jpg"

    if header.startswith(PNG_SIGNATURE):
        return "png"

    # Check for DOC file signatures
    for doc_sig in DOC_SIGNATURES:
        if header.startswith(doc_sig):
            return "doc"

    # Handle Office documents (DOCX/XLSX)
    if header.startswith(DOCX_SIGNATURE):
        try:
            with ZipFile(BytesIO(file_bytes)) as zf:
                # Check for specific files in Office Open XML
                if "word/document.xml" in zf.namelist():
                    return "docx"
                if "xl/workbook.xml" in zf.namelist():
                    return "xlsx"
        except:
            pass

    # Handle CSV with content validation
    try:
        # Read first 1024 bytes for encoding detection
        raw_data = file_bytes[:1024]
        encoding = chardet.detect(raw_data)["encoding"]

        # Try to decode and check for commas
        content = raw_data.decode(encoding)
        if "," in content.splitlines()[0]:
            return "csv"
    except:
        pass

    # Handle TXT files with encoding validation
    try:
        # Read first 1024 bytes for encoding detection
        raw_data = file_bytes[:1024]
        encoding = chardet.detect(raw_data)["encoding"]

        # Try to decode the content
        content = raw_data.decode(encoding)
        # Check if the content is printable ASCII or valid Unicode
        if any(ord(c) < 128 and c.isprintable() for c in content):
            return "txt"
    except:
        pass

    # Additional text file validation for general text
    try:
        # Read first 1024 bytes
        raw_data = file_bytes[:1024]

        # Check if content appears to be text
        encoding = chardet.detect(raw_data)
        if encoding["encoding"] and encoding["confidence"] > 0.9:
            text = raw_data.decode(encoding["encoding"])
            # Check if content is primarily printable characters
            printable_ratio = sum(c.isprintable() for c in text) / len(text)
            if printable_ratio > 0.95:
                return "txt"
    except:
        pass

    return "unknown"
