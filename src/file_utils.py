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
    Detect the file type (PDF, DOC, DOCX, JPG/JPEG, PNG, CSV, XLSX, TXT) with enhanced validation.

    Args:
        file_bytes (bytes): File content as bytes.

    Returns:
        str: 'pdf', 'doc', 'docx', 'jpg', 'png', 'csv', 'xlsx', 'txt', or 'unknown'.
    """
    # Basic signature check
    if len(file_bytes) < 4:  # We only need 4 bytes for JPEG
        return "unknown"

    # JPEG starts with FF D8 FF
    if file_bytes.startswith(bytes([0xFF, 0xD8, 0xFF])):
        # The fourth byte is usually E0, E1, E2, E8, DB, or EE
        fourth_byte = file_bytes[3] if len(file_bytes) > 3 else 0
        if fourth_byte in [0xE0, 0xE1, 0xE2, 0xE8, 0xDB, 0xEE]:
            return "jpg"

    # Rest of the file type checks...
    if len(file_bytes) < 8:  # Other formats need 8 bytes
        return "unknown"

    header = file_bytes[:8]

    # PDF signature
    if header.startswith(bytes([0x25, 0x50, 0x44, 0x46, 0x2D])):  # %PDF-
        return "pdf"

    # PNG signature
    if header.startswith(bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A])):
        return "png"

    # DOC signatures
    DOC_SIGNATURES = [
        bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1]),
        bytes([0x0D, 0x44, 0x4F, 0x43]),
        bytes([0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, 0x00]),
    ]
    for sig in DOC_SIGNATURES:
        if header.startswith(sig):
            return "doc"

    # DOCX/XLSX (ZIP) signature
    if header.startswith(bytes([0x50, 0x4B, 0x03, 0x04])):
        try:
            with ZipFile(BytesIO(file_bytes)) as zf:
                filelist = zf.namelist()
                if "word/document.xml" in filelist:
                    return "docx"
                if "xl/workbook.xml" in filelist:
                    return "xlsx"
        except:
            pass

    # Text-based formats (CSV, TXT)
    try:
        sample = file_bytes[:1024]
        # Try UTF-8 first for better compatibility
        try:
            text = sample.decode("utf-8")
        except UnicodeDecodeError:
            # Fallback to chardet detection
            encoding = chardet.detect(sample)
            if (
                encoding["encoding"] and encoding["confidence"] > 0.7
            ):  # Lower confidence threshold
                text = sample.decode(encoding["encoding"], errors="replace")
            else:
                return "unknown"

        # Check for CSV (more robust check)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) > 0:
            if all(
                len(line.split(",")) > 1 for line in lines[:3]
            ):  # Check multiple lines
                return "csv"

        # Improved text validation
        acceptable_chars = sum(
            c.isprintable() or c.isspace()  # Count spaces/newlines as valid
            for c in text
        )
        total_chars = max(len(text), 1)  # Avoid division by zero

        if (acceptable_chars / total_chars) > 0.8:  # Lower threshold
            return "txt"

    except Exception:
        pass

    return "unknown"
