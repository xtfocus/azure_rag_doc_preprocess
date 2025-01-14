import hashlib


def create_file_metadata_from_bytes(file_bytes: bytes, file_name: str):
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
