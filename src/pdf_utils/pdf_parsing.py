"""
Define a pdf parser object that extract texts and images from doc
while maintaining page information
"""

from typing import Dict, Iterator, List, Union

from loguru import logger
from pdfplumber.page import Page
from pdfplumber.pdf import PDF as Doc

from src.models import FileImage, FileText

from .pdf_utils import (doc_exported_from_ppt, get_images_as_base64,
                        is_infographic_page, page_extract_tables_md,
                        page_to_base64, pdf_blob_to_pdfplumber_doc,
                        pdf_page_is_landscape)


def process_page_as_an_image(
    page: Page, page_no: int
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process a page like the whole page is an image"""
    page_image = FileImage(
        page_no=page_no, image_no=page_no, image_base64=page_to_base64(page, scale=2)
    )
    return {"texts": [], "images": [page_image]}


def process_regular_pdf_page(
    page: Page,
    page_no: int,
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process regular PDF page with text and images"""
    if is_infographic_page(page):
        logger.info(
            f"Page {page_no} contains multiple visual elements and will be treated as an image"
        )
        return process_page_as_an_image(
            page,
            page_no,
        )

    if pdf_page_is_landscape(page):
        logger.info(
            f"Page {page_no} has landscape layout and will be treated as an image"
        )
        return process_page_as_an_image(
            page,
            page_no,
        )

    text = page.extract_text()
    images_base64 = get_images_as_base64(page)

    tables_str = page_extract_tables_md(page)
    tables: List[FileText] = [FileText(page_no=page_no, text=tab) for tab in tables_str]

    if not text:
        if images_base64:
            logger.info(
                f"Page {page_no} contains no text elements and will be treated as an image"
            )
            return process_page_as_an_image(
                page,
                page_no,
            )
        else:
            logger.info(f"Page {page_no} contains no elements and will be skipped")
            return {"texts": [], "images": [], "tables": tables}

    # Process text and images
    texts = [FileText(page_no=page_no, text=text)]
    images = [
        FileImage(page_no=page_no, image_base64=img, image_no=i)
        for i, img in enumerate(images_base64)
    ]

    return {"texts": texts, "images": images, "tables": tables}


def pdfplumber_extract_texts_and_images(
    doc: Doc, report: bool = False, batch_size: int = 100
) -> Dict:
    """Extract texts and images from a PDF document using sequential batch processing"""
    all_texts: List[FileText] = []
    all_tables: List[FileText] = []
    all_images: List[FileImage] = []

    process_fn = (
        process_page_as_an_image
        if doc_exported_from_ppt(doc)
        else process_regular_pdf_page
    )

    total_pages = len(doc.pages)
    logger.info(f"Processing PDF with {total_pages} pages")

    # Process in batches to manage memo        # Process each page in the batch sequentially
    for page_no in range(total_pages):
        try:
            page = doc.pages[page_no]
            processing_output = process_fn(
                page,
                page_no,
            )

            all_texts.extend(processing_output["texts"])
            all_images.extend(processing_output["images"])
            all_tables.extend(processing_output.get("tables", []))

            # Free the page reference explicitly
            page = None

        except Exception as e:
            logger.error(f"Error processing page {page_no}: {str(e)}")

    return {"texts": all_texts, "images": all_images, "tables": all_tables}


def pdf_extract_texts_and_images(file_content: bytes) -> Dict:
    texts = []
    images = []
    tables = []
    num_pages = None
    with pdf_blob_to_pdfplumber_doc(file_content) as doc:
        # Create file metadata
        num_pages = len(doc.pages)

        extraction = pdfplumber_extract_texts_and_images(doc, report=True)
        texts, images, tables = (
            extraction["texts"],
            extraction["images"],
            extraction["tables"],
        )
        logger.info("Extracted raw texts and images")
    return {
        "texts": texts,
        "images": images,
        "tables": tables,
        "num_pages": num_pages,
    }


def pdf_extract_texts_and_images_batch(
    file_content: bytes, batch_size: int = 100
) -> Iterator[Dict]:
    with pdf_blob_to_pdfplumber_doc(file_content) as doc:
        num_pages = len(doc.pages)
        process_fn = (
            process_page_as_an_image
            if doc_exported_from_ppt(doc)
            else process_regular_pdf_page
        )

        # Use an iterator for doc.pages
        page_iter = iter(doc.pages)
        batch_texts = []
        batch_images = []
        batch_tables = []
        page_no = 0

        for page in page_iter:
            processing_output = process_fn(page, page_no)

            page.flush_cache()
            page.get_textmap.cache_clear()
            page.close()

            batch_texts.extend(processing_output.get("texts", []))
            batch_images.extend(processing_output.get("images", []))
            batch_tables.extend(processing_output.get("tables", []))

            page_no += 1

            # Yield a batch when batch_size is reached
            if page_no % batch_size == 0:
                yield {
                    "texts": batch_texts,
                    "images": batch_images,
                    "tables": batch_tables,
                    "num_pages": num_pages,
                }
                # Clear the batch data
                batch_texts = []
                batch_images = []
                batch_tables = []

        # Yield the last batch if it has any data
        if batch_texts or batch_images or batch_tables:
            yield {
                "texts": batch_texts,
                "images": batch_images,
                "tables": batch_tables,
                "num_pages": num_pages,
            }
