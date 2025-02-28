"""
Define a pdf parser object that extract texts and images from doc
while maintaining page information
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Union

from loguru import logger
from pdfplumber.page import Page
from pdfplumber.pdf import PDF as Doc

from src.models import FileImage, FileText, PageStats

from .pdf_utils import (doc_exported_from_ppt, get_images_as_base64,
                        is_infographic_page, page_extract_tables_md,
                        page_to_base64, pdf_blob_to_pdfplumber_doc,
                        pdf_page_is_landscape)


def process_page_as_an_image(
    page: Page, page_no: int, stats: PageStats
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process a page like the whole page is an image"""
    page_image = FileImage(
        page_no=page_no, image_no=page_no, image_base64=page_to_base64(page, scale=2)
    )
    stats.update(has_text=False, has_images=True)
    return {"texts": [], "images": [page_image]}


def process_regular_pdf_page(
    page: Page, page_no: int, stats: PageStats
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process regular PDF page with text and images"""
    if is_infographic_page(page):
        logger.info(
            f"Page {page_no} contains multiple visual elements and will be treated as an image"
        )
        return process_page_as_an_image(page, page_no, stats)

    if pdf_page_is_landscape(page):
        logger.info(
            f"Page {page_no} has landscape layout and will be treated as an image"
        )
        return process_page_as_an_image(page, page_no, stats)

    text = page.extract_text()
    images_base64 = get_images_as_base64(page)

    tables_str = page_extract_tables_md(page)
    tables: List[FileText] = [FileText(page_no=page_no, text=tab) for tab in tables_str]

    if not text:
        if images_base64:
            logger.info(
                f"Page {page_no} contains no text elements and will be treated as an image"
            )
            return process_page_as_an_image(page, page_no, stats)
        else:
            logger.info(f"Page {page_no} contains no elements and will be skipped")
            return {"texts": [], "images": [], "tables": tables}

    # Process text and images
    texts = [FileText(page_no=page_no, text=text)]
    images = [
        FileImage(page_no=page_no, image_base64=img, image_no=i)
        for i, img in enumerate(images_base64)
    ]

    stats.update(has_text=bool(text), has_images=bool(images))
    return {"texts": texts, "images": images, "tables": tables}


def pdfplumber_extract_texts_and_images(doc: Doc, report: bool = False) -> Dict:
    """Extract texts and images from PDF document using parallel processing"""
    stats = PageStats()
    all_texts: List[FileText] = []
    all_tables: List[FileText] = []
    all_images: List[FileImage] = []

    process_fn = (
        process_page_as_an_image
        if doc_exported_from_ppt(doc)
        else process_regular_pdf_page
    )

    # Parallel processing
    with ThreadPoolExecutor() as executor:
        future_to_page = {
            executor.submit(process_fn, page, page_no, stats): page_no
            for page_no, page in enumerate(doc.pages)
        }

        for future in as_completed(future_to_page):
            processing_output = future.result()
            all_texts.extend(processing_output["texts"])
            all_images.extend(processing_output["images"])
            all_tables.extend(processing_output.get("tables", []))

    if report:
        stats.log_summary(doc.metadata)

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
