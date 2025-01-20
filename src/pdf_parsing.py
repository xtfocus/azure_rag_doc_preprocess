"""
Define a pdf parser object that extract texts and images from doc
while maintaining page information
"""

import base64
import io
from typing import Dict, List, Union

from loguru import logger
from pdfplumber.page import Page
from pdfplumber.pdf import PDF as Doc

from src.models import FileImage, FileText, PageStats
from src.pdf_utils import (get_images_as_base64, page_extract_tables_md,
                           page_to_base64, pdf_blob_to_pdfplumber_doc)


def doc_exported_from_ppt(pdf: Doc) -> bool:
    """Return True if pdf document is a PowerPoint export"""
    metadata = pdf.metadata
    return any(
        "PowerPoint" in metadata.get(field, "") for field in ["Creator", "Producer"]
    )


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


def process_page_as_an_image(
    page: Page, page_no: int, stats: PageStats
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process a page like the whole page is an image"""
    page_image = FileImage(
        page_no=page_no, image_no=page_no, image_base64=page_to_base64(page, scale=1)
    )
    stats.update(has_text=False, has_images=True)
    return {"texts": [], "images": [page_image]}


def process_regular_page(
    page: Page,
    page_no: int,
    stats: PageStats,
) -> Dict[str, Union[List[FileText], List[FileImage]]]:
    """Process regular PDF page with text and images"""
    if is_infographic_page(page):
        logger.info(
            f"Page {page_no} contains multiple visual elements and will be treated as an image"
        )
        return process_page_as_an_image(page, page_no, stats)

    text = page.extract_text()
    tables = "\n\n".join(page_extract_tables_md(page))
    text += tables
    images_base64 = get_images_as_base64(page)

    if not text:
        if images_base64:
            logger.info(
                f"Page {page_no} contains no text elements and will be treated as an image"
            )
            return process_page_as_an_image(page, page_no, stats)
        else:
            logger.info(f"Page {page_no} contains no elements and will be skipped")
            return {"texts": [], "images": []}

    # Process text and images
    texts = [FileText(page_no=page_no, text=text)]
    images = [
        FileImage(page_no=page_no, image_base64=img, image_no=i)
        for i, img in enumerate(images_base64)
    ]

    stats.update(has_text=bool(text), has_images=bool(images))
    return {"texts": texts, "images": images}


def pdfplumber_extract_texts_and_images(doc: Doc, report: bool = False) -> Dict:
    """Extract texts and images from PDF document"""
    stats = PageStats()
    all_texts: List[FileText] = []
    all_images: List[FileImage] = []

    process_fn = (
        process_page_as_an_image if doc_exported_from_ppt(doc) else process_regular_page
    )

    for page_no, page in enumerate(doc.pages):
        processing_output = process_fn(page, page_no, stats)
        texts, images = processing_output["texts"], processing_output["images"]
        all_texts.extend(texts)
        all_images.extend(images)

    if report:
        stats.log_summary(doc.metadata)

    return {"texts": all_texts, "images": all_images}


def pdf_extract_texts_and_images(file_content: bytes) -> Dict:
    texts = []
    images = []
    num_pages = None
    with pdf_blob_to_pdfplumber_doc(file_content) as doc:
        # Create file metadata
        num_pages = len(doc.pages)
        extraction = pdfplumber_extract_texts_and_images(doc, report=True)
        texts, images = extraction["texts"], extraction["images"]
        logger.info("Extracted raw texts and images")
    return {
        "texts": texts,
        "images": images,
        "num_pages": num_pages,
    }
