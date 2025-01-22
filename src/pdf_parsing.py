"""
Define a pdf parser object that extract texts and images from doc
while maintaining page information
"""

import base64
import io
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from loguru import logger
from pdfplumber.page import Page
from pdfplumber.pdf import PDF as Doc
from pydantic import BaseModel

from src.pdf_utils import (get_images_as_base64, page_extract_tables_md,
                           pdf_doc_is_ppt, pdf_page_is_landscape)


class FileText(BaseModel):
    """
    Represents a page of text
    """

    page_no: int
    text: Optional[str]


class FileImage(BaseModel):
    """
    Represent an image
    """

    page_no: int
    image_no: int
    image_base64: str


@dataclass
class PageStats:
    """
    Document statistics on the number of pages grouped by
        whether they contain or not contain any texts or images
    """

    text_yes_image_yes: int = 0
    text_yes_image_no: int = 0
    text_no_image_yes: int = 0
    text_no_image_no: int = 0

    def update(self, has_text: bool, has_images: bool) -> None:
        if has_text and has_images:
            self.text_yes_image_yes += 1
        elif has_text:
            self.text_yes_image_no += 1
        elif has_images:
            self.text_no_image_yes += 1
        else:
            self.text_no_image_no += 1

    def log_summary(self, doc_metadata: dict) -> None:
        logger.info(f"File metadata: {doc_metadata}")
        logger.info(
            "\n"
            "|                    | Has Images         | No Images          |\n"
            "|--------------------|--------------------|--------------------|\n"
            f"| **Has Text**       | {self.text_yes_image_yes:>18} | {self.text_yes_image_no:>18} |\n"
            f"| **No Text**        | {self.text_no_image_yes:>18} | {self.text_no_image_no:>18} |"
        )


def page_to_base64(page: Page, format: str = "PNG", scale: int = 2) -> str:
    """Convert whole page to base64 image"""
    # Convert page to image using pdfplumber's native method
    img = page.to_image(resolution=72 * scale)

    # Get the image as bytes
    img_buffer = io.BytesIO()
    img.original.save(img_buffer, format=format)

    return base64.b64encode(img_buffer.getvalue()).decode()


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
        page_no=page_no, image_no=page_no, image_base64=page_to_base64(page, scale=2)
    )
    stats.update(has_text=False, has_images=True)
    return {"texts": [], "images": [page_image]}


def process_regular_page(
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
    tables = page_extract_tables_md(page)
    images_base64 = get_images_as_base64(page)
    tables = [FileText(page_no=page_no, text=tab) for tab in tables]

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


def extract_texts_images_tables(doc: Doc, report: bool = False) -> Dict:
    """Extract texts and images from PDF document"""
    stats = PageStats()
    all_texts: List[FileText] = []
    all_tables: List[FileText] = []
    all_images: List[FileImage] = []

    process_fn = (
        process_page_as_an_image if pdf_doc_is_ppt(doc) else process_regular_page
    )

    for page_no, page in enumerate(doc.pages):
        result = process_fn(page, page_no, stats)

        texts, images, tables = (
            result["texts"],
            result["images"],
            result.get("tables", []),
        )
        all_texts.extend(texts)
        all_images.extend(images)
        all_tables.extend(tables)

    if report:
        stats.log_summary(doc.metadata)

    return {"texts": all_texts, "images": all_images, "tables": all_tables}
