"""
Define a pdf parser object that extract texts and images from doc
while maintaining page information
"""

import base64
from typing import List, Optional, Tuple

from fitz import Document, Matrix
from pydantic import BaseModel

from src.file_utils import get_images_as_base64, page_extract_images


class FileText(BaseModel):
    """
    Represents a page of text
    """

    page_no: int
    text: Optional[str]


class FileImage(BaseModel):
    page_no: int
    image_no: int
    image_base64: str


from typing import List, Tuple

from loguru import logger


def extract_texts_and_images(
    doc: Document,
    report=False,
) -> Tuple[List[FileText], List[FileImage]]:
    """
    Extract texts and images for each page and log a summary table in a 2x2 matrix format
    """
    texts: List = []
    images: List = []
    # Matrix to track the counts
    page_stats = {
        "text_yes_image_yes": 0,  # Pages with both text and images
        "text_yes_image_no": 0,  # Pages with text but no images
        "text_no_image_yes": 0,  # Pages with images but no text
        "text_no_image_no": 0,  # Pages with neither text nor images
    }

    for page_no, page in enumerate(doc):
        images_base64 = get_images_as_base64(page)

        text = page.get_text()

        # Select only images having more than one color
        # In the future, we probably also exclude certain logos, icons, etc.
        images_pixmap = page_extract_images(page)
        images_is_multicolor = [(not image.is_unicolor) for image in images_pixmap]
        images_base64 = [
            image
            for image, is_multicolor in zip(images_base64, images_is_multicolor)
            if is_multicolor
        ]

        if report:
            # Update the appropriate category in the matrix
            if text and images_base64:
                page_stats["text_yes_image_yes"] += 1
            elif text:
                page_stats["text_yes_image_no"] += 1
            elif images_base64:
                page_stats["text_no_image_yes"] += 1
            else:
                page_stats["text_no_image_no"] += 1

        if not bool(text):  # If no text detected, convert the whole page to an image
            pix = page.get_pixmap(matrix=Matrix(2, 2))  # 2x scaling for better quality
            img_data = pix.tobytes("png")  # Get PNG bytes
            img_base64 = base64.b64encode(img_data).decode()  # Convert to Base64
            images.append(
                FileImage(
                    page_no=page_no, image_no=len(images), image_base64=img_base64
                )
            )
        else:
            texts.append(FileText(page_no=page_no, text=text))
            if images_base64:
                images += [
                    FileImage(page_no=page_no, image_base64=image_base64, image_no=i)
                    for i, image_base64 in enumerate(images_base64)
                ]

    if report:
        # Log the summary as a markdown 2x2 matrix
        logger.info(
            "\n"
            "|                     | Images Yes         | Images No          |\n"
            "|---------------------|--------------------|--------------------|\n"
            f"| **Text Yes**        | {page_stats['text_yes_image_yes']:>18} | {page_stats['text_yes_image_no']:>18} |\n"
            f"| **Text No**         | {page_stats['text_no_image_yes']:>18} | {page_stats['text_no_image_no']:>18} |"
        )

    return texts, images
