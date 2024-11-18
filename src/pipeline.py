import asyncio
from typing import Any, Callable, Dict, List, Tuple

from loguru import logger
from pydantic import BaseModel

from src.file_utils import (create_file_metadata_from_bytes,
                            pdf_blob_to_pymupdf_doc)
from src.image_descriptor import ImageDescriptor
from src.models import BaseChunk, PageRange
from src.pdf_parsing import extract_texts_and_images
from src.splitters import SimplePageTextSplitter
from src.vector_stores import MyAzureSearch


class MyFile(BaseModel):
    file_name: str
    file_content: bytes


class Pipeline:
    """
    Orchestrating the extracting > chunking > embedding > indexing using Azure resources
    """

    def __init__(
        self,
        text_vector_store: MyAzureSearch,
        image_vector_store: MyAzureSearch,
        embedding_function: Callable,
        text_splitter: SimplePageTextSplitter,
        image_descriptor: ImageDescriptor,
    ):
        """Initialize the pipeline with necessary components

        Args:
            text_vector_store: Vector store for text chunks
            image_vector_store: Vector store for image descriptions
            embedding_function: Function to create embeddings
            text_splitter: Text splitting strategy
            image_descriptor: OpenAI client wrapper for image description
        """
        self.text_vector_store = text_vector_store
        self.image_vector_store = image_vector_store
        self.embedding_function = embedding_function
        self.text_splitter = text_splitter
        self.image_descriptor = image_descriptor

    async def _process_images(self, images: List[Any]) -> List[str]:
        """Process multiple images concurrently to get their descriptions

        Args:
            images: List of image objects with image_base64 attribute

        Returns:
            List[str]: List of image descriptions
        """
        tasks = [self.image_descriptor.run(img.image_base64) for img in images]
        return await asyncio.gather(*tasks)

    def _create_text_chunks(
        self, texts: List[Any], file_metadata: Dict
    ) -> Tuple[List[str], List[Dict]]:
        """Create text chunks and their metadata

        Args:
            texts: List of text objects
            file_metadata: Metadata about the file

        Returns:
            Tuple containing lists of texts and their metadata
        """
        text_chunks = self.text_splitter.split_text((text.dict() for text in texts))
        return self.text_vector_store.create_texts_and_metadatas(
            text_chunks, file_metadata, prefix="text"
        )

    def _create_image_chunks(
        self, images: List[Any], descriptions: List[str], file_metadata: Dict
    ) -> Tuple[List[str], List[Dict]]:
        """Create image chunks and their metadata

        Args:
            images: List of image objects
            descriptions: List of image descriptions
            file_metadata: Metadata about the file

        Returns:
            Tuple containing lists of image texts and their metadata
        """
        image_chunks = [
            BaseChunk(
                chunk_no=f"{img.page_no}_{img.image_no}",
                page_range=PageRange(start_page=img.page_no, end_page=img.page_no),
                chunk=desc,
            )
            for img, desc in zip(images, descriptions)
        ]
        return self.image_vector_store.create_texts_and_metadatas(
            image_chunks, file_metadata, prefix="image"
        )

    async def process_file(self, file: MyFile) -> Dict[str, Any]:
        """Process a single file through the pipeline

        Args:
            file: MyFile object containing file name and content

        Returns:
            Dict containing processing results and statistics
        """
        # Convert PDF to document
        doc = pdf_blob_to_pymupdf_doc(file.file_content)

        # Create file metadata
        file_metadata = create_file_metadata_from_bytes(
            file_bytes=file.file_content, file_name=file.file_name
        )

        # Extract texts and images
        texts, images = extract_texts_and_images(doc, report=True)

        if texts:
            # Create and index text chunks
            input_texts, input_metadatas = self._create_text_chunks(
                texts, file_metadata
            )
            self.text_vector_store.add_texts(
                texts=input_texts, metadatas=input_metadatas
            )

        if images:
            # Process images in parallel
            image_descriptions = await self._process_images(images)
            # Create and index image chunks
            image_texts, image_metadatas = self._create_image_chunks(
                images, image_descriptions, file_metadata
            )
            self.image_vector_store.add_texts(
                texts=image_texts, metadatas=image_metadatas
            )

        else:
            logger.info(f"Neither text nor image found in {file.file_name}")

        logger.info(f"Processed file {file.file_name}")

        return {
            "file_name": file.file_name,
            "num_pages": len(doc),
            "num_texts": len(texts),
            "num_images": len(images),
            "metadata": file_metadata,
        }
