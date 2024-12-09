import asyncio
from typing import Any, Callable, Dict, List, Tuple

from loguru import logger
from pydantic import BaseModel

from src.azure_container_client import AzureContainerClient
from src.file_summarizer import FileSummarizer
from src.file_utils import (create_file_metadata_from_bytes,
                            pdf_blob_to_pymupdf_doc)
from src.image_descriptor import ImageDescriptor
from src.models import BaseChunk, PageRange
from src.pdf_parsing import FileImage, extract_texts_and_images
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
        summary_vector_store: MyAzureSearch,
        embedding_function: Callable,
        text_splitter: SimplePageTextSplitter,
        image_descriptor: ImageDescriptor,
        file_summarizer: FileSummarizer,
        image_container_client: AzureContainerClient,
    ):
        """Initialize the pipeline with necessary components

        Args:
            text_vector_store: Vector store for text chunks
            image_vector_store: Vector store for image descriptions
            embedding_function: Function to create embeddings
            text_splitter: Text splitting strategy
            image_descriptor: OpenAI client wrapper for image description
            image_container_client: client wrapper for image storage
        """
        self.text_vector_store = text_vector_store
        self.image_vector_store = image_vector_store
        self.summary_vector_store = summary_vector_store
        self.embedding_function = embedding_function
        self.text_splitter = text_splitter
        self.image_descriptor = image_descriptor
        self.file_summarizer = file_summarizer
        self.image_container_client = image_container_client

    async def _process_images(
        self, images: List[FileImage], max_concurrent_requests: int = 5
    ) -> List[str]:
        """Process multiple images concurrently with rate limiting using a semaphore."""
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def process_single_image(image):
            async with semaphore:
                return await self.image_descriptor.run(image.image_base64)

        tasks = [process_single_image(img) for img in images]
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

    async def _create_and_add_text_chunks(self, texts: List[Any], file_metadata: Dict):
        """Combine creation and adding of text chunks"""
        if not texts:
            return None

        input_texts, input_metadatas = self._create_text_chunks(texts, file_metadata)
        return await self.text_vector_store.add_texts(
            texts=input_texts, metadatas=input_metadatas
        )

    async def _create_and_add_image_chunks(
        self, images: List[Any], descriptions: List[str], file_metadata: Dict
    ):
        """Combine creation and adding of image chunks"""
        if not images:
            return None
        image_texts, image_metadatas = self._create_image_chunks(
            images, descriptions, file_metadata
        )
        return (
            await self.image_vector_store.add_texts(
                texts=image_texts,
                metadatas=image_metadatas,
                filter_by_min_len=10,
            ),
            image_metadatas,
        )

    async def _create_and_add_file_summaries(self, texts, images, file_metadata: Dict):
        """Create and add file summary"""
        summary = await self.file_summarizer.run(texts, images)

        summary_texts, summary_metadatas = (
            self.summary_vector_store.create_texts_and_metadatas(
                [
                    BaseChunk(
                        chunk=summary,
                        chunk_no="0",
                        page_range=PageRange(start_page=0, end_page=0),
                    )
                ],
                file_metadata,
                prefix="summary",
            )
        )

        return await self.summary_vector_store.add_texts(
            texts=summary_texts, metadatas=summary_metadatas
        )

    async def process_file(self, file: MyFile) -> Dict[str, Any]:
        """Process a single file through the pipeline with optimized concurrent operations"""
        # Convert PDF to document
        doc = pdf_blob_to_pymupdf_doc(file.file_content)

        # Create file metadata
        file_metadata = create_file_metadata_from_bytes(
            file_bytes=file.file_content, file_name=file.file_name
        )

        texts, images = extract_texts_and_images(doc, report=True)
        logger.info("Extracted raw texts and images")

        # Start file summary generation early since it's I/O bound
        summary_task = asyncio.create_task(
            self._create_and_add_file_summaries(
                [i.text for i in texts], images, file_metadata
            )
        )

        image_descriptions_task = None
        image_upload_task = None
        text_task = None

        # Process texts (relatively quick)
        if texts:
            text_task = asyncio.create_task(
                self._create_and_add_text_chunks(texts, file_metadata)
            )

        # Process images and upload them
        if images:
            await summary_task
            image_descriptions_task = asyncio.create_task(self._process_images(images))

            # Wait for image descriptions
            image_descriptions = await image_descriptions_task

            # Create and add image chunks
            image_result, image_metadatas = await self._create_and_add_image_chunks(
                images, image_descriptions, file_metadata
            )

            image_upload_task = asyncio.create_task(
                self.image_container_client.upload_base64_image_to_blob(
                    (i["chunk_id"] for i in image_metadatas),
                    (image.image_base64 for image in images),
                )
            )

        # Wait for all remaining tasks and summary
        await summary_task
        if texts:
            await asyncio.gather(text_task)
        if images:
            await asyncio.gather(image_upload_task)

        if images:
            logger.info(
                f"Saved images to blob container {self.image_container_client.container_name}"
            )

        if not (texts or images):
            logger.info(f"Neither text nor image found in {file.file_name}")

        logger.info(f"Processed file {file.file_name}")

        return {
            "file_name": file.file_name,
            "num_pages": len(doc),
            "num_texts": len(texts),
            "num_images": len(images),
            "metadata": file_metadata,
        }
