import asyncio
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from loguru import logger
from pydantic import Base64Str

from src.azure_container_client import AzureContainerClient
from src.file_summarizer import FileSummarizer
from src.image_descriptor import ImageDescriptor
from src.models import BaseChunk, MyFile, MyFileMetaData, PageRange
from src.pdf_parsing import FileImage, FileText, extract_texts_images_tables
from src.pdf_utils import pdf_blob_to_pdfplumber_doc
from src.splitters import SimplePageTextSplitter
from src.upload_metadata import create_file_upload_metadata
from src.vector_stores import MyAzureSearch


class ProcessingResult(NamedTuple):
    """Structured return type for process_file method"""

    file_name: str
    num_pages: int
    num_texts: int
    num_images: int
    metadata: Any
    errors: Optional[List[str]] = None


class ProcessingError(Exception):
    """
    Custom exception to represent errors during file processing.

    Attributes:
        file_name (str): The name of the file that caused the error.
        num_pages (int): The number of pages processed (default is 0).
        num_texts (int): The number of text chunks processed (default is 0).
        num_images (int): The number of images processed (default is 0).
        metadata (dict): Additional metadata related to the file (default is empty).
        errors (list): List of error messages (default is an empty list).
    """

    def __init__(
        self,
        file_name: str,
        num_pages: int = 0,
        num_texts: int = 0,
        num_images: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        errors: Optional[List[str]] = None,
    ):
        self.file_name = file_name
        self.num_pages = num_pages
        self.num_texts = num_texts
        self.num_images = num_images
        self.metadata = metadata or {}
        self.errors = errors or []
        super().__init__(self.format_error())

    def format_error(self) -> str:
        """
        Format the error message for the exception.
        """
        return (
            f"ProcessingError in file '{self.file_name}': "
            f"pages={self.num_pages}, texts={self.num_texts}, images={self.num_images}. "
            f"Metadata: {self.metadata}. Errors: {self.errors}"
        )


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
        self, images: List[FileImage], summary: str, max_concurrent_requests: int = 20
    ) -> List[str]:
        """Process multiple images concurrently with rate limiting using a semaphore."""
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def process_single_image(image) -> str:
            async with semaphore:
                return await self.image_descriptor.run(image.image_base64, summary)

        tasks = [process_single_image(img) for img in images]
        return await asyncio.gather(*tasks)

    def _create_text_chunks(
        self, texts: List[FileText], file_metadata: MyFileMetaData, chunking=True
    ) -> Dict[str, List[Any]]:
        """Create text chunks and their metadata

        Args:
            texts: List of text objects
            file_metadata: Metadata about the file

        Returns:
            Tuple containing lists of texts and their metadata
        """
        if chunking:
            text_chunks: List[BaseChunk] = self.text_splitter.split_text(
                (text.model_dump() for text in texts)
            )
        else:
            text_chunks = [
                BaseChunk(
                    chunk_no=f"whole{i}",  # whole thing as a chunk
                    chunk=text.text,
                    page_range=PageRange(
                        start_page=text.page_no, end_page=text.page_no
                    ),
                )
                for i, text in enumerate(texts)
            ]
        return self.text_vector_store.create_texts_and_metadatas(
            text_chunks, file_metadata, prefix="text"
        )

    def _create_image_chunks(
        self,
        images: List[FileImage],
        descriptions: List[str],
        file_metadata: MyFileMetaData,
    ) -> Dict:
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

    async def _create_and_add_text_chunks(
        self, texts: List[FileText], file_metadata: MyFileMetaData, chunking=True
    ):
        """Combine creation and adding of text chunks"""
        if not texts:
            return None

        logger.debug("Start Indexing text chunks")
        text_chunking_output = self._create_text_chunks(
            texts, file_metadata, chunking=chunking
        )
        input_texts, input_metadatas = (
            text_chunking_output["texts"],
            text_chunking_output["metadatas"],
        )
        result = await self.text_vector_store.add_entries(
            texts=input_texts, metadatas=input_metadatas
        )
        logger.debug("Finish Indexing text chunks")
        return result

    async def _create_and_add_image_chunks(
        self,
        images: List[FileImage],
        descriptions: List[str],
        file_metadata: MyFileMetaData,
    ) -> Dict[str, Any]:
        """Combine creation and adding of image chunks"""
        if not images:
            return {"status": "no_images", "image_metadatas": []}

        image_chunking_output = self._create_image_chunks(
            images, descriptions, file_metadata
        )
        image_texts, image_metadatas = (
            image_chunking_output["texts"],
            image_chunking_output["metadatas"],
        )
        result = await self.image_vector_store.add_entries(
            texts=image_texts,
            metadatas=image_metadatas,
            filter_by_min_len=10,
        )
        return {"result": result, "image_metadatas": image_metadatas}

    async def _create_summary(self, texts: List[str], images: List[FileImage]) -> str:
        """Just create the summary"""

        return await self.file_summarizer.run(texts, images)

    async def _add_file_summary_to_store(
        self, summary: str, file_metadata: MyFileMetaData
    ):
        """Add the summary to vector store"""
        summary_output = self.summary_vector_store.create_texts_and_metadatas(
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
        summary_texts, summary_metadatas = (
            summary_output["texts"],
            summary_output["metadatas"],
        )

        return await self.summary_vector_store.add_entries(
            texts=summary_texts, metadatas=summary_metadatas
        )

    async def process_file(self, file: MyFile) -> ProcessingResult:
        """Process a single file through the pipeline with optimized concurrent operations"""

        errors = []
        file_name = file.file_name
        try:
            texts: List[FileText] = []
            images: List[FileImage] = []
            # Convert PDF to document
            with pdf_blob_to_pdfplumber_doc(file.file_content) as doc:
                # Create file metadata
                file_metadata: MyFileMetaData = create_file_upload_metadata(file)
                logger.info(f"Created file upload metadata: {file_metadata}")
                num_pages = len(doc.pages)
                extraction = extract_texts_images_tables(doc, report=True)
                texts, images, tables = (
                    extraction["texts"],
                    extraction["images"],
                    extraction["tables"],
                )
                logger.info("Extracted raw texts and images")

            summary = ""
            # Create tasks dict to track all async operations
            tasks = {}
            # Start summary generation if we have content
            if texts or images:
                tasks["summary"] = asyncio.create_task(
                    self._create_summary([i.text for i in texts], images)
                )
            # Process texts if available
            if texts:
                tasks["text"] = asyncio.create_task(
                    self._create_and_add_text_chunks(texts, file_metadata)
                )
            if tables:
                tasks["text"] = asyncio.create_task(
                    self._create_and_add_text_chunks(
                        tables, file_metadata, chunking=False
                    )
                )

            # Wait for summary before processing images
            try:
                if "summary" in tasks:
                    summary = await tasks["summary"]
                    logger.info(f"Created summary for {file_name}")
                    tasks["summary_upload"] = asyncio.create_task(
                        self._add_file_summary_to_store(summary, file_metadata)
                    )
            except Exception as e:
                logger.error(f"Summary generation failed: {str(e)}")
                errors.append(f"Summary generation failed: {str(e)}")
                raise e

            # Process images if available
            if images:
                try:
                    descriptions: List[str] = await self._process_images(
                        images,
                        summary=summary,
                    )

                    logger.info(f"Created image descriptions for {file_name}")

                    image_chunk_result = await self._create_and_add_image_chunks(
                        images, descriptions, file_metadata
                    )
                    image_metadatas = image_chunk_result["image_metadatas"]

                    logger.info(f"Created image index for {file_name}")

                    tasks["image_upload"] = asyncio.create_task(
                        self.image_container_client.upload_base64_image_to_blob(
                            (i.chunk_id for i in image_metadatas),
                            (image.image_base64 for image in images),
                            metadata=file_metadata.model_dump(),
                        )
                    )

                except Exception as e:
                    logger.error(f"Image processing failed: {str(e)}")
                    errors.append(f"Image processing failed: {str(e)}")

            # Wait for all remaining tasks to complete
            try:
                await asyncio.gather(*tasks.values())
            except Exception as e:
                logger.error(f"Task completion error: {str(e)}")
                errors.append(f"Task completion error: {str(e)}")

            logger.info(f"Processed file {file_name}")

            return ProcessingResult(
                file_name=file_name,
                num_pages=num_pages,
                num_texts=len(texts),
                num_images=len(images),
                metadata=file_metadata,
                errors=errors if errors else None,
            )
        except Exception as e:
            logger.error(f"Fatal error processing {file_name}: {str(e)}")
            return ProcessingResult(
                file_name=file_name,
                num_pages=0,
                num_texts=0,
                num_images=0,
                metadata={},
                errors=[f"Fatal error: {str(e)}"],
            )
