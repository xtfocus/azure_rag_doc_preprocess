import asyncio
import gc
import time
from typing import (Any, Callable, Dict, List, NamedTuple, Optional, TypedDict,
                    Union)

from loguru import logger

from src.azure_container_client import AzureContainerClient
from src.docx_parsing import (doc_extract_texts_and_images,
                              docx_extract_texts_and_images)
from src.file_summarizer import FileSummarizer
from src.file_utils import detect_file_type
from src.image_descriptor import ImageDescription, ImageDescriptor
from src.image_utils import image_file_extract
from src.models import (BaseChunk, FileImage, FileText, MyFile, MyFileMetaData,
                        PageRange, SensitiveInformationDetectedException)
from src.pdf_utils.pdf_parsing import (pdf_extract_texts_and_images,
                                       pdf_extract_texts_and_images_batch)
from src.pii_scanning import check_pii_async, check_sensitive_information
from src.splitters import SimplePageTextSplitter
from src.txt_utils import txt_extract_texts
from src.upload_metadata import create_file_upload_metadata
from src.vector_stores import MyAzureSearch


class ProcessingResult(TypedDict):
    """Structured return type for process_file method"""

    file_name: str
    num_pages: int
    num_texts: int
    num_images: int
    metadata: Any
    errors: Optional[Dict[str, Any]]


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
        pii_service_endpoint: str,
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
        self.pii_service_endpoint = pii_service_endpoint

    async def _process_images(
        self, images: List[FileImage], summary, max_concurrent_requests: int = 50
    ) -> List[ImageDescription | None]:
        """Process multiple images concurrently with rate limiting using a semaphore."""
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def process_single_image(image):
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

    async def _add_text_chunks(self, text_chunking_output):
        result = await self.text_vector_store.add_entries(
            texts=text_chunking_output["texts"],
            metadatas=text_chunking_output["metadatas"],
        )
        return result

    async def _create_and_add_text_chunks(
        self, texts: List[FileText], file_metadata: MyFileMetaData, chunking=True
    ):
        """Combine creation and adding of text chunks"""
        if not texts:
            return None

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
        return result

    async def _create_and_add_image_chunks(
        self,
        images: List[FileImage],
        descriptions: List[ImageDescription],
        file_metadata: MyFileMetaData,
    ) -> Dict[str, Any]:
        """
        Combine creation and adding of image chunks
        Remove chunk that are not of interest
        """
        if not images:
            return {"status": "no_images", "image_metadatas": []}

        REMOVE_IMAGES = ["logo", "shape", "icon"]  # This should be declared in a config
        # Filter images and descriptions based on image_type
        filtered_images = []
        filtered_descriptions = []
        for image, description in zip(images, descriptions):
            if description.image_type not in REMOVE_IMAGES:
                filtered_images.append(image)
                filtered_descriptions.append(description.image_description)

            else:
                logger.debug(f"removed {description.image_type}")

        if not filtered_images:
            return {"status": "no_relevant_images", "image_metadatas": []}

        image_chunking_output = self._create_image_chunks(
            filtered_images, filtered_descriptions, file_metadata
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
        logger.debug(f"file_metadata = {file_metadata}")
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

    @staticmethod
    def extract_texts_and_images(
        file: MyFile,
    ) -> Dict[str, Union[List[FileText], List[FileImage]]]:
        extraction: Dict = {"texts": [], "images": [], "num_pages": None}

        file_type: str = detect_file_type(file.file_content)

        logger.debug(f"File type {file_type} detected")
        if file_type == "pdf":
            extraction = pdf_extract_texts_and_images(file.file_content)
        elif file_type == "docx":
            extraction = docx_extract_texts_and_images(file.file_content)
        elif file_type == "doc":
            extraction = doc_extract_texts_and_images(file.file_content)
        elif file_type == "txt":
            extraction = txt_extract_texts(file.file_content)
        elif file_type in ("jpg", "jpeg", "png"):
            extraction = image_file_extract(file.file_content)
        else:
            raise ValueError(f"File type {file_type} not supported")

        return extraction

    async def process_file_pdf(
        self, file: MyFile, pii_scanning: bool
    ) -> ProcessingResult:
        import gc

        errors = []
        file_name = file.file_name
        file_metadata = create_file_upload_metadata(file)
        summary = ""
        batch_size = 500  # Adjust based on memory constraints

        try:
            extraction_gen = pdf_extract_texts_and_images_batch(
                file.file_content, batch_size=batch_size
            )
            first_batch = next(extraction_gen)

            # Generate summary from the first batch
            if first_batch["texts"] or first_batch["images"]:
                summary = await self._create_summary(
                    [text.text for text in first_batch["texts"]], first_batch["images"]
                )
                logger.info(f"Generated summary from the first batch: {summary}")

            # Process the first batch
            await self._process_batch(
                first_batch, file_metadata, summary, pii_scanning, errors
            )

            # Clean up first batch
            try:
                # Explicitly delete the first batch data
                del first_batch["texts"]
                del first_batch["images"]
                del first_batch["tables"]
                del first_batch
                gc.collect()
            except Exception as e:
                logger.warning(f"Error cleaning up first batch: {e} ")

            # Process remaining batches
            for batch in extraction_gen:
                await self._process_batch(
                    batch, file_metadata, summary, pii_scanning, errors
                )

                try:
                    # Explicitly delete the first batch data
                    del batch["texts"]
                    del batch["images"]
                    del batch["tables"]
                    del batch
                    gc.collect()
                except Exception as e:
                    logger.warning(f"Error cleaning up first batch: {e} ")

            return ProcessingResult(
                file_name=file_name,
                num_pages=0,
                num_texts=0,
                num_images=0,
                metadata=file_metadata.model_dump(),
                errors=errors if errors else [],
            )

        except Exception as e:
            logger.error(f"Fatal error processing {file_name}: {str(e)}")
            if isinstance(e, SensitiveInformationDetectedException):
                error_type = "sensitive_information_detected"
                error_data = e.detected_data

            else:
                error_type = "unclassified"
                error_data = [str(e)]

            return ProcessingResult(
                file_name=file_name,
                num_pages=0,
                num_texts=0,
                num_images=0,
                metadata={},
                errors={"error_type": error_type, "error_data": error_data},
            )

    async def _process_batch(self, batch, file_metadata, summary, pii_scanning, errors):
        """Process a single batch of pages"""
        texts = batch["texts"]
        images = batch["images"]
        tables = batch["tables"]

        # PII Scanning (only for the first batch)
        if pii_scanning:
            logger.debug("scanning batch")
            try:
                pii_scan_result = await check_pii_async(
                    service_endpoint=self.pii_service_endpoint,
                    documents=[
                        dict(
                            doc_name=file_metadata.title,
                            doc_file_text=[i.model_dump() for i in texts],
                            language="ja",  # Japanese
                        )
                    ],
                )
                logger.debug(pii_scan_result)
                detected_data = check_sensitive_information(pii_scan_result)

            except Exception as e:
                errors.append(f"PII scanning error: {str(e)}")
                raise

            if detected_data:
                logger.error(
                    f"PII Scanning found issues. Will not index this file: {file_metadata.title}. \n"
                )
                raise SensitiveInformationDetectedException(detected_data)

        try:
            await self._add_file_summary_to_store(summary, file_metadata)
            logger.info("Indexed File Summary")
        except Exception as e:
            logger.error(f"Error indexing summary {e}")
            raise

        # Index texts
        if texts:
            try:
                text_chunking_output = self._create_text_chunks(texts, file_metadata)
                await self._add_text_chunks(text_chunking_output)
            except Exception as e:
                errors.append(f"Text indexing error: {str(e)}")

        # Index tables
        if tables:
            try:
                await self._create_and_add_text_chunks(
                    tables, file_metadata, chunking=False
                )
            except Exception as e:
                errors.append(f"Table indexing error: {str(e)}")

        # Process images
        if images:
            try:
                descriptions = await self._process_images(images, summary)
                image_result = await self._create_and_add_image_chunks(
                    images, descriptions, file_metadata
                )
                await self.image_container_client.upload_base64_image_to_blob(
                    (meta.chunk_id for meta in image_result["image_metadatas"]),
                    (img.image_base64 for img in images),
                    file_metadata.model_dump(),
                )
            except Exception as e:
                errors.append(f"Image processing error: {str(e)}")

        # Free memory
        del texts, images, tables
        gc.collect()

    async def process_file(self, file: MyFile, pii_scanning: bool) -> ProcessingResult:
        """Process a single file through the pipeline with optimized concurrent operations"""

        errors = []
        file_name = file.file_name

        try:
            texts: List[FileText] = []
            images: List[FileImage] = []
            num_pages: int = 0

            # Convert PDF to document
            file_metadata: MyFileMetaData = create_file_upload_metadata(file)

            logger.info(f"Created file upload metadata: {file_metadata}")

            file_type: str = detect_file_type(file.file_content)

            logger.debug(f"File type {file_type} detected")
            if file_type == "docx":
                extraction = docx_extract_texts_and_images(file.file_content)
            elif file_type == "doc":
                extraction = doc_extract_texts_and_images(file.file_content)
            elif file_type == "txt":
                extraction = txt_extract_texts(file.file_content)
            elif file_type in ("jpg", "jpeg", "png"):
                extraction = image_file_extract(file.file_content)
            elif file_type == "pdf":
                return await self.process_file_pdf(file, pii_scanning)
            else:
                raise ValueError(f"File type {file_type} not supported")

            content_extraction_result = self.extract_texts_and_images(file)

            texts, images, tables, num_pages = (
                content_extraction_result.get("texts", []),
                content_extraction_result.get("images", []),
                content_extraction_result.get("tables", []),
                content_extraction_result.get("num_pages", 0),
            )
            logger.info("Extracted raw texts and images")
            logger.info(
                f"no. texts: {len(texts)}\nno. images: {len(images)}\nno. tables: {len(tables)}\nno. pages: {num_pages}"
            )

            summary = ""
            # Create tasks dict to track all async operations
            tasks = {}

            if pii_scanning:

                try:
                    ###### START SCANNING FOR SENSITIVE INFORMATION
                    logger.debug(f"Sending request to PII Scanning service ... ")

                    pii_scan_result = await check_pii_async(
                        service_endpoint=self.pii_service_endpoint,
                        documents=[
                            dict(
                                doc_name=file_name,
                                doc_file_text=[i.model_dump() for i in texts],
                                language="ja",  # Japanese
                            )
                        ],
                    )
                    # logger.debug(pii_scan_result)

                    detected_data = check_sensitive_information(pii_scan_result)

                except Exception as e:
                    logger.error(
                        f"PII PService Error: Failed to pii scan document with error {e}"
                    )
                    raise e

                if detected_data:
                    raise SensitiveInformationDetectedException(detected_data)

            text_chunking_output = self._create_text_chunks(
                texts, file_metadata, chunking=True
            )

            # Start summary generation if we have content
            if texts or images:
                tasks["summary"] = asyncio.create_task(
                    self._create_summary(
                        [i for i in text_chunking_output["texts"]], images
                    )
                )
            # Process texts if available
            if texts:
                tasks["text"] = asyncio.create_task(
                    self._add_text_chunks(text_chunking_output)
                )
            if tables:
                tasks["tables"] = asyncio.create_task(
                    self._create_and_add_text_chunks(
                        tables, file_metadata, chunking=False
                    )
                )

            # Wait for summary before processing images
            try:
                if "summary" in tasks:
                    summary = await tasks["summary"]
                    logger.info(f"Created and indexed summary for {file_name}")
                    tasks["summary_upload"] = asyncio.create_task(
                        self._add_file_summary_to_store(summary, file_metadata)
                    )
            except Exception as e:
                error_msg = f"Summary generation failed: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

            # Process images if available
            if images:
                try:
                    descriptions: List[ImageDescription] = await self._process_images(
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
                    error_msg = f"Image Processing failed: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Wait for all remaining tasks to complete
            try:
                await asyncio.gather(*tasks.values())
            except Exception as e:
                error_msg = f"Task completion error: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

            logger.info(f"Processed file {file_name}")

            return ProcessingResult(
                file_name=file_name,
                num_pages=num_pages,
                num_texts=len(texts),
                num_images=len(images),
                metadata=file_metadata.model_dump(),  # dict
                errors=errors if errors else [],  # list[str]
            )
        except Exception as e:
            logger.error(f"Fatal error processing {file_name}: {str(e)}")
            if isinstance(e, SensitiveInformationDetectedException):
                error_type = "sensitive_information_detected"
                error_data = e.detected_data

            else:
                error_type = "unclassified"
                error_data = [str(e)]

            return ProcessingResult(
                file_name=file_name,
                num_pages=0,
                num_texts=0,
                num_images=0,
                metadata={},
                errors={"error_type": error_type, "error_data": error_data},
            )
