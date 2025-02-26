from typing import List

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (SearchIndex, SemanticSearch,
                                                   VectorSearch)
from loguru import logger
from openai import AzureOpenAI

from src.models import AzureSearchDocMetaData, BaseChunk, MyFileMetaData


class MyAzureSearch:
    def __init__(
        self,
        azure_search_endpoint: str,
        azure_search_key: str,
        index_name: str,
        embedding_function,
        fields: List,
        vector_search: VectorSearch,
        semantic_search: SemanticSearch,
    ):
        self.endpoint = azure_search_endpoint
        self.index_name = index_name
        self.fields = fields
        self.embedding_function = embedding_function

        # Create clients for interacting with the search service and index
        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(azure_search_key),
        )

        self.index_client = SearchIndexClient(
            endpoint=self.endpoint, credential=AzureKeyCredential(azure_search_key)
        )

        self.vector_search = vector_search
        self.semantic_search = semantic_search

        # Ensure the index exists or create it if not
        self._create_index_if_not_exists()

    def _create_index_if_not_exists(self):
        """Creates the index if it does not already exist."""
        try:
            # Check if the index exists
            self.index_client.get_index(name=self.index_name)
            logger.info(f"Index '{self.index_name}' already exists.")

        except ResourceNotFoundError:
            index = SearchIndex(
                name=self.index_name,
                fields=self.fields,
                vector_search=self.vector_search,
                semantic_search=self.semantic_search,
            )
            self.index_client.create_index(index)
            logger.info(f"Index '{self.index_name}' has been created.")

    async def upload_documents(self, documents):
        """Uploads documents to the Azure Search index."""
        self.search_client.upload_documents(documents=documents)

    @staticmethod
    def filtered_texts_and_metadatas_by_min_length(
        texts: List[str], metadatas: List[AzureSearchDocMetaData], min_len=10
    ):
        """
        Filter chunk by its length. Useful to remove small details
        """

        # Filter texts and metadatas where text length is >= 10
        filtered_batch = [
            (text, metadata)
            for text, metadata in zip(texts, metadatas)
            if len(text) >= min_len
        ]

        # Unpack the filtered batch back into separate lists
        filtered_texts, filtered_metadatas = (
            zip(*filtered_batch) if filtered_batch else ([], [])
        )

        diff = len(texts) - len(filtered_texts)

        if diff:
            logger.info(
                f"{diff} texts removed by length: {[i for i in texts if len(i) < min_len]}"
            )

        return filtered_texts, filtered_metadatas

    async def add_entries(
        self,
        texts: List[str],
        metadatas: List[AzureSearchDocMetaData],
        batch_size: int = 500,
        filter_by_min_len: int = 0,
    ):
        """Adds texts and their associated metadata to the Azure Search index."""
        documents = []
        n_texts = len(texts)

        for i in range(0, len(texts), batch_size):
            logger.debug(f"working on {i}/{n_texts}")
            batch_texts = texts[i : i + batch_size]
            batch_metadatas = metadatas[i : i + batch_size]

            if filter_by_min_len:
                filtered_texts, filtered_metadatas = (
                    self.filtered_texts_and_metadatas_by_min_length(
                        batch_texts, batch_metadatas, min_len=filter_by_min_len
                    )
                )
            else:
                filtered_texts, filtered_metadatas = batch_texts, batch_metadatas

            if not bool(filtered_texts):
                continue

            try:
                # Batch embedding request
                embeddings = self.embedding_function(filtered_texts)
            except Exception as e:
                logger.error(f" Error during text embedding for batch {i}: {str(e)}")
                logger.error(
                    "Showing batch \n" + "<end>\n---\n<start>".join(filtered_texts)
                )
                raise

            for text, embedding, metadata in zip(
                filtered_texts, embeddings, filtered_metadatas
            ):
                doc = {
                    "chunk": text or "no description",
                    "vector": embedding,
                }
                doc.update(metadata.model_dump())
                documents.append(doc)

        if documents:
            # Upload prepared documents to the index
            upload_success = await self.upload_documents(documents)
            return upload_success

    @staticmethod
    def create_texts_and_metadatas(
        chunks: List[BaseChunk], metadata: MyFileMetaData, prefix="text"
    ):
        """
        Given BaseChunk and Parent file metadata, prepare texts and metadata to
        be used with `add_entries`
        """
        # Extract texts and metadata
        texts = [chunk.chunk for chunk in chunks]
        metadatas = [
            AzureSearchDocMetaData.from_chunk(
                chunk, prefix=prefix, file_metadata=metadata
            )
            for chunk in chunks
        ]

        return {"texts": texts, "metadatas": metadatas}


class MyAzureOpenAIEmbeddings:
    def __init__(
        self,
        api_key: str,
        api_version: str,
        azure_endpoint: str,
        model: str,
        dimensions: str,
    ):
        """
        Initializes the MyAzureOpenAIEmbeddings instance.

        Args:
            api_key (str): Azure OpenAI API key.
            api_version (str): Azure OpenAI API version.
            azure_endpoint (str): Azure OpenAI endpoint.
            model (str): The embedding model deployment name.
        """
        self.client = AzureOpenAI(
            api_key=api_key, api_version=api_version, azure_endpoint=azure_endpoint
        )
        self.model = model
        self.dimensions = int(dimensions)

    def embed_query(self, texts: List[str]) -> List[list]:
        """
        Generates embeddings for a batch of texts.

        Args:
            texts (List[str]): List of input texts to generate embeddings for.

        Returns:
            List[list]: List of embedding vectors.
        """
        response = self.client.embeddings.create(
            input=texts, model=self.model, dimensions=self.dimensions
        )
        return [item.embedding for item in response.data]
