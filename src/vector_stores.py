import json
from typing import List

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (SearchIndex, SemanticSearch,
                                                   VectorSearch)
from loguru import logger
from openai import AzureOpenAI

from src.models import BaseChunk, FileMetadata


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

    async def add_texts(self, texts, metadatas=None):
        """Adds texts and their associated metadata to the Azure Search index."""
        documents = []

        for i, text in enumerate(texts):
            metadata = metadatas[i] if metadatas else {}
            doc = {
                "chunk_id": metadata[
                    "chunk_id"
                ],  # Generate a unique ID for each document
                "chunk": text
                or "no description",  # Make sure some text is there otherwise the embedding api raise error
                "vector": self.embedding_function(text),
                "metadata": json.dumps(metadata["metadata"]),
                "parent_id": metadata["parent_id"],
                "title": metadata["title"],
            }
            documents.append(doc)

        # Upload prepared documents to the index
        upload_success = await self.upload_documents(documents)
        return upload_success

    def search(self, search_text):
        """Performs a search on the Azure Search index."""
        if self.embedding_function:
            # Generate embedding for the search query if an embedding function is provided
            query_vector = self.embedding_function(search_text)
            results = self.search_client.search(search_text="", vector=query_vector)
        else:
            results = self.search_client.search(search_text)

        return [result for result in results]

    @staticmethod
    def create_texts_and_metadatas(
        chunks: BaseChunk, file_metadata: FileMetadata, prefix="text"
    ):
        """
        Given BaseChunk and Parent file metadata, prepare texts and metadata to
        be used with `add_texts`
        """
        # Extract texts and metadata
        texts = [chunk.chunk for chunk in chunks]
        metadatas = [
            {
                "chunk_id": f"{prefix}_{file_metadata['file_hash']}_{chunk.chunk_no}",
                "metadata": json.dumps({"page_range": chunk.page_range.dict()}),
                "title": file_metadata["title"],
                "parent_id": file_metadata["file_hash"],
            }
            for chunk in chunks
        ]

        return texts, metadatas


class MyAzureOpenAIEmbeddings:
    def __init__(self, api_key, api_version, azure_endpoint, model, dimensions):
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
        self.dimensions = dimensions

    def embed_query(self, text: str) -> list:
        """
        Generates embeddings for the given text.

        Args:
            text (str): The input text to generate embeddings for.

        Returns:
            list: The embedding vector.
        """
        response = self.client.embeddings.create(
            input=[text], model=self.model, dimensions=self.dimensions
        )
        return response.data[0].embedding
