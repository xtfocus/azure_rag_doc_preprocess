from openai import AsyncAzureOpenAI

from src.azure_container_client import AzureContainerClient
from src.get_vector_stores import get_vector_stores
from src.image_descriptor import ImageDescriptor
from src.pipeline import Pipeline
from src.splitters import SimplePageTextSplitter
from src.vector_stores import MyAzureOpenAIEmbeddings


def get_pipeline(
    config, oai_client: AsyncAzureOpenAI, image_container_client: AzureContainerClient
) -> Pipeline:

    vector_stores = get_vector_stores(config)
    image_vector_store = vector_stores["image_vector_store"]
    text_vector_store = vector_stores["text_vector_store"]

    image_descriptor = ImageDescriptor(
        oai_client,
        config,
        """Convert the content of the uploaded image into meaningful text for use in a Q&A platform where user can seek answers from a knowledge base of thousands of corporate documents.
        You must follow these rules:
        - Detect if the image carry no information of interest:
            + if the image is a simple shape (e.g.,. line, boxes,), simply return 'a shape' then terminate.
            + if the image is a logo, simply return 'a logo' then terminate.
            + No further processing needed. Simply terminate
        - Otherwise, extract all informative facts from the image
            + Preserve all details facts. Summarization is forbidden because it results in information loss.
            + Use a clear, natural tone in paragraphs.
            + Tables (if exists) must be convert to paragraphs.
            + Preserve all numeric values and quantities related details in the final output.
            + In the end, generate a list of up to 5 standalone questions. A standalone question is explicit, minimum reference language ('this', 'that', 'the'), and makes sense on itself without the need for additional contexts nor reference resolution.
            + Output in the Markdown format
        - Refrain from providing your own additional commentaries or thought process.""",
    )

    my_embedding_function = MyAzureOpenAIEmbeddings(
        api_key=config.AZURE_OPENAI_API_KEY,
        api_version=config.AZURE_OPENAI_API_VERSION,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        model=config.AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
        dimensions=config.AZURE_OPENAI_EMBEDDING_DIMENSIONS,
    ).embed_query

    text_splitter = SimplePageTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=[
            "\n\n",  # Paragraph boundaries
            ".\n",  # English sentence end with newline
            ". ",  # English sentence end with space
            ", ",  # English comma with space
            "",  # Fallback to no separators
            "。",  # Japanese period
            "、",  # Japanese comma
            "！",  # Japanese exclamation mark
            "？",  # Japanese question mark
            "「",  # Start of a quote
            "」",  # End of a quote
            "『",  # Start of emphasized text
            "』",  # End of emphasized text
            " ",  # Spaces
            "\n",  # Line breaks
        ],
    )

    pipeline = Pipeline(
        text_vector_store,
        image_vector_store,
        my_embedding_function,
        text_splitter,
        image_descriptor,
        image_container_client,
    )
    return pipeline
