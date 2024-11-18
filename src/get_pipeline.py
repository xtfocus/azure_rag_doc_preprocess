from openai import AsyncAzureOpenAI

from src.get_vector_stores import get_vector_stores
from src.image_descriptor import ImageDescriptor
from src.pipeline import Pipeline
from src.splitters import SimplePageTextSplitter
from src.vector_stores import MyAzureOpenAIEmbeddings


def get_pipeline(config, oai_client: AsyncAzureOpenAI) -> Pipeline:

    vector_stores = get_vector_stores(config)
    image_vector_store = vector_stores["image_vector_store"]
    text_vector_store = vector_stores["text_vector_store"]
    image_descriptor = ImageDescriptor(
        oai_client,
        config,
        "You are a useful AI assistant who can describe images. You will be provided an image. "
        "Describe the image in details in Markdown format. Preserve numeric information. Do not "
        "add your own information or assumption.",
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
    )
    return pipeline
