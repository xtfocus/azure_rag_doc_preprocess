from openai import AsyncAzureOpenAI

from src.azure_container_client import AzureContainerClient
from src.file_summarizer import FileSummarizer
from src.get_vector_stores import get_vector_stores
from src.image_descriptor import ImageDescriptor
from src.pipeline import Pipeline
from src.splitters import SimplePageTextSplitter
from src.vector_stores import MyAzureOpenAIEmbeddings


def get_pipeline(
    config,
    oai_client: AsyncAzureOpenAI,
    image_container_client: AzureContainerClient,
    pii_service_endpoint: str,
) -> Pipeline:

    vector_stores = get_vector_stores(config)
    image_vector_store = vector_stores["image_vector_store"]
    summary_vector_store = vector_stores["summary_vector_store"]
    text_vector_store = vector_stores["text_vector_store"]

    file_summarizer = FileSummarizer(
        oai_client,
        config,
        """
        Task: Document Summary
        Instruction: 
            Identify key information:
                - Document Type and Purpose:
                     Identify the type of document (e.g., report, agreement, article) and its main purpose.
                - Entities:
                    List all entities prominently mentioned in this document: organizations (groups, companies, facilities, departments, etc.), people, location etc.
                - Location and Context:
                     Include relevant locations or settings if applicable.
                - Main Topics:
                     Highlight the primary topics or issues addressed in the document.
                - Timeframe:
                     Note any specific dates or time periods covered by the document.
                - Important Details:
                     Summarize key points, findings, or conclusions.
                - Legal or Compliance Aspects:
                     If applicable, mention any legal, compliance, or regulatory elements.
                - Purpose and Implications:
                     Explain the broader implications or intended outcomes of the document.

            By following these steps, the summary will capture the essential elements needed for accurate retrieval.
            
        Task 2: Generate 10 QA pairs based on information in the document
        Instruction: We desire Specific questions with specific details are preferred because they give the full context
            Examples of generic questions (undesirable):
                'What was the rate of change according to the agreement?' (undesirable because it uses `the agreement` instead of giving the full context)
                'How many people were involved?' (involved in what, for what? This is too generic)
                'What was the limit for extending work hours under this document (undesitable because it mentions some document without giving the whole context)
            Examples of specific questions (desirable):
                'What was the growth rate of interest rate in 6th month according to the loan agreement by Techcombank for priority customers?' (Very specific which documentation is being referred to, full context achieved)
                'How many researchers from Stanford University participated in the climate change study conducted in Arctic regions between 2020-2022?'
                'What was the limit for extending work hours according to Nike Vietnam factory regulation in 2023?'
            Follow this pattern to create Specific questions that incorporate the specific who, what, where, when, and why from the document. Avoid Generic questions.

        Following is sampled content from a document. Provide a summarization and 10 QA pairs as instructed.
        """,
    )

    image_descriptor = ImageDescriptor(
        oai_client,
        config,
        """Task: Image-to-Text Conversion
        
        Objective: Transform the provided image of a document into text form without information loss.
        
        Instructions: Detect image type AND Create image description
            1. Detect image type: available type include: "icon", "shape", "logo", "picture", "information" where:
              + "shape" applies to simple shapes such as lines, boxes, curves, etc.
              + "information" applies to documents, diagrams, or infographic
              + "picture" applies to pictures of things, except icons, shapes, logos
              + "icon" and "logo" are self-explanatory

            2. Create description
            If image type if one of "icon", "shape", "logo", leave image description blank
            Otherwise (for "document" and "picture"):
                - Examine the Image: Carefully look at the document to identify sections, headings, and any structured information.
                - Identify Key Elements:
                    + Note the name of the organization or company involved.
                    + Highlight every specific terms, conditions, or numerical data.
                    + Pay attention to dates, names, and signatures that might indicate the document’s purpose or validity period.
                - Produce output: 
                    + If the image is a document, simply write down all the text from the image as accurately as possible. Pay attention to details like numbers, dates, and specific terms. If tables exists, transcribe them in valid Markdown tables (consistent column count between headers and data). Stay true to the source material, do not include introduction or commentary. 
                    + If the image is a picture, describe it in details

        Review for Accuracy: Double-check your transcription for any errors or omissions to ensure completeness.
        Submit Your Work: Provide the transcribed text in a clear and organized format.
        By completing this task, you will help capture the detailed content and context of the document.
        """,
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
            "```Markdown",
            "```",
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
        text_vector_store=text_vector_store,
        image_vector_store=image_vector_store,
        summary_vector_store=summary_vector_store,
        embedding_function=my_embedding_function,
        text_splitter=text_splitter,
        image_descriptor=image_descriptor,
        file_summarizer=file_summarizer,
        image_container_client=image_container_client,
        pii_service_endpoint=pii_service_endpoint,
    )
    return pipeline
