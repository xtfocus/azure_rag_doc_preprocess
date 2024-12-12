# Azure naive ingestion pipeline for image and text retrieval

A dual-indexing system

todo:

Indexing:
- [x] extract_texts_and_images takes too long for some reason. need checks
- [x] add summary image description before embedding
- Recognize complex pages and handle them like a total image.
    - Recognize big text blocks within complex pages that can also be treated like normal text?


Retrieval (in my other repo):
- [x] add image to selected context chunks
- [x] file summary as the first retrieval round
