# Azure customized ingestion pipeline for image and text retrieval

A dual-indexing system for RAG

Indexing:
- [x] extract_texts_and_images takes too long for some reason. need checks
- [x] add summary image description before embedding
- [x] Recognize complex pages and handle them like a total image.


Retrieval (in my other repo):
- [x] add image to selected context chunks
- [x] file summary as the first retrieval round

# Before pushing:

- change dockerignore and Docker back to prod standard
- limit openai access
