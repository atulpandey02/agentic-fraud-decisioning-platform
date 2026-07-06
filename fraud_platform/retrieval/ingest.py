# =============================================================
# INGEST — reads policy docs + case studies, chunks, embeds,
# uploads to Weaviate. Run once after authoring/editing docs.
# =============================================================

import os
import glob
import logging

from .config import POLICY_DOCS_DIR, CASE_STUDIES_DIR, DOC_TYPE_POLICY, DOC_TYPE_CASE_STUDY
from .chunker import DocumentChunker
from .embedder import Embedder
from .weaviate_client import FraudKnowledgeBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


def load_documents(directory: str, document_type: str, chunker: DocumentChunker) -> list:
    """Read every .md file in a directory and chunk it."""
    chunks = []
    pattern = os.path.join(directory, "*.md")
    files = sorted(glob.glob(pattern))

    if not files:
        logger.warning(f"No .md files found in {directory}")
        return chunks

    for filepath in files:
        filename = os.path.basename(filepath)
        with open(filepath, "r") as f:
            text = f.read()
        chunks.extend(chunker.chunk_document(text, source_file=filename, document_type=document_type))

    return chunks


def main():
    logger.info("=" * 60)
    logger.info("FRAUD KNOWLEDGE BASE — INGESTION")
    logger.info("=" * 60)

    chunker = DocumentChunker()

    logger.info(f"Reading policy documents from {POLICY_DOCS_DIR}...")
    policy_chunks = load_documents(POLICY_DOCS_DIR, DOC_TYPE_POLICY, chunker)

    logger.info(f"Reading case studies from {CASE_STUDIES_DIR}...")
    case_study_chunks = load_documents(CASE_STUDIES_DIR, DOC_TYPE_CASE_STUDY, chunker)

    all_chunks = policy_chunks + case_study_chunks
    if not all_chunks:
        logger.error("No chunks produced — check that .md files exist in policy_docs/ and case_studies/")
        return

    logger.info(f"Total chunks: {len(all_chunks)} ({len(policy_chunks)} policy, {len(case_study_chunks)} case study)")

    # Print a per-pattern breakdown so it's immediately visible
    # whether every fraud pattern actually got represented
    from collections import Counter
    pattern_counts = Counter(c.pattern for c in all_chunks)
    for pattern, count in sorted(pattern_counts.items()):
        logger.info(f"  {pattern}: {count} chunks")

    logger.info("Loading embedding model...")
    embedder = Embedder()

    logger.info("Embedding all chunks...")
    texts = [c.text for c in all_chunks]
    vectors = embedder.embed_batch(texts)

    logger.info("Connecting to Weaviate...")
    with FraudKnowledgeBase() as kb:
        kb.clear_collection()  # fresh start every ingest run — avoids
                                 # stale chunks from a previous doc
                                 # version lingering alongside new ones
        inserted = kb.upsert_chunks(all_chunks, vectors)

    logger.info("=" * 60)
    logger.info(f"INGESTION COMPLETE — {inserted} chunks in Weaviate")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()