# =============================================================
# WEAVIATE CLIENT — schema management, upsert, hybrid search
# =============================================================
# Server is configured with DEFAULT_VECTORIZER_MODULE=none (see
# docker-compose.yml) — Weaviate does NOT embed anything itself.
# We embed everything ourselves via Embedder and hand Weaviate
# the finished vectors directly. This means every write AND every
# query needs both the text (for BM25) and the vector (for dense
# search) supplied explicitly — there's no shortcut where Weaviate
# embeds a query string for us.
# =============================================================

import logging
from typing import List, Dict, Optional

import weaviate
from weaviate.classes.config import Configure, Property, DataType
from weaviate.classes.query import MetadataQuery

from .config import (
    WEAVIATE_HOST,
    WEAVIATE_HTTP_PORT,
    WEAVIATE_GRPC_PORT,
    WEAVIATE_COLLECTION_NAME,
    HYBRID_SEARCH_ALPHA,
    HYBRID_SEARCH_DEFAULT_LIMIT,
)
from .chunker import Chunk

logger = logging.getLogger(__name__)


class FraudKnowledgeBase:
    """
    Manages the Weaviate collection holding fraud policy and
    case-study chunks, and provides hybrid search over it.

    Usage:
        kb = FraudKnowledgeBase()
        kb.connect()
        kb.create_schema()               # idempotent — safe to call every run
        kb.upsert_chunks(chunks, vectors)
        results = kb.hybrid_search("why did this transaction get flagged", query_vector)
        kb.close()
    """

    def __init__(self):
        self._client: Optional[weaviate.WeaviateClient] = None

    # ----------------------------------------------------------
    # CONNECTION
    # ----------------------------------------------------------
    def connect(self) -> "FraudKnowledgeBase":
        logger.info(f"Connecting to Weaviate at {WEAVIATE_HOST}:{WEAVIATE_HTTP_PORT}...")
        self._client = weaviate.connect_to_local(
            host=WEAVIATE_HOST,
            port=WEAVIATE_HTTP_PORT,
            grpc_port=WEAVIATE_GRPC_PORT,
        )
        if not self._client.is_ready():
            raise RuntimeError("Weaviate connection established but server not ready")
        logger.info("Weaviate connected and ready.")
        return self

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            logger.info("Weaviate connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ----------------------------------------------------------
    # SCHEMA
    # ----------------------------------------------------------
    def create_schema(self) -> None:
        """
        Create the FraudPolicyChunk collection if it doesn't
        already exist. Idempotent — safe to call on every ingest
        run without duplicating or erroring.

        vectorizer_config=Configure.Vectorizer.none() is the
        client-side confirmation matching the server's
        DEFAULT_VECTORIZER_MODULE=none — both sides must agree,
        or Weaviate will try (and fail, with no API key
        configured) to call an external embedding service.
        """
        if self._client.collections.exists(WEAVIATE_COLLECTION_NAME):
            logger.info(f"Collection '{WEAVIATE_COLLECTION_NAME}' already exists — skipping creation")
            return

        self._client.collections.create(
            name=WEAVIATE_COLLECTION_NAME,
            vectorizer_config=Configure.Vectorizer.none(),
            properties=[
                # text is what Weaviate's native BM25 keyword search
                # indexes against — this is the "sparse" half of
                # hybrid search, requiring zero extra configuration
                Property(name="text", data_type=DataType.TEXT),
                Property(name="pattern", data_type=DataType.TEXT),
                Property(name="section", data_type=DataType.TEXT),
                Property(name="source_file", data_type=DataType.TEXT),
                Property(name="document_type", data_type=DataType.TEXT),
            ],
        )
        logger.info(f"Collection '{WEAVIATE_COLLECTION_NAME}' created.")

    # ----------------------------------------------------------
    # WRITE
    # ----------------------------------------------------------
    def upsert_chunks(self, chunks: List[Chunk], vectors: List[List[float]]) -> int:
        """
        Batch insert chunks with their pre-computed vectors.
        chunks and vectors must be the same length and same order
        — this is the caller's responsibility (ingest.py builds
        both from the same list in lockstep).
        """
        if len(chunks) != len(vectors):
            raise ValueError(
                f"chunks ({len(chunks)}) and vectors ({len(vectors)}) "
                f"length mismatch — cannot upsert"
            )

        collection = self._client.collections.get(WEAVIATE_COLLECTION_NAME)
        count = 0
        with collection.batch.dynamic() as batch:
            for chunk, vector in zip(chunks, vectors):
                batch.add_object(
                    properties={
                        "text": chunk.text,
                        "pattern": chunk.pattern,
                        "section": chunk.section,
                        "source_file": chunk.source_file,
                        "document_type": chunk.document_type,
                    },
                    vector=vector,
                )
                count += 1

        failed = collection.batch.failed_objects
        if failed:
            logger.error(f"{len(failed)} objects failed to upsert — see Weaviate batch errors")
        logger.info(f"Upserted {count - len(failed)} chunks into '{WEAVIATE_COLLECTION_NAME}'")
        return count - len(failed)

    def clear_collection(self) -> None:
        """
        Delete and recreate the collection — used when re-ingesting
        from scratch after editing policy docs, so stale chunks
        from a previous version don't linger alongside new ones.
        """
        if self._client.collections.exists(WEAVIATE_COLLECTION_NAME):
            self._client.collections.delete(WEAVIATE_COLLECTION_NAME)
            logger.info(f"Collection '{WEAVIATE_COLLECTION_NAME}' deleted.")
        self.create_schema()

    # ----------------------------------------------------------
    # READ — HYBRID SEARCH
    # ----------------------------------------------------------
    def hybrid_search(
        self,
        query_text: str,
        query_vector: List[float],
        alpha: float = HYBRID_SEARCH_ALPHA,
        limit: int = HYBRID_SEARCH_DEFAULT_LIMIT,
        pattern_filter: Optional[str] = None,
    ) -> List[Dict]:
        """
        Hybrid search combining BM25 keyword scoring and vector
        similarity, natively fused by Weaviate — no manual score
        normalization needed, unlike Pinecone's sparse-dense
        pattern which requires explicit rebalancing before
        production use.

        alpha controls the blend: 0.0 = pure keyword, 1.0 = pure
        vector, values in between blend the two. Both query_text
        AND query_vector must be supplied — Weaviate has no
        vectorizer configured, so it cannot embed the query text
        itself for the vector half of the search.

        pattern_filter optionally restricts results to one fraud
        pattern (e.g. "GEO_JUMP") — useful once the agent already
        knows which pattern triggered a flag and just needs that
        pattern's policy detail, not a broad search across all four.
        """
        collection = self._client.collections.get(WEAVIATE_COLLECTION_NAME)

        query_kwargs = dict(
            query=query_text,
            vector=query_vector,
            alpha=alpha,
            limit=limit,
            return_metadata=MetadataQuery(score=True),
        )

        if pattern_filter:
            from weaviate.classes.query import Filter
            query_kwargs["filters"] = Filter.by_property("pattern").equal(pattern_filter)

        response = collection.query.hybrid(**query_kwargs)

        results = []
        for obj in response.objects:
            results.append({
                "text": obj.properties["text"],
                "pattern": obj.properties["pattern"],
                "section": obj.properties["section"],
                "source_file": obj.properties["source_file"],
                "document_type": obj.properties["document_type"],
                "score": obj.metadata.score,
            })
        return results