# =============================================================
# RETRIEVAL LAYER CONFIGURATION — Phase 2
# =============================================================
# All tunable values for chunking, embedding, and Weaviate live
# here — same pattern as every other phase (config.py separate
# from logic classes, so a threshold change never requires
# touching engine code).
# =============================================================

import os

# -------------------------------------------------------------
# EMBEDDING MODEL
# all-MiniLM-L6-v2 — same model used in the prior Enterprise
# Document Intelligence RAG project. 384-dimensional output.
#
# CRITICAL constraint, verified via HuggingFace docs and the
# sentence-transformers source directly: this model SILENTLY
# truncates any input longer than 256 word-piece tokens — no
# warning, no error, it just drops the tail and embeds only
# what fits. This is why CHUNK_MAX_WORDS below is set well
# below that ceiling, not right up against it — tokenization
# can split a single word into multiple sub-word tokens, so a
# 200-word chunk can easily exceed 256 tokens even though the
# word count looks safe at a glance.
# -------------------------------------------------------------
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIMENSIONS = 384
EMBEDDING_MAX_TOKENS = 256          # hard model ceiling — informational only,
                                     # do not rely on this number directly;
                                     # use CHUNK_MAX_WORDS as the real budget

# -------------------------------------------------------------
# CHUNKING
# Structure-aware: one Markdown H2 section = one chunk. This
# only works because we author these documents ourselves and
# can design them for retrieval from the start, rather than
# fighting a generic splitter against unstructured prose.
#
# CHUNK_MAX_WORDS is a SAFETY NET, not the primary chunking
# mechanism — sections should naturally stay well under this.
# If a section runs long, split it further at paragraph
# boundaries rather than let the embedding model silently
# truncate it.
# -------------------------------------------------------------
CHUNK_MAX_WORDS = 180                # conservative vs. the 256-token ceiling
CHUNK_HEADER_PATTERN = r"^##\s+(.+)$"  # regex for H2 section boundaries
CHUNK_TITLE_PATTERN = r"^#\s+(.+)$"    # regex for the H1 document title

# Every chunk gets this prefix so it's self-contained when
# retrieved in isolation — the LLM never sees surrounding
# context, only the chunk itself, so the chunk must carry its
# own identity.
CHUNK_CONTEXT_PREFIX_TEMPLATE = "Fraud Pattern: {pattern}\n\n"

# -------------------------------------------------------------
# WEAVIATE
# DEFAULT_VECTORIZER_MODULE=none on the server side (see
# docker-compose.yml) means WE supply vectors directly — this
# client-side config just points at the container.
# -------------------------------------------------------------
WEAVIATE_HOST = os.getenv("WEAVIATE_HOST", "localhost")
WEAVIATE_HTTP_PORT = int(os.getenv("WEAVIATE_HTTP_PORT", 8090))
WEAVIATE_GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", 50051))
WEAVIATE_COLLECTION_NAME = "FraudPolicyChunk"

# -------------------------------------------------------------
# HYBRID SEARCH
# alpha=0.0 -> pure BM25 (keyword only)
# alpha=1.0 -> pure vector (semantic only)
# alpha=0.5 -> balanced — this is Weaviate's native hybrid
#              fusion, no manual score normalization required
#              (unlike Pinecone's sparse-dense pattern, which
#              needs explicit rebalancing before production use)
# -------------------------------------------------------------
HYBRID_SEARCH_ALPHA = 0.5
HYBRID_SEARCH_DEFAULT_LIMIT = 5

# -------------------------------------------------------------
# SOURCE DOCUMENT PATHS
# -------------------------------------------------------------
POLICY_DOCS_DIR = os.path.join(os.path.dirname(__file__), "policy_docs")
CASE_STUDIES_DIR = os.path.join(os.path.dirname(__file__), "case_studies")

# Document type tag — stored as a Weaviate property so retrieval
# can filter "policy only" vs "case study only" vs both.
DOC_TYPE_POLICY = "POLICY"
DOC_TYPE_CASE_STUDY = "CASE_STUDY"

# -------------------------------------------------------------
# THE FOUR FRAUD PATTERNS — must match feature_engine.py exactly
# Kept here (not re-derived) so the retrieval layer and the
# feature engine never silently drift apart on pattern names.
# -------------------------------------------------------------
FRAUD_PATTERNS = [
    "VELOCITY_SPIKE",
    "GEO_JUMP",
    "NEW_DEVICE",
    "AMOUNT_ANOMALY",
]