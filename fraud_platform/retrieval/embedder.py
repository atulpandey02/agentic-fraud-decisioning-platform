# =============================================================
# EMBEDDER — wraps sentence-transformers, guards the silent
# truncation behavior of all-MiniLM-L6-v2
# =============================================================
# Verified directly against HuggingFace's model card and the
# sentence-transformers documentation: this model truncates any
# input over 256 word-piece tokens with NO warning and NO error
# — it just silently drops the tail. CHUNK_MAX_WORDS in config.py
# is the first line of defense (chunks are built well under this
# ceiling), but this class adds a second, explicit check at
# embedding time so a truncation risk is logged loudly rather
# than discovered later as "retrieval quality feels a bit off"
# with no obvious cause.
# =============================================================

import logging
from typing import List

from sentence_transformers import SentenceTransformer

from .config import EMBEDDING_MODEL_NAME, EMBEDDING_MAX_TOKENS

logger = logging.getLogger(__name__)


class Embedder:
    """
    Thin wrapper around SentenceTransformer, with an explicit
    tokenizer-based length check before embedding — not just a
    word-count guess, but the model's OWN tokenizer, so we know
    with certainty whether a given chunk will be truncated.
    """

    def __init__(self, model_name: str = EMBEDDING_MODEL_NAME):
        logger.info(f"Loading embedding model: {model_name}")
        self._model = SentenceTransformer(model_name)
        actual_max_seq_length = self._model.max_seq_length
        if actual_max_seq_length != EMBEDDING_MAX_TOKENS:
            logger.warning(
                f"Model's actual max_seq_length ({actual_max_seq_length}) "
                f"differs from config's EMBEDDING_MAX_TOKENS "
                f"({EMBEDDING_MAX_TOKENS}) — update config.py to match."
            )
        logger.info(f"Embedding model ready. Max sequence length: {actual_max_seq_length}")

    def check_truncation_risk(self, text: str) -> bool:
        """
        Returns True if this text would be truncated by the model.
        Uses the model's own tokenizer directly — not a word-count
        approximation — since a single word can expand into
        multiple sub-word tokens, meaning a text that "looks" short
        by word count can still exceed the token ceiling.
        """
        token_count = len(self._model.tokenizer.encode(text))
        return token_count > self._model.max_seq_length

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a batch of texts. Logs a loud warning for any text
        that would be truncated, BEFORE embedding — so a silent
        quality problem becomes a visible one in the logs instead.
        """
        for i, text in enumerate(texts):
            if self.check_truncation_risk(text):
                token_count = len(self._model.tokenizer.encode(text))
                logger.warning(
                    f"Chunk {i} will be TRUNCATED: {token_count} tokens > "
                    f"{self._model.max_seq_length} max. Content past the "
                    f"limit will be silently dropped by the model. "
                    f"First 80 chars: {text[:80]!r}"
                )

        embeddings = self._model.encode(texts, show_progress_bar=False)
        return embeddings.tolist()

    def embed_query(self, query: str) -> List[float]:
        """
        Embed a single query string for search. Same truncation
        check applies — a long, rambling query risks losing its
        actual intent if it runs past the token ceiling.
        """
        if self.check_truncation_risk(query):
            logger.warning(f"Query will be TRUNCATED: {query[:80]!r}")
        return self._model.encode(query, show_progress_bar=False).tolist()