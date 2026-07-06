# =============================================================
# QUERY TEST — validate hybrid search retrieval quality
# =============================================================
# Run this after ingest.py to sanity-check that retrieval
# actually returns the RIGHT chunk for a range of query styles:
# exact keyword matches, paraphrased/semantic queries, and
# queries that mix both. This is the test that matters most —
# a vector store with wrong or irrelevant retrieval is worse
# than useless, since it would feed an agent confidently wrong
# context.
# =============================================================

import logging

from .embedder import Embedder
from .weaviate_client import FraudKnowledgeBase
from .config import HYBRID_SEARCH_ALPHA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


# Deliberately mixed test queries:
#   - exact keyword style (tests the BM25 half)
#   - paraphrased/semantic style (tests the vector half)
#   - a query with no exact keyword overlap at all (pure semantic test)
TEST_QUERIES = [
    "velocity spike threshold",                              # keyword-heavy
    "how many transactions in 15 minutes counts as suspicious",  # paraphrase of velocity
    "impossible travel between two cities",                  # paraphrase of geo_jump
    "someone bought something for exactly one dollar",        # paraphrase of card testing
    "should I block a transaction from a brand new phone",    # paraphrase of new_device
    "why does velocity have a blind spot",                    # tests case study + limitation reasoning
]


def print_results(query: str, results: list) -> None:
    print(f"\n{'=' * 70}")
    print(f"QUERY: {query}")
    print(f"{'=' * 70}")
    if not results:
        print("  (no results)")
        return
    for i, r in enumerate(results, 1):
        print(f"\n  [{i}] score={r['score']:.4f} | {r['pattern']} / {r['section']} ({r['document_type']})")
        preview = r["text"].replace("\n", " ")[:180]
        print(f"      {preview}...")


def main():
    logger.info("Loading embedder...")
    embedder = Embedder()

    with FraudKnowledgeBase() as kb:
        for query in TEST_QUERIES:
            query_vector = embedder.embed_query(query)
            results = kb.hybrid_search(
                query_text=query,
                query_vector=query_vector,
                alpha=HYBRID_SEARCH_ALPHA,
                limit=3,
            )
            print_results(query, results)

    print(f"\n{'=' * 70}")
    print("Review each result above: does the TOP result actually match")
    print("the query's intent? If a paraphrased query returns the wrong")
    print("pattern's chunk, that's a real retrieval quality problem to")
    print("investigate — not something to explain away.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()