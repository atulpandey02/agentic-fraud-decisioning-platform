# =============================================================
# UNIT TESTS — structure-aware chunker (Priority 4)
# =============================================================
# Pure, no infra. The load-bearing property (Priority 4 item 3):
# EVERY chunk must respect CHUNK_MAX_WORDS, because the embedding
# model (all-MiniLM-L6-v2) SILENTLY truncates anything longer — an
# over-budget chunk isn't just untidy, its tail is dropped with no
# error, quietly degrading retrieval. So "one giant paragraph" must
# be split, even though the original design preferred never to.
# =============================================================

from fraud_platform.retrieval.chunker import DocumentChunker
from fraud_platform.retrieval.config import CHUNK_MAX_WORDS


def _doc(body: str) -> str:
    return f"# GEO_JUMP\n\n## Detection\n{body}\n"


class TestStructureAwareChunking:
    def test_small_section_is_one_chunk(self):
        chunks = DocumentChunker().chunk_document(
            _doc("A short section well under the limit."), "geo.md", "POLICY"
        )
        assert len(chunks) == 1
        assert chunks[0].pattern == "GEO_JUMP"
        assert chunks[0].section == "Detection"

    def test_missing_h1_raises(self):
        import pytest
        with pytest.raises(ValueError):
            DocumentChunker().chunk_document("## NoTitle\nbody", "x.md", "POLICY")

    def test_multi_paragraph_section_splits_on_paragraph_boundaries(self):
        # several moderate paragraphs -> grouped into >1 chunk, each
        # under budget, split at blank lines (no paragraph cut)
        para = " ".join(["word"] * 60)
        body = "\n\n".join([para] * 6)  # 360 words over 4 paragraphs' worth
        chunks = DocumentChunker().chunk_document(_doc(body), "geo.md", "POLICY")
        assert len(chunks) > 1
        for c in chunks:
            assert c.word_count <= CHUNK_MAX_WORDS, f"{c.word_count} > {CHUNK_MAX_WORDS}"


class TestOversizedParagraphRespectsLimit:
    """Priority 4 item 3 — the gap: a SINGLE paragraph longer than the
    budget must still be split so no chunk exceeds CHUNK_MAX_WORDS."""

    def test_single_giant_paragraph_is_split_under_limit(self):
        giant = " ".join(["word"] * (CHUNK_MAX_WORDS * 3))  # one paragraph, 3x the budget
        chunks = DocumentChunker().chunk_document(_doc(giant), "geo.md", "POLICY")
        assert len(chunks) >= 3
        for c in chunks:
            assert c.word_count <= CHUNK_MAX_WORDS, (
                f"chunk of {c.word_count} words exceeds the {CHUNK_MAX_WORDS} "
                f"hard limit — the embedder would silently truncate it"
            )

    def test_no_content_words_are_lost_when_splitting(self):
        # splitting must preserve the words, not drop the tail (the very
        # failure mode the limit exists to prevent)
        n = CHUNK_MAX_WORDS * 2
        giant = " ".join([f"w{i}" for i in range(n)])
        chunks = DocumentChunker().chunk_document(_doc(giant), "geo.md", "POLICY")
        joined = " ".join(c.text for c in chunks)
        for i in (0, n // 2, n - 1):
            assert f"w{i}" in joined

    def test_every_chunk_keeps_pattern_and_section_prefix(self):
        giant = " ".join(["word"] * (CHUNK_MAX_WORDS * 2))
        chunks = DocumentChunker().chunk_document(_doc(giant), "geo.md", "POLICY")
        for c in chunks:
            # each split chunk stays self-contained for isolated retrieval
            assert "Fraud Pattern: GEO_JUMP" in c.text
            assert "Detection" in c.text
