# =============================================================
# DOCUMENT CHUNKER — structure-aware, not size-driven
# =============================================================
# Splits Markdown policy documents by H2 section rather than by
# a fixed character/token count. This only works because we
# author these documents ourselves and can design them for
# retrieval from the start — see the Phase 2 design discussion:
# fixed-size or recursive chunking would risk cutting a fraud
# rule apart from its own threshold; structure-aware chunking
# means one chunk = one coherent, retrievable idea, because we
# control where the sections fall.
#
# Every chunk is made self-contained by prepending the fraud
# pattern name — retrieved chunks are handed to an LLM in
# isolation with no surrounding context, so a chunk that just
# says "must exceed 5 in a 15-minute window" is meaningless on
# its own without that prefix.
# =============================================================

import re
import logging
from dataclasses import dataclass
from typing import List, Optional

from .config import (
    CHUNK_MAX_WORDS,
    CHUNK_HEADER_PATTERN,
    CHUNK_TITLE_PATTERN,
    CHUNK_CONTEXT_PREFIX_TEMPLATE,
)

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """
    One retrievable unit. Dataclass instead of a plain dict —
    gives us type-checked fields and IDE autocomplete for the
    properties every downstream consumer (embedder, Weaviate
    client) needs to agree on.
    """
    pattern: str            # e.g. "VELOCITY_SPIKE" — which fraud pattern
    section: str            # e.g. "Detection Threshold" — which H2 section
    text: str               # the actual chunk text, WITH the prefix applied
    source_file: str        # which file this came from, for traceability
    document_type: str      # "POLICY" or "CASE_STUDY"
    word_count: int         # computed at creation — used for validation


class DocumentChunker:
    """
    Parses a Markdown document into structure-aware chunks.

    Usage:
        chunker = DocumentChunker()
        chunks = chunker.chunk_document(markdown_text, source_file="geo_jump.md", document_type="POLICY")
    """

    def __init__(self):
        self._header_re = re.compile(CHUNK_HEADER_PATTERN, re.MULTILINE)
        self._title_re = re.compile(CHUNK_TITLE_PATTERN, re.MULTILINE)

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    def chunk_document(
        self, markdown_text: str, source_file: str, document_type: str
    ) -> List[Chunk]:
        """
        Parse one Markdown document into a list of Chunks.

        Flow:
            1. Extract the H1 title -> this is the fraud pattern name
            2. Split the body by H2 headers -> each section is a
               candidate chunk
            3. For each section: check word count against
               CHUNK_MAX_WORDS. If it fits, it's one chunk. If not,
               split further at paragraph boundaries — still
               self-contained, still tagged with pattern + section.
        """
        pattern = self._extract_title(markdown_text)
        if not pattern:
            raise ValueError(
                f"{source_file}: no H1 title found — every policy/case-study "
                f"doc must start with '# PATTERN_NAME'"
            )

        sections = self._split_by_h2(markdown_text)
        if not sections:
            # No H2 sections at all (e.g. a short case study) — treat
            # the whole body as one section called "Case Study" or
            # whatever text follows the H1, so it still gets chunked
            # consistently rather than falling through silently.
            body = self._title_re.sub("", markdown_text, count=1).strip()
            sections = [("Full Document", body)]

        chunks: List[Chunk] = []
        for section_name, section_body in sections:
            chunks.extend(
                self._build_chunks_for_section(
                    pattern, section_name, section_body, source_file, document_type
                )
            )

        logger.info(f"{source_file}: {len(chunks)} chunk(s) from {len(sections)} section(s)")
        return chunks

    # ----------------------------------------------------------
    # PARSING HELPERS
    # ----------------------------------------------------------
    def _extract_title(self, markdown_text: str) -> Optional[str]:
        match = self._title_re.search(markdown_text)
        return match.group(1).strip() if match else None

    def _split_by_h2(self, markdown_text: str) -> List[tuple]:
        """
        Split document body into (section_name, section_body) pairs
        at every H2 (## ) header. Text before the first H2 (usually
        just the H1 title line) is discarded here — it's already
        captured as the pattern name.
        """
        matches = list(self._header_re.finditer(markdown_text))
        if not matches:
            return []

        sections = []
        for i, match in enumerate(matches):
            section_name = match.group(1).strip()
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown_text)
            section_body = markdown_text[start:end].strip()
            sections.append((section_name, section_body))
        return sections

    # ----------------------------------------------------------
    # CHUNK BUILDING
    # ----------------------------------------------------------
    def _build_chunks_for_section(
        self,
        pattern: str,
        section_name: str,
        section_body: str,
        source_file: str,
        document_type: str,
    ) -> List[Chunk]:
        """
        One section becomes one chunk if it fits under
        CHUNK_MAX_WORDS. If not, split at paragraph boundaries
        (blank-line-separated) and group paragraphs into
        sub-chunks that each stay under budget.

        Paragraph boundaries are PREFERRED (splitting there keeps a
        thought intact), but they are not always sufficient: a single
        paragraph can itself exceed the budget. When it does, the
        HARD LIMIT wins over the tidiness preference — that paragraph
        is split at word boundaries into budget-sized pieces. This is
        deliberate: the embedding model silently truncates anything
        past its token ceiling, so an over-budget chunk loses its tail
        with no warning. A mid-paragraph split is strictly better than
        a silent truncation (Priority 4 item 3).
        """
        prefix = CHUNK_CONTEXT_PREFIX_TEMPLATE.format(pattern=pattern)
        full_text = f"{prefix}## {section_name}\n{section_body}"
        word_count = len(full_text.split())

        if word_count <= CHUNK_MAX_WORDS:
            return [
                Chunk(
                    pattern=pattern,
                    section=section_name,
                    text=full_text,
                    source_file=source_file,
                    document_type=document_type,
                    word_count=word_count,
                )
            ]

        # Section too long — split at paragraph boundaries and
        # regroup into sub-chunks, each still self-contained with
        # the same pattern + section prefix so retrieval in
        # isolation still makes sense for every piece.
        logger.warning(
            f"{source_file} [{section_name}]: {word_count} words exceeds "
            f"CHUNK_MAX_WORDS ({CHUNK_MAX_WORDS}) — splitting further"
        )

        # The prefix + header get added to EVERY sub-chunk in
        # _finalize_subchunk, so the grouping budget below must
        # leave room for that fixed overhead — otherwise a group
        # that fits CHUNK_MAX_WORDS on paragraph text alone can
        # still exceed it once the prefix is actually attached.
        # This was a real bug caught by testing against the actual
        # policy docs: one chunk landed at 181 words against a 180
        # budget, off by exactly the prefix's own word count.
        overhead_words = len(f"{prefix}## {section_name}\n".split())
        effective_budget = CHUNK_MAX_WORDS - overhead_words

        paragraphs = self._split_oversized_paragraphs(
            [p.strip() for p in section_body.split("\n\n") if p.strip()],
            effective_budget,
        )
        sub_chunks: List[Chunk] = []
        current_group: List[str] = []
        current_words = 0

        for para in paragraphs:
            para_words = len(para.split())
            if current_words + para_words > effective_budget and current_group:
                sub_chunks.append(
                    self._finalize_subchunk(
                        pattern, section_name, current_group, source_file, document_type
                    )
                )
                current_group = []
                current_words = 0
            current_group.append(para)
            current_words += para_words

        if current_group:
            sub_chunks.append(
                self._finalize_subchunk(
                    pattern, section_name, current_group, source_file, document_type
                )
            )

        return sub_chunks

    @staticmethod
    def _split_oversized_paragraphs(paragraphs: List[str], budget: int) -> List[str]:
        """
        Pre-pass before grouping: any single paragraph longer than
        `budget` words is broken into word-boundary pieces of at most
        `budget` words each. Paragraphs that already fit pass through
        untouched, so the paragraph-boundary preference still holds for
        everything that doesn't force the issue. This is what
        guarantees no downstream sub-chunk can exceed the budget on a
        single paragraph alone (Priority 4 item 3).
        """
        out: List[str] = []
        for para in paragraphs:
            words = para.split()
            if len(words) <= budget:
                out.append(para)
                continue
            for i in range(0, len(words), budget):
                out.append(" ".join(words[i:i + budget]))
        return out

    def _finalize_subchunk(
        self,
        pattern: str,
        section_name: str,
        paragraphs: List[str],
        source_file: str,
        document_type: str,
    ) -> Chunk:
        prefix = CHUNK_CONTEXT_PREFIX_TEMPLATE.format(pattern=pattern)
        text = f"{prefix}## {section_name}\n" + "\n\n".join(paragraphs)
        return Chunk(
            pattern=pattern,
            section=section_name,
            text=text,
            source_file=source_file,
            document_type=document_type,
            word_count=len(text.split()),
        )