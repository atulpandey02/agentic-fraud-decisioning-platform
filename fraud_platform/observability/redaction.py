# =============================================================
# TRACE REDACTION — strip direct PII before it is persisted
# =============================================================
# Correction to Priority 1 item 7. DATA_GOVERNANCE.md declared a
# redaction POLICY ("agents reason on derived signals, not names")
# but nothing ENFORCED it — and the policy was wishful: the
# get_user_history tool returns full_name / home_city / home_country
# straight from DIM_USERS, and AgentTraceWriter wrote that tool
# output verbatim into FACT_AGENT_TRACES.tool_output. So direct PII
# was landing in trace storage on every run.
#
# TraceRedactor runs on the trace rows BEFORE the Snowflake insert.
# It works in two passes because PII leaks two ways:
#   1. STRUCTURED — the tool output is JSON with known PII keys.
#      Those values are hashed in place (a stable, non-reversible
#      pseudonym: the same name maps to the same token, so audit
#      correlation still works, but the plaintext is gone).
#   2. FREE TEXT — an LLM reasoning step may quote a name or city it
#      saw. Pass 1 collects the literal PII values it hashed; pass 2
#      scrubs any occurrence of those exact strings from every text
#      field (tool_output, tool_input, reasoning_text).
#
# transaction_id / user_id are deliberately NOT redacted — they are
# already opaque UUIDs (not direct identifiers) and are the join
# keys the audit trail exists to preserve.
# =============================================================

from __future__ import annotations

import json
import hashlib
from typing import Dict, List, Set

# Direct-identifier keys (lower-cased) redacted wherever they appear
# in a tool output's JSON — names and home location. Extendable.
DEFAULT_PII_KEYS = frozenset({
    "full_name", "name", "home_city", "home_country",
    "home_latitude", "home_longitude", "email", "phone",
})

# Fields on a trace row that may carry PII and get scrubbed.
_TEXT_FIELDS = ("tool_output", "tool_input", "reasoning_text")


def _pseudonym(value: str) -> str:
    """Stable, non-reversible token for a PII value. Same input ->
    same token (audit correlation preserved), plaintext unrecoverable."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"[REDACTED:{digest}]"


class TraceRedactor:
    def __init__(self, pii_keys: frozenset = DEFAULT_PII_KEYS):
        self._pii_keys = {k.lower() for k in pii_keys}

    # ----------------------------------------------------------
    def redact_rows(self, rows: List[Dict]) -> List[Dict]:
        """Redact a full trace (list of FACT_AGENT_TRACES row dicts)
        in place and return it. Safe to call on rows with None fields."""
        collected: Set[str] = set()

        # Pass 1 — redact structured PII in every field, collecting the
        # literal values that were hashed.
        for row in rows:
            for field in _TEXT_FIELDS:
                val = row.get(field)
                if isinstance(val, str) and val:
                    row[field] = self._redact_structured(val, collected)

        # Pass 2 — scrub any collected literal PII values from all text
        # (catches an LLM reasoning step that quoted a name/city).
        if collected:
            for row in rows:
                for field in _TEXT_FIELDS:
                    val = row.get(field)
                    if isinstance(val, str) and val:
                        row[field] = self._scrub_literals(val, collected)
        return rows

    # ----------------------------------------------------------
    def _redact_structured(self, text: str, collected: Set[str]) -> str:
        """If `text` is (or contains) JSON, walk it and hash any
        PII-keyed value. Non-JSON text passes through unchanged (pass 2
        handles literal scrubbing)."""
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            return text
        redacted = self._walk(parsed, collected)
        return json.dumps(redacted)

    def _walk(self, obj, collected: Set[str]):
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if isinstance(k, str) and k.lower() in self._pii_keys and v is not None:
                    sval = str(v)
                    collected.add(sval)
                    out[k] = _pseudonym(sval)
                else:
                    out[k] = self._walk(v, collected)
            return out
        if isinstance(obj, list):
            return [self._walk(x, collected) for x in obj]
        if isinstance(obj, str):
            # A tool output often nests JSON inside a string field
            # (e.g. {"content": "{...user profile json...}"}). Recurse.
            try:
                inner = json.loads(obj)
            except (ValueError, TypeError):
                return obj
            if isinstance(inner, (dict, list)):
                return json.dumps(self._walk(inner, collected))
            return obj
        return obj

    @staticmethod
    def _scrub_literals(text: str, values: Set[str]) -> str:
        # Longest first, so "New York City" is replaced before "New York".
        for v in sorted(values, key=len, reverse=True):
            if v and v in text:
                text = text.replace(v, _pseudonym(v))
        return text
