# =============================================================
# CONDITIONS — deterministic step guards ($ref OP literal)
# =============================================================
# The fix for the "straight-line plan runs every step" bug. A step
# may carry a `when` condition; PYTHON evaluates it, never the LLM —
# the same invariants-from-code / judgment-from-models split as the
# governance tiers and the feasibility checker.
#
# The grammar is deliberately tiny and NOT eval(): a single
# comparison between one reference and one literal, e.g.
#   "$step_1.count >= 2"   "$trigger.amount > 1000"   "$step_2.tier == 'HIGH'"
# Anything else is a syntax error caught at feasibility time. There
# is no boolean algebra, no function calls, no attribute chains — the
# surface area an attacker or a hallucinating model could reach is a
# regex and a comparison, nothing more.
# =============================================================

from __future__ import annotations

import operator
import re
from dataclasses import dataclass

_OPS = {
    ">=": operator.ge, "<=": operator.le, "==": operator.eq,
    "!=": operator.ne, ">": operator.gt, "<": operator.lt,
}

# one reference ($step_N.field or $trigger.field), one operator, ONE literal:
# a quoted string, or a single non-whitespace token (number/bool/bareword).
# The single-token rule is what rejects compound conditions like "2 and x" —
# there is no boolean algebra in this grammar, by design.
_COND_RE = re.compile(
    r"^\s*(\$(?:step_\d+|trigger)(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*"
    r"(>=|<=|==|!=|>|<)\s*('[^']*'|\"[^\"]*\"|\S+)\s*$"
)


@dataclass
class Condition:
    ref: str        # e.g. "$step_1.count"
    op: str         # e.g. ">="
    literal: object  # parsed: int / float / bool / str


def _coerce(raw: str) -> object:
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    if len(raw) >= 2 and raw[0] in "\"'" and raw[-1] == raw[0]:
        return raw[1:-1]           # quoted string literal
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw                 # bare string


def parse_condition(when: str) -> Condition:
    """Parse a `when` string into a Condition. Raises ValueError on anything
    that is not exactly '$ref OP literal' — that rejection is the guardrail."""
    m = _COND_RE.match(when)
    if not m:
        raise ValueError(f"malformed condition {when!r} — expected '$ref OP value' "
                         f"(one reference, one operator, one literal)")
    return Condition(m.group(1), m.group(2), _coerce(m.group(3).strip()))


def compare(left: object, op: str, literal: object) -> bool:
    """Evaluate `left OP literal`. Numeric literals coerce the left side to a
    number so a tool returning a stringy count still compares correctly; an
    incomparable pair raises (the executor turns that into a failed step, not a
    silently-false guard)."""
    fn = _OPS[op]
    if isinstance(literal, (int, float)) and not isinstance(left, bool):
        try:
            left = float(left)
        except (TypeError, ValueError):
            raise ValueError(f"cannot compare non-numeric {left!r} to {literal!r}")
    return bool(fn(left, literal))
