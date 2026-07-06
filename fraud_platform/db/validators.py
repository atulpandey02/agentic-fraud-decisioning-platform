# =============================================================
# VALIDATORS — application-level invariants at the write boundary
# =============================================================
# Priority 2 item 5: validate confidence, risk scores, amounts,
# enums, and cross-field invariants in APPLICATION code, not by
# leaning on Snowflake CHECK constraints alone.
#
# Why both, and why app-level is the FIRST line:
#   The DDL still carries CHECK constraints (decision IN (...),
#   governance_tier IN (...)) — those stay as the database's last
#   line of defense. But a CHECK violation surfaces through the
#   connector as an opaque driver error, at the far end of a write,
#   with no indication which field or row was wrong. Validating at
#   the boundary means a bad value fails FAST, with a readable
#   message naming the field — and it catches the things a CHECK
#   cannot express at all: NaN/inf floats (a CHECK on a FLOAT
#   passes NaN), out-of-range probabilities, and cross-field
#   invariants that span columns.
#
# Pure functions, no I/O, no heavy imports — unit-tested in full
# (tests/test_validators.py) with no credentials.
# =============================================================

import math

from .schema_contract import ENUMS


class ValidationError(ValueError):
    """
    A value violated an application invariant. Subclasses ValueError
    so existing callers that catch ValueError keep working, but is a
    distinct type so new code can catch precisely this.
    """


# -------------------------------------------------------------
# Scalars
# -------------------------------------------------------------
def validate_probability(value, field: str, *, allow_none: bool = True):
    """
    A probability-like score in [0.0, 1.0] — used for
    confidence_score and llm_judge_score. Rejects NaN and infinity,
    which a Snowflake FLOAT column (and its CHECK) would silently
    accept and then poison every downstream AVG().
    """
    if value is None:
        if allow_none:
            return value
        raise ValidationError(f"{field} is required and must be between 0.0 and 1.0.")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError(f"{field} must be a number, got {type(value).__name__}.")
    if math.isnan(value) or math.isinf(value):
        raise ValidationError(f"{field} must be a finite number, got {value!r}.")
    if not (0.0 <= value <= 1.0):
        raise ValidationError(f"{field} must be between 0.0 and 1.0, got {value}.")
    return value


def validate_risk_score(value, field: str = "risk_score_raw"):
    """Risk score shares the probability contract (0..1, finite)."""
    return validate_probability(value, field, allow_none=False)


def validate_amount(value, field: str = "amount"):
    """
    A monetary amount: finite and non-negative. A negative or NaN
    amount is a data-quality failure that would corrupt z-scores and
    tier decisions (the AUTO_ALLOW value cap compares against it).
    """
    if value is None:
        raise ValidationError(f"{field} is required.")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError(f"{field} must be a number, got {type(value).__name__}.")
    if math.isnan(value) or math.isinf(value):
        raise ValidationError(f"{field} must be finite, got {value!r}.")
    if value < 0:
        raise ValidationError(f"{field} must be non-negative, got {value}.")
    return value


def validate_enum(value, enum_name: str, field: str, *, allow_none: bool = False):
    """Membership in one of schema_contract.ENUMS."""
    allowed = ENUMS[enum_name]
    if value is None:
        if allow_none:
            return value
        raise ValidationError(f"{field} is required; must be one of {sorted(allowed)}.")
    if value not in allowed:
        raise ValidationError(
            f"{field} must be one of {sorted(allowed)}, got {value!r}."
        )
    return value


def validate_review_outcome(outcome, field: str = "human_outcome"):
    """Human review verdict — the enum record_review() gates on."""
    return validate_enum(outcome, "HUMAN_OUTCOME", field, allow_none=False)


def validate_identified_pattern(value, field: str = "identified_pattern"):
    """
    An agent's identified pattern is a fraud-pattern label OR the
    literal 'NONE' (no pattern applies) OR NULL. 'NONE' is
    deliberately NOT in the FRAUD_PATTERN enum (it is not a pattern),
    so it is permitted explicitly here rather than by widening the
    enum — keeping FRAUD_PATTERN meaning exactly the four real labels.
    """
    if value is None or value == "NONE":
        return value
    return validate_enum(value, "FRAUD_PATTERN", field, allow_none=False)


# -------------------------------------------------------------
# Cross-field: a whole decision record
# -------------------------------------------------------------
def validate_decision_record(
    *, decision, confidence_score, identified_pattern, governance_tier=None
):
    """
    Validate a decision before it is persisted — every field and the
    invariants that span them. Raises ValidationError on the first
    problem (fail fast, named field).

    Cross-field invariants enforced:
      - An autonomous decision (ALLOW/BLOCK) MUST carry a confidence
        score. You cannot act without a stated confidence; only a
        deferral (ESCALATE) may leave it unset. This is the machine
        twin of the governance floor — a NULL confidence must never
        be treated as "confident".
      - governance_tier, when present, must be a valid tier. (Its
        consistency WITH the decision — e.g. ESCALATE => SUGGEST — is
        owned by GovernancePolicyFramework, not re-litigated here, to
        keep one authority for tier logic.)
    """
    validate_enum(decision, "DECISION", "decision", allow_none=False)
    validate_identified_pattern(identified_pattern)
    if governance_tier is not None:
        validate_enum(governance_tier, "GOVERNANCE_TIER", "governance_tier")

    autonomous = decision in ("ALLOW", "BLOCK")
    validate_probability(
        confidence_score, "confidence_score", allow_none=not autonomous
    )
    if autonomous and confidence_score is None:
        raise ValidationError(
            f"decision={decision} requires a confidence_score "
            f"(only ESCALATE may omit it)."
        )
