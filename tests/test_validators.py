# =============================================================
# UNIT TESTS — application-level validators (Priority 2 item 5)
# =============================================================
# Pure, no credentials. Locks the invariants that used to live only
# in Snowflake CHECK constraints (or nowhere, for NaN/inf).
# =============================================================

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "db"))

from validators import (  # noqa: E402
    ValidationError,
    validate_probability,
    validate_risk_score,
    validate_amount,
    validate_enum,
    validate_review_outcome,
    validate_identified_pattern,
    validate_decision_record,
)


class TestProbability:
    @pytest.mark.parametrize("v", [0.0, 0.5, 1.0])
    def test_valid(self, v):
        assert validate_probability(v, "x") == v

    @pytest.mark.parametrize("v", [-0.01, 1.01, 2, -5])
    def test_out_of_range(self, v):
        with pytest.raises(ValidationError):
            validate_probability(v, "x")

    def test_nan_and_inf_rejected(self):
        # the case a FLOAT CHECK constraint silently passes
        with pytest.raises(ValidationError):
            validate_probability(float("nan"), "x")
        with pytest.raises(ValidationError):
            validate_probability(float("inf"), "x")

    def test_bool_rejected(self):
        # bool is an int subclass — must not sneak in as 0/1
        with pytest.raises(ValidationError):
            validate_probability(True, "x")

    def test_none_allowed_by_default(self):
        assert validate_probability(None, "x") is None

    def test_none_rejected_when_required(self):
        with pytest.raises(ValidationError):
            validate_probability(None, "x", allow_none=False)


class TestAmount:
    @pytest.mark.parametrize("v", [0, 0.0, 12.5, 1000000])
    def test_valid(self, v):
        assert validate_amount(v) == v

    @pytest.mark.parametrize("v", [-0.01, -100])
    def test_negative_rejected(self, v):
        with pytest.raises(ValidationError):
            validate_amount(v)

    def test_nan_rejected(self):
        with pytest.raises(ValidationError):
            validate_amount(float("nan"))

    def test_none_rejected(self):
        with pytest.raises(ValidationError):
            validate_amount(None)


class TestRiskScore:
    def test_valid(self):
        assert validate_risk_score(0.6) == 0.6

    def test_none_rejected(self):
        # risk score is always computed — None is a bug
        with pytest.raises(ValidationError):
            validate_risk_score(None)


class TestEnums:
    def test_decision_valid(self):
        assert validate_enum("BLOCK", "DECISION", "decision") == "BLOCK"

    def test_decision_invalid(self):
        with pytest.raises(ValidationError):
            validate_enum("MAYBE", "DECISION", "decision")

    def test_review_outcome(self):
        assert validate_review_outcome("CONFIRMED") == "CONFIRMED"
        with pytest.raises(ValidationError):
            validate_review_outcome("APPROVED")  # not a valid outcome

    def test_tier_enum(self):
        assert validate_enum("SUGGEST", "GOVERNANCE_TIER", "t") == "SUGGEST"
        with pytest.raises(ValidationError):
            validate_enum("MAYBE_LATER", "GOVERNANCE_TIER", "t")


class TestIdentifiedPattern:
    @pytest.mark.parametrize("v", ["GEO_JUMP", "VELOCITY_SPIKE", "NONE", None])
    def test_valid(self, v):
        assert validate_identified_pattern(v) == v

    def test_invalid(self):
        with pytest.raises(ValidationError):
            validate_identified_pattern("TELEPORT")


class TestDecisionRecordCrossField:
    def test_valid_autonomous(self):
        validate_decision_record(
            decision="BLOCK", confidence_score=0.9,
            identified_pattern="GEO_JUMP", governance_tier="NOTIFY_ONLY",
        )

    def test_valid_escalate_without_confidence(self):
        # ESCALATE is a deferral — it MAY omit confidence
        validate_decision_record(
            decision="ESCALATE", confidence_score=None,
            identified_pattern="NONE", governance_tier="SUGGEST",
        )

    def test_autonomous_without_confidence_rejected(self):
        # the load-bearing cross-field rule: you cannot ALLOW/BLOCK
        # without a stated confidence
        with pytest.raises(ValidationError):
            validate_decision_record(
                decision="ALLOW", confidence_score=None,
                identified_pattern="NONE",
            )

    def test_bad_decision_rejected(self):
        with pytest.raises(ValidationError):
            validate_decision_record(
                decision="HOLD", confidence_score=0.5, identified_pattern="NONE",
            )

    def test_bad_pattern_rejected(self):
        with pytest.raises(ValidationError):
            validate_decision_record(
                decision="BLOCK", confidence_score=0.9, identified_pattern="XYZ",
            )
