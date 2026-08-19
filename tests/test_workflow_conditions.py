# =============================================================
# UNIT TESTS — deterministic step conditions (no LLM, no eval)
# =============================================================

import pytest

from fraud_platform.workflow_engine.conditions import compare, parse_condition


class TestParse:
    def test_parses_numeric_comparison(self):
        c = parse_condition("$step_1.count >= 2")
        assert (c.ref, c.op, c.literal) == ("$step_1.count", ">=", 2)

    def test_parses_string_and_bool_and_float(self):
        assert parse_condition("$step_2.tier == 'HIGH'").literal == "HIGH"
        assert parse_condition("$trigger.flagged == true").literal is True
        assert parse_condition("$step_1.ratio > 0.5").literal == 0.5

    @pytest.mark.parametrize("bad", [
        "count >= 2",                 # no $ref
        "$step_1.count",              # no operator
        "$step_1.count >= 2 and x",   # compound not allowed
        "$step_1.count =~ 2",         # bad operator
        "delete $step_1",             # not a comparison
    ])
    def test_malformed_conditions_raise(self, bad):
        with pytest.raises(ValueError):
            parse_condition(bad)


class TestCompare:
    def test_numeric_operators(self):
        assert compare(3, ">=", 2) is True
        assert compare(1, ">=", 2) is False
        assert compare(2, "==", 2) is True
        assert compare(2, "!=", 3) is True

    def test_stringy_count_coerces_to_number(self):
        # a tool returning "3" still compares numerically
        assert compare("3", ">=", 2) is True

    def test_string_equality(self):
        assert compare("HIGH", "==", "HIGH") is True
        assert compare("LOW", "==", "HIGH") is False

    def test_incomparable_raises(self):
        with pytest.raises(ValueError):
            compare("not-a-number", ">=", 2)
