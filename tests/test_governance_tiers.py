# =============================================================
# UNIT TESTS — governance tier assignment (Priority 4)
# =============================================================
# GovernancePolicyFramework.assign_tier is pure. Thresholds are
# passed to the constructor so the test pins them rather than
# depending on config values.
# =============================================================

import pytest

from fraud_platform.governance.policy_framework import GovernancePolicyFramework
from fraud_platform.governance import config


@pytest.fixture
def fw():
    return GovernancePolicyFramework(confidence_floor=0.75, auto_allow_max_amount=500.0)


class TestTierAssignment:
    def test_escalate_always_suggest(self, fw):
        # even at high confidence / low amount, an escalation is a suggestion
        tier, _ = fw.assign_tier("ESCALATE", 0.99, 10.0)
        assert tier == config.TIER_SUGGEST

    def test_low_confidence_allow_held(self, fw):
        tier, _ = fw.assign_tier("ALLOW", 0.5, 10.0)
        assert tier == config.TIER_SUGGEST

    def test_none_confidence_held(self, fw):
        tier, _ = fw.assign_tier("BLOCK", None, 10.0)
        assert tier == config.TIER_SUGGEST

    def test_confident_block_notify_only(self, fw):
        tier, _ = fw.assign_tier("BLOCK", 0.95, 10.0)
        assert tier == config.TIER_NOTIFY_ONLY

    def test_confident_small_allow_auto_approve(self, fw):
        tier, _ = fw.assign_tier("ALLOW", 0.9, 32.17)
        assert tier == config.TIER_AUTO_APPROVE

    def test_confident_large_allow_notify_only(self, fw):
        # above the value cap, a confident allow still executes but is surfaced
        tier, _ = fw.assign_tier("ALLOW", 0.9, 750.0)
        assert tier == config.TIER_NOTIFY_ONLY

    def test_boundary_confidence_at_floor_is_confident(self, fw):
        # floor is a strict '<' — exactly at 0.75 counts as confident
        tier, _ = fw.assign_tier("ALLOW", 0.75, 10.0)
        assert tier == config.TIER_AUTO_APPROVE

    def test_boundary_amount_at_cap_is_silent(self, fw):
        # '<=' cap — exactly 500 is still AUTO_APPROVE
        tier, _ = fw.assign_tier("ALLOW", 0.9, 500.0)
        assert tier == config.TIER_AUTO_APPROVE

    def test_withhold_beats_grant_on_conflict(self, fw):
        # low-confidence small allow matches both "hold" and "grant"
        # rules; the cautious side must win (rule order)
        tier, _ = fw.assign_tier("ALLOW", 0.5, 10.0)
        assert tier == config.TIER_SUGGEST

    def test_rationale_is_returned(self, fw):
        _, rationale = fw.assign_tier("ALLOW", 0.9, 10.0)
        assert isinstance(rationale, str) and rationale
