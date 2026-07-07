# =============================================================
# UNIT TESTS — feature scoring + velocity thresholds (Priority 4)
# =============================================================
# Pure: the weighted composite (compute_risk_score) and the
# threshold constants that define the two-tier scoring. No Spark.
# =============================================================

import pytest

from fraud_platform.stream_processing.feature_engine import (
    compute_risk_score,
    FeatureEngineConfig,
)
from fraud_platform.stream_processing import window_config


WEIGHTS = FeatureEngineConfig.RISK_WEIGHTS


class TestRiskScore:
    def test_all_zero_signals(self):
        assert compute_risk_score(0, 0, 0, 0, WEIGHTS) == 0.0

    def test_all_saturated_clamps_to_one(self):
        # every signal at 1.0 sums to the total weight (1.0) — clamp holds
        assert compute_risk_score(1.0, 1.0, 1.0, 1.0, WEIGHTS) == 1.0

    def test_weights_applied_per_signal(self):
        # only velocity saturated -> exactly its weight
        assert compute_risk_score(1.0, 0, 0, 0, WEIGHTS) == pytest.approx(WEIGHTS["velocity_15min"])
        assert compute_risk_score(0, 1.0, 0, 0, WEIGHTS) == pytest.approx(WEIGHTS["amount_zscore"])
        assert compute_risk_score(0, 0, 1.0, 0, WEIGHTS) == pytest.approx(WEIGHTS["geo_distance"])
        assert compute_risk_score(0, 0, 0, 1.0, WEIGHTS) == pytest.approx(WEIGHTS["new_device"])

    def test_single_signal_cannot_cross_flag_threshold(self):
        # the structural fact that MOTIVATED the hard rules: no single
        # signal's weight reaches the 0.60 flag threshold on its own
        for sig in ("velocity_15min", "amount_zscore", "geo_distance", "new_device"):
            assert WEIGHTS[sig] < FeatureEngineConfig.RISK_FLAG_THRESHOLD

    def test_moderate_combination_can_cross_threshold(self):
        # velocity + amount + geo all moderately elevated -> over 0.60
        score = compute_risk_score(0.8, 0.8, 0.8, 0.0, WEIGHTS)
        assert score > FeatureEngineConfig.RISK_FLAG_THRESHOLD

    def test_result_is_rounded_4dp(self):
        score = compute_risk_score(0.333333, 0.1, 0.1, 0.0, WEIGHTS)
        assert score == round(score, 4)


class TestVelocityThresholds:
    def test_flag_threshold_is_jp_morgan_five(self):
        assert window_config.VELOCITY_FLAG_THRESHOLD == 5

    def test_normalization_saturates_at_max_normal(self):
        # velocity normalization: min(count / VELOCITY_MAX_NORMAL, 1.0)
        vmax = FeatureEngineConfig.VELOCITY_MAX_NORMAL
        assert min(5 / vmax, 1.0) == 1.0     # at threshold -> saturated
        assert min(10 / vmax, 1.0) == 1.0    # above -> still clamped
        assert min(2 / vmax, 1.0) == pytest.approx(0.4)

    def test_velocity_hard_rule_fires_at_saturation(self):
        # v15 >= HARD_RULE_VELOCITY_THRESHOLD (1.0) means count >= 5
        vmax = FeatureEngineConfig.VELOCITY_MAX_NORMAL
        assert min(5 / vmax, 1.0) >= FeatureEngineConfig.HARD_RULE_VELOCITY_THRESHOLD
        assert min(4 / vmax, 1.0) < FeatureEngineConfig.HARD_RULE_VELOCITY_THRESHOLD

    def test_windows_shared_from_window_config(self):
        # feature engine and velocity engine must not drift on windows
        assert FeatureEngineConfig.WINDOWS is window_config.VELOCITY_WINDOWS
