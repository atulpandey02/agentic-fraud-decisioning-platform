# =============================================================
# UNIT TESTS — eval metrics (Priority 5 item 6)
# =============================================================
# Pure functions over a fixed list of scored decisions, no infra.
# =============================================================

from fraud_platform.observability import metrics


def _d(decision, is_fraud, **kw):
    base = {"decision": decision, "is_fraud": is_fraud, "truth_pattern": None,
            "agent_pattern": None, "confidence_score": 0.9, "amount": 100.0}
    base.update(kw)
    return base


class TestClassification:
    def test_confusion_and_rates(self):
        ds = [
            _d("BLOCK", True),   # TP
            _d("BLOCK", True),   # TP
            _d("BLOCK", False),  # FP
            _d("ALLOW", True),   # FN
            _d("ALLOW", False),  # TN
            _d("ALLOW", False),  # TN
            _d("ESCALATE", True),  # excluded
        ]
        m = metrics.classification_metrics(ds)
        assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (2, 1, 1, 2)
        assert m["precision"] == 2 / 3
        assert m["recall"] == 2 / 3
        assert m["false_positive_rate"] == 1 / 3

    def test_escalations_excluded_from_confusion(self):
        ds = [_d("ESCALATE", True), _d("ESCALATE", False)]
        c = metrics.confusion(ds)
        assert c == {"tp": 0, "fp": 0, "fn": 0, "tn": 0}


class TestEscalation:
    def test_rate_and_cost(self):
        ds = [_d("ESCALATE", True), _d("ALLOW", False), _d("BLOCK", True), _d("ESCALATE", False)]
        e = metrics.escalation_metrics(ds, cost_per_escalation=5.0)
        assert e["escalations"] == 2
        assert e["escalation_rate"] == 0.5
        assert e["escalation_cost"] == 10.0


class TestPerPattern:
    def test_recall_and_pattern_id(self):
        ds = [
            _d("BLOCK", True, truth_pattern="GEO_JUMP", agent_pattern="GEO_JUMP"),
            _d("ALLOW", True, truth_pattern="GEO_JUMP", agent_pattern="NONE"),
            _d("ESCALATE", True, truth_pattern="GEO_JUMP", agent_pattern="GEO_JUMP"),
        ]
        pp = metrics.per_pattern_recall(ds)["GEO_JUMP"]
        assert pp["n"] == 3
        assert pp["caught"] == 2            # BLOCK + ESCALATE, not the ALLOW
        assert pp["recall"] == 2 / 3
        assert pp["pattern_id_accuracy"] == 2 / 3
        # Backward-compat: with no elevated_patterns, detection falls back to
        # the final label, so detection == primary-id here.
        assert pp["detection_rate"] == 2 / 3
        assert pp["missed"] == 1           # the ALLOW row named NONE


class TestPatternDetection:
    """The pattern-eval fix: detection (surfaced anywhere) must be kept
    distinct from primary-label id (the final single label matched)."""

    def test_detected_but_mislabeled_is_not_a_miss(self):
        # The real bug: NEW_DEVICE fraud where the feature_agent elevated BOTH
        # AMOUNT_ANOMALY and NEW_DEVICE, but the final label said AMOUNT_ANOMALY.
        # Old code scored this pattern_id=0 (total miss). It should now count as
        # detected + mislabeled, NOT missed.
        ds = [_d("BLOCK", True, truth_pattern="NEW_DEVICE",
                 agent_pattern="AMOUNT_ANOMALY",
                 elevated_patterns=["AMOUNT_ANOMALY", "NEW_DEVICE"])]
        b = metrics.per_pattern_recall(ds)["NEW_DEVICE"]
        assert b["detected"] == 1
        assert b["mislabeled"] == 1
        assert b["missed"] == 0
        assert b["primary_correct"] == 0
        assert b["detection_rate"] == 1.0
        assert b["pattern_id_accuracy"] == 0.0   # strict number stays honest

    def test_completely_missed_when_true_pattern_never_surfaced(self):
        ds = [_d("BLOCK", True, truth_pattern="GEO_JUMP",
                 agent_pattern="AMOUNT_ANOMALY",
                 elevated_patterns=["AMOUNT_ANOMALY"])]
        b = metrics.per_pattern_recall(ds)["GEO_JUMP"]
        assert b["detected"] == 0 and b["missed"] == 1 and b["mislabeled"] == 0
        assert b["detection_rate"] == 0.0

    def test_primary_correct_counts_as_detected_not_mislabeled(self):
        ds = [_d("BLOCK", True, truth_pattern="VELOCITY_SPIKE",
                 agent_pattern="VELOCITY_SPIKE",
                 elevated_patterns=["VELOCITY_SPIKE"])]
        b = metrics.per_pattern_recall(ds)["VELOCITY_SPIKE"]
        assert (b["detected"], b["primary_correct"], b["mislabeled"], b["missed"]) == (1, 1, 0, 0)
        assert b["detection_rate"] == 1.0 and b["pattern_id_accuracy"] == 1.0

    def test_elevated_patterns_accepts_comma_string(self):
        # elevated_patterns may arrive as a comma-joined string, not a list.
        ds = [_d("BLOCK", True, truth_pattern="NEW_DEVICE",
                 agent_pattern="AMOUNT_ANOMALY",
                 elevated_patterns="AMOUNT_ANOMALY, NEW_DEVICE")]
        b = metrics.per_pattern_recall(ds)["NEW_DEVICE"]
        assert b["detected"] == 1 and b["mislabeled"] == 1 and b["missed"] == 0


class TestCalibration:
    def test_perfect_calibration_low_brier(self):
        # confident-correct predictions -> Brier near 0
        ds = [_d("BLOCK", True, confidence_score=1.0),
              _d("ALLOW", False, confidence_score=1.0)]
        cal = metrics.calibration(ds)
        assert cal["brier_score"] == 0.0

    def test_overconfident_wrong_high_brier(self):
        # confident but wrong -> Brier near 1
        ds = [_d("BLOCK", False, confidence_score=1.0),
              _d("ALLOW", True, confidence_score=1.0)]
        cal = metrics.calibration(ds)
        assert cal["brier_score"] == 1.0

    def test_escalations_have_no_probability(self):
        ds = [_d("ESCALATE", True, confidence_score=0.6)]
        assert metrics.calibration(ds)["n"] == 0


class TestJudgeCrossCheck:
    def test_good_judge_scores_correct_higher(self):
        ds = [
            _d("BLOCK", True, judge_score=0.95),    # correct
            _d("BLOCK", False, judge_score=0.40),   # incorrect
            _d("ALLOW", False, judge_score=0.90),   # correct
        ]
        jc = metrics.judge_outcome_agreement(ds)
        assert jc["mean_judge_on_correct"] > jc["mean_judge_on_incorrect"]
        assert jc["agreement_gap"] > 0

    def test_self_preferring_judge_shows_no_gap(self):
        # judge rates everything ~equally -> gap ~0 (not tracking quality)
        ds = [_d("BLOCK", True, judge_score=0.8), _d("BLOCK", False, judge_score=0.8)]
        jc = metrics.judge_outcome_agreement(ds)
        assert jc["agreement_gap"] == 0.0


class TestComputeAll:
    def test_full_scorecard_keys(self):
        ds = [_d("BLOCK", True, truth_pattern="GEO_JUMP", judge_score=0.9)]
        allm = metrics.compute_all(ds)
        assert set(allm) >= {"n", "classification", "escalation", "per_pattern",
                             "calibration", "judge_cross_check"}
