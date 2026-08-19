# =============================================================
# EVAL METRICS — beyond "how many did it get right" (Priority 5 item 6)
# =============================================================
# The Phase 6 eval reported accuracy + an escalation rate. That is
# not enough to judge a fraud system: it hides WHICH errors, at what
# COST, and whether the model's confidence means anything. This
# module computes the metrics a fraud team actually reviews —
# precision, recall, false-positive rate, per-pattern recall,
# escalation cost, and CALIBRATION — from a list of scored decisions,
# as pure functions (no I/O, fully unit-tested).
#
# The classifier framing: BLOCK is the positive class (predict
# fraud), ALLOW is negative. ESCALATE is a DEFERRAL, excluded from
# the precision/recall confusion matrix (scoring it either way is
# wrong — see eval_runner's docstring) and reported as its own rate
# and COST instead.
#
# Independent-judge strategy: the LLM judge shares the agents' model
# family (self-preference bias). judge_outcome_agreement() cross-
# checks the judge's reasoning scores against the OBJECTIVE outcome
# (was the decision actually correct) — if a judge rates wrong
# decisions as highly as right ones, its scores are not measuring
# quality, and that shows up here as low agreement. That objective
# cross-check IS the independence: ground truth doesn't flatter the
# model that produced the reasoning.
# =============================================================

from __future__ import annotations

from typing import List, Dict, Optional


# A scored decision (what eval_runner produces per transaction):
#   decision: "ALLOW" | "BLOCK" | "ESCALATE"
#   is_fraud: bool (ground truth)
#   truth_pattern: str | None
#   agent_pattern: str | None
#   confidence_score: float | None
#   amount: float
#   judge_score: float | None   (optional, for the judge cross-check)


def _safe_div(n: float, d: float) -> Optional[float]:
    return n / d if d else None


def confusion(decisions: List[Dict]) -> Dict[str, int]:
    """BLOCK-as-positive confusion matrix over DECIDED rows only
    (escalations excluded — a deferral is neither TP nor FN)."""
    tp = fp = fn = tn = 0
    for d in decisions:
        if d["decision"] == "ESCALATE":
            continue
        blocked = d["decision"] == "BLOCK"
        fraud = bool(d["is_fraud"])
        if blocked and fraud:
            tp += 1
        elif blocked and not fraud:
            fp += 1
        elif not blocked and fraud:
            fn += 1
        else:
            tn += 1
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn}


def classification_metrics(decisions: List[Dict]) -> Dict[str, Optional[float]]:
    c = confusion(decisions)
    tp, fp, fn, tn = c["tp"], c["fp"], c["fn"], c["tn"]
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    fpr = _safe_div(fp, fp + tn)
    f1 = _safe_div(2 * precision * recall, precision + recall) if precision and recall else None
    return {"precision": precision, "recall": recall, "false_positive_rate": fpr, "f1": f1, **c}


def escalation_metrics(decisions: List[Dict], cost_per_escalation: float = 1.0) -> Dict:
    """Escalations aren't free — each one is human time. Report the
    rate AND the total cost so the tradeoff is explicit."""
    n = len(decisions)
    esc = sum(1 for d in decisions if d["decision"] == "ESCALATE")
    return {
        "escalation_rate": _safe_div(esc, n),
        "escalations": esc,
        "escalation_cost": esc * cost_per_escalation,
    }


def _surfaced_patterns(d: Dict) -> set:
    """The set of fraud patterns the system actually surfaced for a
    decision: the feature_agent's `elevated_patterns` PLUS the final
    `agent_pattern` label. Normalizes a list, a comma-joined string, or
    None into an upper-cased set.

    This is the fix for the pattern-eval bug: the agent frequently DETECTS
    the true pattern (it appears in `elevated_patterns`) but the final
    single label names a plausible co-pattern instead — e.g. a high-amount
    purchase on a new device surfaces both AMOUNT_ANOMALY and NEW_DEVICE but
    gets labeled AMOUNT_ANOMALY. Comparing only the final label scored that
    as a total miss; comparing against the surfaced SET credits the
    detection and separates it from a genuine miss."""
    raw = d.get("elevated_patterns")
    if raw is None:
        surfaced = set()
    elif isinstance(raw, str):
        surfaced = {p.strip().upper() for p in raw.split(",") if p.strip()}
    else:  # list / tuple / set of patterns
        surfaced = {str(p).strip().upper() for p in raw if str(p).strip()}
    primary = (d.get("agent_pattern") or "").strip().upper()
    if primary and primary != "NONE":
        surfaced.add(primary)
    return surfaced


def per_pattern_recall(decisions: List[Dict]) -> Dict[str, Dict]:
    """Per true-fraud pattern, over fraud rows only, four honest measures:

      - recall              : fraction CAUGHT (BLOCK or ESCALATE — not waved
                              through).
      - pattern_id_accuracy : fraction where the FINAL label (`agent_pattern`)
                              matched the true pattern exactly — the STRICT
                              number (unchanged meaning from before).
      - detection_rate      : fraction where the true pattern was surfaced
                              ANYWHERE (in `elevated_patterns` or as the final
                              label) — the CHARITABLE number that credits a
                              detected-but-mislabeled pattern.
      - mislabeled_rate     : detected, but the final label named a different
                              pattern (usually a plausible co-signal).
      - missed_rate         : the true pattern was never surfaced at all — the
                              genuine identification failure.

    detection_rate >= pattern_id_accuracy always; the gap between them is the
    "caught it but called it something else" rate. Raw counts are kept
    alongside the rates so a caller can aggregate across patterns."""
    out: Dict[str, Dict] = {}
    for d in decisions:
        if not d["is_fraud"]:
            continue
        pat = (d.get("truth_pattern") or "UNKNOWN").strip().upper()
        bucket = out.setdefault(pat, {
            "n": 0, "caught": 0, "primary_correct": 0,
            "detected": 0, "mislabeled": 0, "missed": 0,
        })
        bucket["n"] += 1
        if d["decision"] in ("BLOCK", "ESCALATE"):
            bucket["caught"] += 1

        is_primary = (d.get("agent_pattern") or "NONE").strip().upper() == pat
        is_detected = pat in _surfaced_patterns(d)
        if is_primary:
            bucket["primary_correct"] += 1
        if is_detected:
            bucket["detected"] += 1
            if not is_primary:
                bucket["mislabeled"] += 1
        else:
            bucket["missed"] += 1

    for b in out.values():
        b["recall"] = _safe_div(b["caught"], b["n"])
        b["pattern_id_accuracy"] = _safe_div(b["primary_correct"], b["n"])
        b["detection_rate"] = _safe_div(b["detected"], b["n"])
        b["mislabeled_rate"] = _safe_div(b["mislabeled"], b["n"])
        b["missed_rate"] = _safe_div(b["missed"], b["n"])
    return out


def _fraud_probability(d: Dict) -> Optional[float]:
    """Map a decision+confidence to an implied P(fraud): a confident
    BLOCK means high P(fraud); a confident ALLOW means low. Used for
    calibration. ESCALATE has no implied probability."""
    c = d.get("confidence_score")
    if c is None or d["decision"] == "ESCALATE":
        return None
    return c if d["decision"] == "BLOCK" else 1.0 - c


def calibration(decisions: List[Dict], bins: int = 5) -> Dict:
    """Brier score + a reliability table. Calibration asks: when the
    system says it's 90% sure, is it right ~90% of the time? A model
    can be accurate but badly calibrated (over-confident), which
    matters here because the governance autonomy floor TRUSTS the
    confidence number."""
    pairs = []
    for d in decisions:
        p = _fraud_probability(d)
        if p is None:
            continue
        pairs.append((p, 1.0 if d["is_fraud"] else 0.0))
    if not pairs:
        return {"brier_score": None, "n": 0, "reliability": []}

    brier = sum((p - o) ** 2 for p, o in pairs) / len(pairs)

    table = []
    for i in range(bins):
        lo, hi = i / bins, (i + 1) / bins
        # include the top edge in the last bin
        in_bin = [(p, o) for p, o in pairs if (lo <= p < hi or (i == bins - 1 and p == hi))]
        if not in_bin:
            continue
        avg_p = sum(p for p, _ in in_bin) / len(in_bin)
        frac_pos = sum(o for _, o in in_bin) / len(in_bin)
        table.append({
            "bin": f"[{lo:.1f},{hi:.1f})", "n": len(in_bin),
            "mean_predicted": round(avg_p, 3), "observed_fraud_rate": round(frac_pos, 3),
        })
    return {"brier_score": round(brier, 4), "n": len(pairs), "reliability": table}


def judge_outcome_agreement(decisions: List[Dict]) -> Dict:
    """Independent cross-check of the LLM judge. Compares the judge's
    reasoning score against the OBJECTIVE outcome (decision correct?).
    A useful judge scores correct decisions higher than incorrect ones;
    the GAP between those two means is the signal. A gap near zero means
    the judge isn't tracking real quality (self-preference / noise)."""
    right, wrong = [], []
    for d in decisions:
        js = d.get("judge_score")
        if js is None or d["decision"] == "ESCALATE":
            continue
        correct = (d["decision"] == "BLOCK") == bool(d["is_fraud"])
        (right if correct else wrong).append(js)
    mean_right = _safe_div(sum(right), len(right))
    mean_wrong = _safe_div(sum(wrong), len(wrong))
    gap = (mean_right - mean_wrong) if (mean_right is not None and mean_wrong is not None) else None
    return {
        "mean_judge_on_correct": mean_right,
        "mean_judge_on_incorrect": mean_wrong,
        "agreement_gap": gap,
        "n_correct": len(right), "n_incorrect": len(wrong),
    }


def compute_all(decisions: List[Dict], cost_per_escalation: float = 1.0) -> Dict:
    """One call, the full scorecard."""
    return {
        "n": len(decisions),
        "classification": classification_metrics(decisions),
        "escalation": escalation_metrics(decisions, cost_per_escalation),
        "per_pattern": per_pattern_recall(decisions),
        "calibration": calibration(decisions),
        "judge_cross_check": judge_outcome_agreement(decisions),
    }
