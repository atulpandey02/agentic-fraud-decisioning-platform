# =============================================================
# EVAL RUNNER — measure the agent against ground truth + judge
# =============================================================
# The full-stack integration point: for each sampled transaction
# this runs Phase 4's orchestrator, Phase 5's governance + persistence,
# writes the Phase 6 trace, and then does what only Phase 6 can —
# scores the run. Two orthogonal measurements, deliberately kept
# separate because they fail independently:
#
#   1. OUTCOME accuracy (eval_correct): did the decision match the
#      synthetic ground truth? Objective, free (labels came with
#      the generator), and blind to reasoning quality — an agent
#      can be right for wrong reasons.
#   2. REASONING quality (llm_judge_score): does the reasoning
#      actually cite the evidence and policy it was given, without
#      contradiction or fabrication? Judged by an LLM, because
#      grading free text is exactly what rules can't do — the
#      inverse of the governance layer's logic-not-LLM choice, and
#      the two docstrings deliberately mirror each other.
#
# ESCALATE and eval_correct — the deferral problem:
#   An escalation is neither right nor wrong; it is a paid deferral
#   to a human. Scoring it correct would teach "always escalate"
#   (perfect accuracy, zero autonomy); scoring it wrong would
#   punish exactly the humility the system prompt asks for on
#   borderline cases. So eval_correct stays NULL for escalations
#   (the column is nullable for this reason) and the escalation
#   RATE is reported as its own headline number.
# =============================================================

import time
import logging
from typing import Optional

import snowflake.connector
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, ValidationError

from . import config
from . import metrics
from .langsmith_config import configure_langsmith
from .audit_logger import AgentTraceWriter


def _fmt(x) -> str:
    """Format an Optional[float] metric for the report."""
    return f"{x:.3f}" if isinstance(x, (int, float)) else "n/a"
# This runner drives the orchestrator (Phase 4) and the governance
# layer (Phase 5) — plain package imports now, no sys.path bridging.
from fraud_platform.agents.multi_agent.orchestrator import FraudOrchestrator
from fraud_platform.governance.policy_framework import GovernancePolicyFramework
from fraud_platform.governance.hitl_handler import HITLHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


JUDGE_SYSTEM_PROMPT = """You are a strict evaluator of fraud-decision
reasoning. You are given the evidence an agent team gathered and the final
reasoning they produced. You do NOT judge whether the decision was correct —
you judge whether the REASONING is sound:

- Grounded: does it cite the actual feature values and policy guidance it
  was given, rather than generic fraud lore?
- Consistent: do the cited facts actually support the stated conclusion
  and the stated confidence?
- Complete: does it address the elevated signals rather than ignoring
  inconvenient ones?

Score 0.0-1.0. Reserve scores above 0.9 for reasoning that quotes specific
numbers and policy thresholds accurately. Penalize fabricated facts hard.

Respond with ONLY a single JSON object — no prose, no markdown fences — with
exactly these keys:
  "score": a number from 0.0 to 1.0 (reasoning quality),
  "notes": a string of 1-3 sentences on what was strong or weak, specifically."""


class JudgeVerdict(BaseModel):
    score: float = Field(ge=0.0, le=1.0, description="Reasoning quality, 0.0-1.0.")
    notes: str = Field(description="1-3 sentences: what was strong or weak, specifically.")


class EvalRunner:
    """
    Orchestrates one evaluation batch end to end and prints a
    summary. Instantiates its own judge LLM (temperature 0 — see
    config.py for why the judge, unlike the agents, can afford it).
    """

    def __init__(self):
        self._orchestrator = FraudOrchestrator()
        self._framework = GovernancePolicyFramework()
        self._hitl = HITLHandler()
        self._trace_writer = AgentTraceWriter()
        # JSON-object mode + LOCAL Pydantic validation (deliberately NOT Groq
        # strict json_schema). The judge runs on Qwen, a reasoning model that
        # emits a plain JSON object far more reliably than a strict-schema tool
        # call; we parse and validate that JSON against JudgeVerdict ourselves
        # in judge_reasoning(), so a malformed/failed verdict is a caught,
        # non-fatal judge failure — the objective ground-truth metrics still
        # count and the batch continues.
        self._judge = ChatGroq(
            model=config.JUDGE_MODEL_NAME,
            temperature=config.JUDGE_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        ).bind(response_format={"type": "json_object"})

    # ----------------------------------------------------------
    # SAMPLING
    # ----------------------------------------------------------
    def fetch_eval_sample(self, sample_size: int) -> list:
        """
        Stratified sample: half ground-truth fraud, half legitimate
        (both from the flagged population the agent actually faces).
        Without stratification, the flagged pool's ~2:1 legit skew
        (the hard-rule over-flagging documented in PROJECT_STATUS.md)
        would make the eval mostly a false-positive-handling test
        with almost no recall signal.
        """
        half = max(sample_size // 2, 1)
        conn = snowflake.connector.connect(
            account=config.SNOWFLAKE_ACCOUNT,
            user=config.SNOWFLAKE_USER,
            password=config.SNOWFLAKE_PASSWORD,
            database=config.SNOWFLAKE_DATABASE,
            warehouse=config.SNOWFLAKE_WAREHOUSE,
            role=config.SNOWFLAKE_ROLE,
            schema="FEATURES",
        )
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                WITH fraud AS (
                    SELECT * FROM FEATURES.FACT_FEATURE_SNAPSHOTS
                    WHERE is_flagged_for_review = TRUE AND is_synthetic_fraud = TRUE
                    ORDER BY RANDOM() LIMIT {half}
                ), legit AS (
                    SELECT * FROM FEATURES.FACT_FEATURE_SNAPSHOTS
                    WHERE is_flagged_for_review = TRUE AND is_synthetic_fraud = FALSE
                    ORDER BY RANDOM() LIMIT {half}
                )
                -- INTERLEAVE the two strata (final ORDER BY RANDOM):
                -- without it the fraud rows all come first, so a run cut
                -- short by a rate limit sees only fraud and the metrics
                -- are biased. Shuffling makes any PARTIAL run balanced.
                SELECT * FROM (
                    SELECT snapshot_id, transaction_id, user_id, txn_amount,
                           risk_score_raw, is_flagged_for_review, is_new_device,
                           geo_distance_km, time_since_last_txn_min,
                           amount_zscore, velocity_15min,
                           is_synthetic_fraud, fraud_pattern
                    FROM fraud
                    UNION ALL
                    SELECT snapshot_id, transaction_id, user_id, txn_amount,
                           risk_score_raw, is_flagged_for_review, is_new_device,
                           geo_distance_km, time_since_last_txn_min,
                           amount_zscore, velocity_15min,
                           is_synthetic_fraud, fraud_pattern
                    FROM legit
                )
                ORDER BY RANDOM()
                """
            )
            cols = [d[0].lower() for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            conn.close()

    # ----------------------------------------------------------
    # SCORING
    # ----------------------------------------------------------
    @staticmethod
    def outcome_correct(decision: str, is_fraud: bool):
        """
        BLOCK on fraud and ALLOW on legit are correct; the inverse
        pairs are wrong; ESCALATE is a deferral -> None (see module
        header for why deferrals must not count either way).
        """
        if decision == "ESCALATE":
            return None
        return (decision == "BLOCK") == is_fraud

    def judge_reasoning(self, result: dict) -> Optional[JudgeVerdict]:
        """Score the reasoning with the independent Qwen judge in JSON-object
        mode, then validate the returned JSON against JudgeVerdict LOCALLY.

        Returns None on ANY judge failure — an API/rate-limit error, non-JSON
        output, or a payload that fails Pydantic validation. A None here is
        deliberately non-fatal: the caller records the OBJECTIVE ground-truth
        outcome regardless and the batch keeps going. The judge is a
        best-effort reasoning-quality annotation, never a gate on the
        ground-truth metrics."""
        try:
            msg = self._judge.invoke([
                SystemMessage(content=JUDGE_SYSTEM_PROMPT),
                HumanMessage(content=(
                    f"Evidence gathered by the team:\n"
                    f"- Feature findings: {result.get('feature_findings')}\n"
                    f"- Elevated patterns: {result.get('elevated_patterns')}\n"
                    f"- Risk assessment: {result.get('risk_assessment') or '(risk specialist skipped)'}\n"
                    f"- Policy guidance: {result.get('policy_guidance')}\n\n"
                    f"Final decision: {result['decision']} "
                    f"(confidence {result['confidence_score']}, pattern {result['identified_pattern']})\n\n"
                    f"Reasoning to evaluate:\n{result['reasoning_text']}"
                )),
            ])
            # Local Pydantic validation of the JSON object the model returned.
            return JudgeVerdict.model_validate_json(msg.content)
        except (ValidationError, ValueError, KeyError) as e:
            logger.warning("Judge produced an invalid verdict (non-fatal): %s",
                           str(e).splitlines()[0][:120])
            return None
        except Exception as e:  # noqa: BLE001 — API/rate-limit etc. must not abort
            logger.warning("Judge call failed (non-fatal, objective outcome still counts): %s",
                           str(e).splitlines()[0][:120])
            return None

    def record_scores(self, decision_id: str, correct, judge_score, judge_notes) -> None:
        """UPDATE the eval columns on the already-persisted decision
        row — scoring is an annotation on the decision, not a new
        fact table (the Day 1 schema made this call already). judge_score /
        judge_notes may be None when the judge failed — the objective
        eval_correct is written either way (NULL judge score is expected and
        the column is nullable)."""
        conn = self._hitl._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE DECISIONS.FACT_DECISIONS
                SET eval_correct = %(correct)s,
                    eval_scored_at = CURRENT_TIMESTAMP(),
                    llm_judge_score = %(score)s,
                    llm_judge_notes = %(notes)s
                WHERE decision_id = %(decision_id)s
                """,
                {
                    "correct": correct,
                    "score": judge_score,
                    "notes": judge_notes,
                    "decision_id": decision_id,
                },
            )
            conn.commit()
        finally:
            cursor.close()

    # ----------------------------------------------------------
    # THE BATCH
    # ----------------------------------------------------------
    def run(self, sample_size: int = config.EVAL_SAMPLE_SIZE) -> None:
        rows = self.fetch_eval_sample(sample_size)
        logger.info(f"Evaluating {len(rows)} transactions "
                    f"({sum(1 for r in rows if r['is_synthetic_fraud'])} fraud, "
                    f"{sum(1 for r in rows if not r['is_synthetic_fraud'])} legit)")

        results = []
        aborted = None
        for i, record in enumerate(rows, 1):
          try:
            transaction = {
                "transaction_id": record["transaction_id"],
                "user_id": record["user_id"],
                "amount": float(record["txn_amount"]),
                "merchant_category": "UNKNOWN",
                "city": "UNKNOWN",
                "country": "UNKNOWN",
                "risk_score_raw": float(record["risk_score_raw"]),
                "is_flagged_for_review": bool(record["is_flagged_for_review"]),
                "is_new_device": bool(record["is_new_device"]),
                "geo_distance_km": record["geo_distance_km"],
                "time_since_last_txn_min": record["time_since_last_txn_min"],
                "amount_zscore": record["amount_zscore"],
                "velocity_15min": record["velocity_15min"],
            }
            is_fraud = bool(record["is_synthetic_fraud"])
            truth_pattern = record["fraud_pattern"] or "NONE"

            logger.info(f"[{i}/{len(rows)}] {record['transaction_id']} "
                        f"(truth: {'FRAUD/' + truth_pattern if is_fraud else 'legit'})")

            started = time.time()
            result = self._orchestrator.evaluate(
                transaction, thread_id=f"eval-{record['transaction_id']}"
            )
            latency_ms = int((time.time() - started) * 1000)

            tier, _ = self._framework.assign_tier(
                result["decision"], result["confidence_score"], result["amount"]
            )
            decision_id = self._hitl.persist_decision(
                result, tier, latency_ms, snapshot_id=record["snapshot_id"]
            )
            self._trace_writer.write_trace(decision_id, result["messages"])

            correct = self.outcome_correct(result["decision"], is_fraud)
            # Judge is best-effort and non-fatal: a None verdict (API/rate-limit
            # error or invalid JSON) still records the objective outcome and the
            # batch continues. judge_score is then None → excluded from judge
            # aggregates, never counted as a 0.
            verdict = self.judge_reasoning(result)
            judge_score = verdict.score if verdict else None
            judge_notes = verdict.notes if verdict else "(judge unavailable — objective outcome recorded)"
            self.record_scores(decision_id, correct, judge_score, judge_notes)

            results.append({
                "decision": result["decision"],
                "pattern": result["identified_pattern"] or "NONE",
                "agent_pattern": result["identified_pattern"] or "NONE",
                # The feature_agent's full elevated-pattern LIST — not just the
                # single final label — so the metrics can tell "detected but
                # mislabeled" apart from "completely missed" (see metrics.py).
                "elevated_patterns": result.get("elevated_patterns"),
                "truth_pattern": truth_pattern,
                "is_fraud": is_fraud,
                "correct": correct,
                "confidence_score": result["confidence_score"],
                "amount": result["amount"],
                "judge_score": judge_score,
                "judge_notes": judge_notes,
                "tier": tier,
                "latency_ms": latency_ms,
            })
          except Exception as e:  # noqa: BLE001 — per-transaction isolation
            # One transaction's failure (most often a Groq daily-token
            # rate limit) must NOT discard the whole run. Log it, remember
            # it, and stop cleanly so the summary below still prints the
            # metrics for everything that DID complete. This is why a
            # partial 50-txn run now yields a real scorecard instead of a
            # bare traceback.
            aborted = f"{type(e).__name__}: {str(e).splitlines()[0][:120]}"
            logger.error("Eval stopped at %d/%d — %s", i, len(rows), aborted)
            break

        try:
            if results:
                self._print_summary(results)
            if aborted:
                print(f"\n*** RUN INCOMPLETE: stopped after {len(results)}/{len(rows)} "
                      f"transactions — {aborted}")
        finally:
            self._hitl.close()
            self._trace_writer.close()

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    @staticmethod
    def _print_summary(results: list) -> None:
        decided = [r for r in results if r["correct"] is not None]
        escalated = [r for r in results if r["correct"] is None]
        n_correct = sum(1 for r in decided if r["correct"])

        print("\n" + "=" * 70)
        print("EVALUATION SUMMARY")
        print("=" * 70)
        judged = [r["judge_score"] for r in results if r["judge_score"] is not None]
        judge_failures = len(results) - len(judged)
        for r in results:
            mark = "·ESC" if r["correct"] is None else ("  OK" if r["correct"] else "MISS")
            js = f"{r['judge_score']:.2f}" if r["judge_score"] is not None else "n/a "
            print(f"  [{mark}] {r['decision']:8s} truth={'FRAUD' if r['is_fraud'] else 'legit'}"
                  f"/{r['truth_pattern']:15s} agent_pattern={r['pattern']:15s} "
                  f"judge={js} tier={r['tier']}")
        print("-" * 70)
        if decided:
            print(f"Decision accuracy (excl. escalations): {n_correct}/{len(decided)}")
        print(f"Escalation rate:                       {len(escalated)}/{len(results)}")
        # Judge score is averaged over SUCCESSFUL judgements only; failures are
        # reported separately so a rate-limited judge never drags the mean down
        # or is silently counted as zero. Objective metrics below are unaffected.
        if judged:
            print(f"Mean judge score (of {len(judged)} judged):    "
                  f"{sum(judged) / len(judged):.2f}")
        if judge_failures:
            print(f"Judge failures (non-fatal):            {judge_failures}/{len(results)}")
        print(f"Mean latency:                          "
              f"{sum(r['latency_ms'] for r in results) // len(results)} ms")

        # Richer scorecard from the Priority 5 metrics module — the
        # metrics a fraud team actually reviews (the 4 lines above are
        # kept for continuity with the earlier 6-txn report).
        m = metrics.compute_all(results)
        c = m["classification"]
        print("-" * 70)
        print("CLASSIFICATION (BLOCK=positive, escalations excluded):")
        print(f"  precision={_fmt(c['precision'])}  recall={_fmt(c['recall'])}  "
              f"FPR={_fmt(c['false_positive_rate'])}  F1={_fmt(c['f1'])}")
        print(f"  confusion: TP={c['tp']} FP={c['fp']} FN={c['fn']} TN={c['tn']}")
        cal = m["calibration"]
        print(f"CALIBRATION: Brier={_fmt(cal['brier_score'])} over {cal['n']} decided")
        jc = m["judge_cross_check"]
        print(f"JUDGE CROSS-CHECK: mean on correct={_fmt(jc['mean_judge_on_correct'])} "
              f"vs incorrect={_fmt(jc['mean_judge_on_incorrect'])} "
              f"(gap={_fmt(jc['agreement_gap'])})")
        # PATTERN EVALUATION — detection vs. primary-label id, kept distinct.
        # `detection` credits the true pattern being surfaced anywhere (in the
        # feature_agent's elevated_patterns or the final label); `primary_id`
        # is the strict "final label matched" number. A high detection with a
        # low primary_id means the agent SEES the right pattern but names a
        # plausible co-pattern — a labeling gap, not a detection failure.
        print("PER-PATTERN (true-fraud rows) — recall / detection / primary-label id:")
        tot = {"n": 0, "detected": 0, "primary": 0, "mislabeled": 0, "missed": 0}
        for pat, b in sorted(m["per_pattern"].items()):
            print(f"  {pat:15s} recall={_fmt(b['recall'])} "
                  f"detection={_fmt(b['detection_rate'])} "
                  f"primary_id={_fmt(b['pattern_id_accuracy'])} "
                  f"(n={b['n']}, mislabeled={b['mislabeled']}, missed={b['missed']})")
            tot["n"] += b["n"]; tot["detected"] += b["detected"]
            tot["primary"] += b["primary_correct"]; tot["mislabeled"] += b["mislabeled"]
            tot["missed"] += b["missed"]
        if tot["n"]:
            print(f"  OVERALL (fraud n={tot['n']}): "
                  f"detected {tot['detected']}/{tot['n']}  "
                  f"primary-label-correct {tot['primary']}/{tot['n']}  "
                  f"mislabeled {tot['mislabeled']}  missed {tot['missed']}")
        print("=" * 70)
        print("All decisions, traces, and scores persisted to DECISIONS.*")


def main():
    configure_langsmith(project_name=f"{config.LANGSMITH_PROJECT}-eval")
    EvalRunner().run()


if __name__ == "__main__":
    main()
