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

import snowflake.connector
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field

from . import config
from .langsmith_config import configure_langsmith
from .audit_logger import AgentTraceWriter
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
numbers and policy thresholds accurately. Penalize fabricated facts hard."""


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
        self._judge = ChatGroq(
            model=config.JUDGE_MODEL_NAME,
            temperature=config.JUDGE_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        ).with_structured_output(JudgeVerdict)

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

    def judge_reasoning(self, result: dict) -> JudgeVerdict:
        return self._judge.invoke([
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

    def record_scores(self, decision_id: str, correct, verdict: JudgeVerdict) -> None:
        """UPDATE the eval columns on the already-persisted decision
        row — scoring is an annotation on the decision, not a new
        fact table (the Day 1 schema made this call already)."""
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
                    "score": verdict.score,
                    "notes": verdict.notes,
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
        for i, record in enumerate(rows, 1):
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
            verdict = self.judge_reasoning(result)
            self.record_scores(decision_id, correct, verdict)

            results.append({
                "decision": result["decision"],
                "pattern": result["identified_pattern"] or "NONE",
                "truth_pattern": truth_pattern,
                "is_fraud": is_fraud,
                "correct": correct,
                "judge_score": verdict.score,
                "judge_notes": verdict.notes,
                "tier": tier,
                "latency_ms": latency_ms,
            })

        self._print_summary(results)
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
        pattern_hits = sum(1 for r in results if r["pattern"] == r["truth_pattern"])

        print("\n" + "=" * 70)
        print("EVALUATION SUMMARY")
        print("=" * 70)
        for r in results:
            mark = "·ESC" if r["correct"] is None else ("  OK" if r["correct"] else "MISS")
            print(f"  [{mark}] {r['decision']:8s} truth={'FRAUD' if r['is_fraud'] else 'legit'}"
                  f"/{r['truth_pattern']:15s} agent_pattern={r['pattern']:15s} "
                  f"judge={r['judge_score']:.2f} tier={r['tier']}")
        print("-" * 70)
        if decided:
            print(f"Decision accuracy (excl. escalations): {n_correct}/{len(decided)}")
        print(f"Escalation rate:                       {len(escalated)}/{len(results)}")
        print(f"Pattern identification:                {pattern_hits}/{len(results)}")
        print(f"Mean judge score:                      "
              f"{sum(r['judge_score'] for r in results) / len(results):.2f}")
        print(f"Mean latency:                          "
              f"{sum(r['latency_ms'] for r in results) // len(results)} ms")
        print("=" * 70)
        print("All decisions, traces, and scores persisted to DECISIONS.*")


def main():
    configure_langsmith(project_name=f"{config.LANGSMITH_PROJECT}-eval")
    EvalRunner().run()


if __name__ == "__main__":
    main()
