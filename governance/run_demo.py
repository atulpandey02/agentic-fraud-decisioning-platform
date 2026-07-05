# =============================================================
# DEMO RUNNER — decision -> governance tier -> durable row ->
# human review, end to end
# =============================================================
# The full Phase 5 loop on one real transaction:
#   1. Multi-agent orchestrator (Phase 4) produces a decision
#   2. GovernancePolicyFramework assigns its autonomy tier
#   3. HITLHandler persists it to FACT_DECISIONS — the first
#      moment in this platform's life a decision survives the
#      process that made it
#   4. The review queue is shown; if this decision landed in
#      SUGGEST, a simulated human reviews it and the closed loop
#      is read back from Snowflake
#
# Cross-phase import: same pattern as the orchestrator's own tool
# bridge — insert the multi_agent folder, import, remove. Our
# `config` (a superset of Phase 4's, via the config layering) is
# already cached under the name every Phase 3/4 module imports,
# which is exactly what makes this work.
# =============================================================

import os
import sys
import time
import logging

import snowflake.connector

import config
from policy_framework import GovernancePolicyFramework
from hitl_handler import HITLHandler

_MULTI_AGENT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "agents", "multi_agent")
)
sys.path.insert(0, _MULTI_AGENT_DIR)
try:
    from orchestrator import FraudOrchestrator   # noqa: E402
finally:
    sys.path.remove(_MULTI_AGENT_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def fetch_sample_flagged_transaction() -> dict:
    """
    Same fetch as the agent demos, plus snapshot_id — FACT_DECISIONS
    has a snapshot_id column precisely so a decision can be traced
    back to the exact feature snapshot it was made from (point-in-
    time correctness, same reason DIM_USERS is SCD2). The agent
    demos didn't need it; the persistence layer does.
    """
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
            """
            SELECT snapshot_id, transaction_id, user_id, txn_amount,
                   risk_score_raw, is_flagged_for_review, is_new_device,
                   geo_distance_km, time_since_last_txn_min,
                   amount_zscore, velocity_15min,
                   is_synthetic_fraud, fraud_pattern
            FROM FEATURES.FACT_FEATURE_SNAPSHOTS
            WHERE is_flagged_for_review = TRUE
              AND is_synthetic_fraud IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("No flagged transactions with ground truth found.")
        cols = [d[0].lower() for d in cursor.description]
        record = dict(zip(cols, row))

        return {
            "snapshot_id": record["snapshot_id"],
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
    finally:
        conn.close()


def main():
    logger.info("Fetching a real flagged transaction from Snowflake...")
    transaction = fetch_sample_flagged_transaction()
    snapshot_id = transaction.pop("snapshot_id")

    logger.info("Building orchestrator...")
    orchestrator = FraudOrchestrator()
    framework = GovernancePolicyFramework()
    hitl = HITLHandler()

    # ---- 1. Decide ----
    logger.info("Running multi-agent flow...")
    started = time.time()
    result = orchestrator.evaluate(transaction, thread_id=transaction["transaction_id"])
    latency_ms = int((time.time() - started) * 1000)

    # ---- 2. Assign autonomy tier ----
    tier, rationale = framework.assign_tier(
        decision=result["decision"],
        confidence_score=result["confidence_score"],
        amount=result["amount"],
    )

    # ---- 3. Persist — the decision becomes durable here ----
    decision_id = hitl.persist_decision(
        final_state=result,
        governance_tier=tier,
        processing_latency_ms=latency_ms,
        snapshot_id=snapshot_id,
    )

    print("\n" + "=" * 70)
    print("GOVERNED DECISION")
    print("=" * 70)
    print(f"Decision:        {result['decision']} (confidence {result['confidence_score']})")
    print(f"Pattern:         {result['identified_pattern']}")
    print(f"Latency:         {latency_ms} ms")
    print(f"Governance tier: {tier}")
    print(f"Tier rationale:  {rationale}")
    print(f"Decision id:     {decision_id}")

    # ---- 4. The human side of the loop ----
    queue = hitl.pending_reviews(limit=5)
    print("\n" + "=" * 70)
    print(f"REVIEW QUEUE — {len(queue)} pending suggestion(s)")
    print("=" * 70)
    for item in queue:
        print(
            f"  {str(item['decided_at'])[:19]}  {item['decision']:8s} "
            f"conf={item['confidence_score']}  {item['decision_id']}"
        )

    if tier == config.TIER_SUGGEST:
        print("\nThis decision was held for review — simulating the human closing the loop...")
        hitl.record_review(
            decision_id=decision_id,
            reviewer_id="demo_reviewer",
            outcome="CONFIRMED",
            notes="Demo review: agent reasoning checks out against the cited policy.",
        )
        remaining = hitl.pending_reviews(limit=5)
        print(f"Review recorded. Queue now has {len(remaining)} pending suggestion(s).")
    else:
        print(f"\nThis decision was {tier} — it executed without entering the review queue.")

    hitl.close()


if __name__ == "__main__":
    main()
