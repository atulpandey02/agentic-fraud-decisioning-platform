# =============================================================
# DEMO RUNNER — one real flagged transaction through the team
# =============================================================
# Same fetch as Phase 3's run_demo.py: a real flagged row from
# FEATURES.FACT_FEATURE_SNAPSHOTS with ground truth held back.
# The fetch function is DUPLICATED from Phase 3 rather than
# imported — importing it would pull single_agent/run_demo.py,
# whose `from agent import ...` would collide with this phase's
# module names (the same by-name caching issue the config
# layering and tool bridge already navigate). Forty lines of
# straightforward SQL is under the "reuse conventions, not code"
# threshold tools.py's header established for cross-phase reuse.
#
# What this demo shows beyond Phase 3's: the ROUTING — which
# specialists the orchestrator chose, in what order, what it
# skipped, and why (the narrative log carries each handoff
# rationale).
# =============================================================

import logging
import snowflake.connector

import config
from orchestrator import FraudOrchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def fetch_sample_flagged_transaction() -> dict:
    """One real flagged transaction + held-back ground truth
    (identical contract to Phase 3's fetch — see module header
    for why it's duplicated, not imported)."""
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
            SELECT transaction_id, user_id, txn_amount,
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
            raise RuntimeError(
                "No flagged transactions with ground truth found. "
                "Run feature_engine.py against real data first."
            )
        cols = [d[0].lower() for d in cursor.description]
        record = dict(zip(cols, row))

        return {
            "transaction_id": record["transaction_id"],
            "user_id": record["user_id"],
            "amount": float(record["txn_amount"]),
            "merchant_category": "UNKNOWN",  # not in FACT_FEATURE_SNAPSHOTS — same accepted gap as Phase 3
            "city": "UNKNOWN",
            "country": "UNKNOWN",
            "risk_score_raw": float(record["risk_score_raw"]),
            "is_flagged_for_review": bool(record["is_flagged_for_review"]),
            "is_new_device": bool(record["is_new_device"]),
            "geo_distance_km": record["geo_distance_km"],
            "time_since_last_txn_min": record["time_since_last_txn_min"],
            "amount_zscore": record["amount_zscore"],
            "velocity_15min": record["velocity_15min"],
            "_ground_truth_is_fraud": bool(record["is_synthetic_fraud"]),
            "_ground_truth_pattern": record["fraud_pattern"],
        }
    finally:
        conn.close()


def main():
    logger.info("Fetching a real flagged transaction from Snowflake...")
    transaction = fetch_sample_flagged_transaction()

    ground_truth_fraud = transaction.pop("_ground_truth_is_fraud")
    ground_truth_pattern = transaction.pop("_ground_truth_pattern")

    logger.info(f"Transaction: {transaction['transaction_id']}")
    logger.info(f"Ground truth: is_fraud={ground_truth_fraud}, pattern={ground_truth_pattern}")
    logger.info("(no agent below sees these two ground truth values)")

    logger.info("Building orchestrator + specialists...")
    orchestrator = FraudOrchestrator()

    logger.info("Running multi-agent flow...")
    result = orchestrator.evaluate(transaction, thread_id=transaction["transaction_id"])

    print("\n" + "=" * 70)
    print("ORCHESTRATION TRACE (who ran, in order, and why)")
    print("=" * 70)
    for msg in result["messages"]:
        name = getattr(msg, "name", None)
        if name == "orchestrator":
            print(f"\n  [{name}] {msg.content}")
        elif name:
            preview = msg.content.replace("\n", " ")
            print(f"    [{name}] {preview[:160]}{'...' if len(preview) > 160 else ''}")

    print("\n" + "=" * 70)
    print("TEAM DECISION")
    print("=" * 70)
    print(f"Specialists invoked:  {' -> '.join(result['agents_invoked'])}")
    skipped = [a for a in config.SPECIALIST_AGENTS if a not in result["agents_invoked"]]
    print(f"Specialists skipped:  {skipped or 'none'}")
    print(f"Decision:            {result['decision']}")
    print(f"Confidence:          {result['confidence_score']}")
    print(f"Identified pattern:  {result['identified_pattern']}")
    print(f"Reasoning:\n{result['reasoning_text']}")

    print("\n" + "=" * 70)
    print("GROUND TRUTH (no agent saw this)")
    print("=" * 70)
    print(f"Was actually fraud:  {ground_truth_fraud}")
    print(f"Actual pattern:      {ground_truth_pattern}")
    print("=" * 70)

    # Normalize before comparing: the agent says "NONE" for no
    # pattern, but the ground truth column is SQL NULL for legit
    # rows — comparing the raw values calls every correct
    # "no pattern" verdict a mismatch.
    agent_pattern = result["identified_pattern"] or "NONE"
    truth_pattern = ground_truth_pattern or "NONE"
    pattern_match = agent_pattern == truth_pattern
    print(f"\nPattern match: {'YES' if pattern_match else 'NO — worth investigating why'}")


if __name__ == "__main__":
    main()
