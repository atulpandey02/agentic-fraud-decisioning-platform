# =============================================================
# DEMO RUNNER — evaluate one real flagged transaction
# =============================================================
# Pulls an actual flagged row from FEATURES.FACT_FEATURE_SNAPSHOTS
# (produced by Phase 1's feature engine) rather than using a
# fabricated example — this is a genuine end-to-end test against
# real data the platform actually computed.
# =============================================================

import logging
import snowflake.connector

import config
from agent import FraudDecisioningAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


def fetch_sample_flagged_transaction() -> dict:
    """
    Pull one real flagged transaction with all the fields the
    agent needs, plus the fraud_pattern ground truth column (Day 4
    fix) so we can compare the agent's own identified_pattern
    against what actually happened — a real accuracy check, not
    just "did it run without crashing."
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
            SELECT transaction_id, user_id, txn_amount,
                   risk_score_raw, is_flagged_for_review, is_new_device,
                   geo_distance_km, amount_zscore, velocity_15min,
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
            "merchant_category": "UNKNOWN",  # not in FACT_FEATURE_SNAPSHOTS — acceptable gap for this demo
            "city": "UNKNOWN",
            "country": "UNKNOWN",
            "risk_score_raw": float(record["risk_score_raw"]),
            "is_flagged_for_review": bool(record["is_flagged_for_review"]),
            "is_new_device": bool(record["is_new_device"]),
            "geo_distance_km": record["geo_distance_km"],
            "amount_zscore": record["amount_zscore"],
            "velocity_15min": record["velocity_15min"],
            # ground truth, kept SEPARATE from what's given to the agent —
            # the agent never sees these two fields, they're only used
            # afterward to check the agent's own conclusion
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
    logger.info("(the agent below does NOT see these two ground truth values)")

    logger.info("Building agent...")
    agent = FraudDecisioningAgent()

    logger.info("Running ReAct loop...")
    result = agent.evaluate(transaction, thread_id=transaction["transaction_id"])

    print("\n" + "=" * 70)
    print("AGENT DECISION")
    print("=" * 70)
    print(f"Decision:            {result['decision']}")
    print(f"Confidence:          {result['confidence_score']}")
    print(f"Identified pattern:  {result['identified_pattern']}")
    print(f"Reasoning:\n{result['reasoning_text']}")
    print("\n" + "=" * 70)
    print("GROUND TRUTH (agent did not see this)")
    print("=" * 70)
    print(f"Was actually fraud:  {ground_truth_fraud}")
    print(f"Actual pattern:      {ground_truth_pattern}")
    print("=" * 70)

    pattern_match = result["identified_pattern"] == ground_truth_pattern
    print(f"\nPattern match: {'YES' if pattern_match else 'NO — worth investigating why'}")


if __name__ == "__main__":
    main()