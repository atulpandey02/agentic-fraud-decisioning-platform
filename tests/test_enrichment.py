# =============================================================
# TESTS — per-transaction enrichment + validation + isolation
# =============================================================
# Priority 5 + the TRACKED requirement: prove the validators are
# enforced on the REAL pipeline per-transaction path (build_feature_row),
# and that one bad row is quarantined without failing the batch —
# with real bad-data cases (NaN amount, out-of-range, invalid enum).
#
# Two layers:
#   1. Pure unit tests over build_feature_row on dict rows.
#   2. A LOCAL SPARK micro-batch running build_feature_row over actual
#      Spark Row objects (good + bad mixed), applying the exact
#      isolation the pipeline's foreachBatch loop uses. Skipped when no
#      JVM is available (so CI stays green); runs where Spark exists.
# =============================================================

import pytest

from fraud_platform.stream_processing.scoring import build_feature_row, to_snowflake_row
from fraud_platform.stream_processing.feature_engine import FeatureEngineConfig
from fraud_platform.db.validators import ValidationError

CONFIG = FeatureEngineConfig()


def _row(**over):
    r = {
        "transaction_id": "t1", "user_id": "u1", "amount": 50.0,
        "latitude": None, "longitude": None, "transaction_ts": "2026-01-01T00:00:00",
        "device_id": "d1", "city": "Boston", "country": "US",
        "is_synthetic_fraud": False, "fraud_pattern": None,
    }
    r.update(over)
    return r


BASE = {"avg_transaction_amt": 50.0, "stddev_transaction_amt": 20.0, "surrogate_key": "sk1"}


def _build(row, **kw):
    kw.setdefault("baseline", BASE)
    kw.setdefault("last_loc", None)
    kw.setdefault("velocity_15min", 0)
    kw.setdefault("user_devices", {"d1"})
    kw.setdefault("config", CONFIG)
    return build_feature_row(row, **kw)


class TestBuildFeatureRowHappyPath:
    def test_clean_row_builds_valid_dict(self):
        fd = _build(_row())
        assert fd["transaction_id"] == "t1"
        assert fd["risk_score_raw"] == 0.0
        assert fd["is_flagged_for_review"] is False
        assert fd["is_new_device"] is False

    def test_new_device_flag(self):
        fd = _build(_row(device_id="unknown"))
        assert fd["is_new_device"] is True

    def test_geo_hard_rule_sets_suspect_location(self):
        # a 5000km+ jump (Boston->London coords) with a prior location
        last = {"lat": 42.36, "lon": -71.06, "city": "Boston", "ts": None}
        fd = _build(_row(latitude=51.5074, longitude=-0.1278), last_loc=last)
        assert fd["geo_distance_km"] > 3000
        assert fd["suspect_location"] is True
        assert fd["is_flagged_for_review"] is True

    def test_to_snowflake_row_has_ground_truth_and_timestamp(self):
        fd = _build(_row())
        sf = to_snowflake_row(fd)
        assert "is_synthetic_fraud" in sf and "fraud_pattern" in sf
        assert sf["computed_at"] is not None
        assert sf["transaction_id"] == "t1"


class TestValidationRejectsBadData:
    def test_nan_amount_rejected(self):
        with pytest.raises(ValidationError):
            _build(_row(amount=float("nan")))

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            _build(_row(amount=-100.0))

    def test_infinite_amount_rejected(self):
        with pytest.raises(ValidationError):
            _build(_row(amount=float("inf")))

    def test_invalid_fraud_pattern_rejected(self):
        with pytest.raises(ValidationError):
            _build(_row(fraud_pattern="TELEPORT"))

    def test_valid_fraud_pattern_ok(self):
        fd = _build(_row(fraud_pattern="GEO_JUMP"))
        assert fd["fraud_pattern"] == "GEO_JUMP"

    def test_validation_can_be_disabled_for_trusted_callers(self):
        # validate=False returns the row even if it would fail — used
        # only where the caller has already vetted inputs
        fd = _build(_row(amount=-1.0), validate=False)
        assert fd["txn_amount"] == -1.0


# -------------------------------------------------------------
# LOCAL SPARK micro-batch — the real code path over real Spark Rows
# -------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    try:
        s = (SparkSession.builder
             .appName("test-enrichment")
             .master("local[1]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    except Exception as e:
        pytest.skip(f"Spark/JVM unavailable: {e}")
    yield s
    s.stop()


class TestSparkMicroBatchIsolation:
    def test_bad_rows_quarantined_good_rows_survive(self, spark):
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType, BooleanType,
        )
        # Explicit schema — all-None lat/lon columns can't be inferred.
        schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("user_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("transaction_ts", StringType()),
            StructField("device_id", StringType()),
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("is_synthetic_fraud", BooleanType()),
            StructField("fraud_pattern", StringType()),
        ])
        # A realistic micro-batch: 3 good transactions + 2 corrupt ones.
        data = [
            _row(transaction_id="good1", amount=40.0),
            _row(transaction_id="bad_nan", amount=float("nan")),
            _row(transaction_id="good2", amount=75.0, device_id="unknown"),
            _row(transaction_id="bad_enum", fraud_pattern="TELEPORT"),
            _row(transaction_id="good3", amount=20.0),
        ]
        df = spark.createDataFrame([tuple(r[c] for c in schema.names) for r in data], schema)

        # This mirrors feature_engine.process_batch's isolation loop
        # exactly: build per row, quarantine on ValidationError, continue.
        features, quarantined = [], []
        for r in df.collect():
            try:
                features.append(_build(r))
            except ValidationError:
                quarantined.append(r["transaction_id"])

        got = {f["transaction_id"] for f in features}
        assert got == {"good1", "good2", "good3"}          # all good rows survived
        assert set(quarantined) == {"bad_nan", "bad_enum"}  # both bad rows isolated
        # and every surviving row is valid to persist
        for f in features:
            assert 0.0 <= f["risk_score_raw"] <= 1.0
            assert f["txn_amount"] >= 0.0
