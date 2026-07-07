# =============================================================
# SCORING & ENRICHMENT — the per-transaction feature component
# =============================================================
# Priority 5 item 2 split: the enrichment (amount z-score, geo,
# device) + scoring (weighted composite + hard rules) + the
# per-row feature build were extracted out of feature_engine.py's
# 150-line foreachBatch loop into named, typed, unit-testable
# functions here. feature_engine keeps ONLY the Spark orchestration
# (Kafka read, parse, windows, sinks) and calls build_feature_row
# per row.
#
# Nothing here imports feature_engine — the config object is passed
# IN (build_feature_row takes it as a parameter), so there is no
# circular dependency and this whole module runs on a plain dict
# row with no Spark session. That is exactly what makes the
# validated, isolated per-transaction path (Priority 5 item 1 +
# the tracked validator-wiring requirement) provable on real data
# without a Kafka cluster.
#
# The scoring THRESHOLDS live here now (single source); the old
# FeatureEngineConfig keeps a thin facade over them so every
# existing `config.X` / `FeatureEngineConfig.X` call site is
# unchanged — this is a relocation, not a behavior change.
# =============================================================

import math
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

from fraud_platform.db.validators import (
    ValidationError,
    validate_amount,
    validate_probability,
    validate_identified_pattern,
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# SCORING THRESHOLDS — single source of truth (FeatureEngineConfig
# re-exports these). See GEO_FLAGGING_INVESTIGATION.md for the geo
# rule's derivation and the Day-4 two-tier rationale.
# -------------------------------------------------------------
RISK_WEIGHTS = {
    "velocity_15min":  0.30,   # high weight — most reliable fraud signal
    "amount_zscore":   0.25,   # strong signal for AMOUNT_ANOMALY
    "geo_distance":    0.25,   # strong signal for GEO_JUMP
    "new_device":      0.20,   # supporting signal, not standalone
}
RISK_FLAG_THRESHOLD = 0.60

HARD_RULE_AMOUNT_THRESHOLD   = 0.95
HARD_RULE_VELOCITY_THRESHOLD = 1.0
HARD_RULE_GEO_MIN_JUMP_KM    = 3000.0   # distance arm of the composite geo rule
IMPOSSIBLE_TRAVEL_SPEED_KMH  = 900.0    # speed arm — commercial-jet ceiling
SPEED_RULE_MIN_DISTANCE_KM   = 100.0    # floor under the speed arm (GPS-noise guard)

VELOCITY_MAX_NORMAL = 5.0    # velocity_15min > 5 = JP Morgan flag threshold
GEO_MAX_NORMAL_KM   = 500.0  # within 500km = normal domestic travel
ZSCORE_MAX_NORMAL   = 3.0    # z > 3 = AMOUNT_ANOMALY


def utc_now() -> datetime:
    """Timezone-aware UTC now (Priority 5 item 4). Replaces the
    deprecated naive datetime.utcnow(); the wall-clock value is
    identical, the tz is now explicit."""
    return datetime.now(timezone.utc)


# =============================================================
# PURE SCORING FUNCTIONS (moved verbatim from feature_engine)
# =============================================================
def haversine_distance(lat1, lon1, lat2, lon2) -> Optional[float]:
    """
    Haversine great-circle distance between two points in km.
    Registered as a Spark UDF — runs on each executor, not driver.

    Why Haversine and not Euclidean:
        Earth is a sphere. Euclidean distance on lat/lon is wrong
        at scale — it doesn't account for curvature. Haversine
        gives the correct shortest-path distance on a sphere.
        This is what production fraud systems use for impossible
        travel detection (threshold: > 900 km/h implied speed).
    """
    if any(x is None for x in [lat1, lon1, lat2, lon2]):
        return None

    R = 6371.0  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def compute_implied_speed_kmh(distance_km, time_since_last_min) -> Optional[float]:
    """
    Implied travel speed between consecutive transactions, km/h.

    Returns None when no meaningful speed exists: missing inputs,
    or a NON-POSITIVE time delta. The non-positive case is load-
    bearing, not defensive boilerplate — GEO_FLAGGING_INVESTIGATION.md
    measured that 80% of far-distance legitimate rows had a NEGATIVE
    delta (out-of-order backfill events being compared against a
    location the user visits in the event-time FUTURE). A speed
    computed from that is noise wearing units.
    """
    if distance_km is None or not time_since_last_min or time_since_last_min <= 0:
        return None
    return distance_km / (time_since_last_min / 60.0)


def geo_hard_rule(distance_km, implied_speed_kmh) -> bool:
    """
    The composite geo hard rule from GEO_FLAGGING_INVESTIGATION.md.

    Speed arm: implied speed above the commercial-jet ceiling, with
    a distance floor so GPS jitter over second-scale deltas can't
    fabricate supersonic "speeds" out of meter-scale movement.
    Distance arm: intercontinental-jump scale, needed because most
    rows have no usable time delta and the speed arm alone would
    collapse GEO_JUMP recall (99% -> 9%, measured).
    """
    if distance_km is None:
        return False
    if distance_km >= HARD_RULE_GEO_MIN_JUMP_KM:
        return True
    return (
        implied_speed_kmh is not None
        and implied_speed_kmh > IMPOSSIBLE_TRAVEL_SPEED_KMH
        and distance_km >= SPEED_RULE_MIN_DISTANCE_KM
    )


def compute_risk_score(v15, amt_signal, geo_signal, dev_signal, weights) -> float:
    """
    The weighted composite risk score — the "soft" half of the two-tier
    scoring (the hard rules are geo_hard_rule + the amount/velocity
    saturation checks). Each input is an already-normalized 0..1 signal;
    the weighted sum is clamped to 1.0 and rounded.
    """
    score = (
        weights["velocity_15min"] * v15 +
        weights["amount_zscore"] * amt_signal +
        weights["geo_distance"] * geo_signal +
        weights["new_device"] * dev_signal
    )
    return round(min(score, 1.0), 4)


# =============================================================
# VALIDATION — the boundary check before a row is trusted
# =============================================================
def validate_feature_row(fd: dict) -> None:
    """
    Validate one computed feature row before it is written anywhere
    (Priority 5 + the tracked requirement: enforce the validators on
    LIVE pipeline data, not just at the decision boundary). Raises
    ValidationError on bad data; the caller (build_feature_row's user)
    quarantines the offending transaction and continues the batch.

    Catches exactly the corruption a Snowflake FLOAT/CHECK column would
    accept silently: a NaN/inf amount or z-score, a negative amount, an
    out-of-range risk score, an unrecognized fraud_pattern label.
    """
    validate_amount(fd["txn_amount"], "txn_amount")
    validate_probability(fd["risk_score_raw"], "risk_score_raw", allow_none=False)
    # z-score is unbounded but must be finite (NaN would poison AVG()).
    z = fd["amount_zscore"]
    if z is not None and (not isinstance(z, (int, float)) or math.isnan(z) or math.isinf(z)):
        raise ValidationError(f"amount_zscore must be finite, got {z!r}.")
    validate_identified_pattern(fd["fraud_pattern"], "fraud_pattern")


# =============================================================
# ENRICHMENT + SCORING — one transaction -> one feature row
# =============================================================
def build_feature_row(row, *, baseline, last_loc, velocity_15min, user_devices, config,
                      now: Optional[datetime] = None, validate: bool = True) -> dict:
    """
    Turn one parsed transaction (a Spark Row or a plain dict — both
    support row["field"]) into a validated feature row. This is the
    exact per-transaction logic that used to live inline in
    feature_engine's foreachBatch loop, extracted so it can be tested
    on real data with no Spark/Redis/Kafka.

    All the fraud-scoring reasoning is preserved: amount z-score vs the
    user baseline, geo Haversine + the out-of-order ordering guard, the
    new-device flag, the weighted composite, the two-tier hard rules,
    and the poisoning-guard `suspect_location` flag.

    Raises ValidationError (when validate=True) if the resulting row is
    corrupt — the caller isolates that one transaction and keeps going.
    """
    now = now or utc_now()
    user_id = row["user_id"]

    # ---- Amount z-score ----
    avg_amt    = baseline.get("avg_transaction_amt", 50.0)
    stddev_amt = baseline.get("stddev_transaction_amt", 20.0)
    amount     = float(row["amount"] or 0)
    zscore     = (amount - avg_amt) / stddev_amt if stddev_amt > 0 else 0.0

    # ---- Geo features from Redis state ----
    geo_distance_km = None
    time_since_last_min = None
    prev_city = None
    prev_ts = None

    if last_loc and row["latitude"] and row["longitude"]:
        try:
            geo_distance_km = haversine_distance(
                last_loc["lat"], last_loc["lon"],
                float(row["latitude"]), float(row["longitude"])
            )
            prev_city = last_loc.get("city")
            prev_ts = last_loc.get("ts")

            if prev_ts:
                txn_ts = row["transaction_ts"]
                if txn_ts and prev_ts:
                    try:
                        prev_dt = datetime.fromisoformat(str(prev_ts).replace("Z", ""))
                        curr_dt = txn_ts if isinstance(txn_ts, datetime) else datetime.fromisoformat(str(txn_ts))
                        time_since_last_min = (curr_dt - prev_dt).total_seconds() / 60
                    except Exception:
                        pass

            # ---- Ordering guard (GEO_FLAGGING_INVESTIGATION.md) ----
            # A non-positive delta means this event arrived out of
            # event-time order and the "previous" location is one the
            # user visits in the FUTURE — the distance is meaningless,
            # not merely imprecise. Null the derived features.
            if time_since_last_min is not None and time_since_last_min <= 0:
                geo_distance_km = None
                time_since_last_min = None
        except Exception as e:
            logger.warning("Geo computation failed for %s: %s", user_id, e)

    # ---- New device flag ----
    device_id = row["device_id"]
    is_new_device = device_id not in user_devices if device_id else False

    # ---- Signals + risk score ----
    v15 = min(velocity_15min / VELOCITY_MAX_NORMAL, 1.0)
    amt_signal = min(abs(zscore) / ZSCORE_MAX_NORMAL, 1.0)
    geo_signal = min((geo_distance_km or 0) / GEO_MAX_NORMAL_KM, 1.0)
    dev_signal = 1.0 if is_new_device else 0.0

    risk_score = compute_risk_score(v15, amt_signal, geo_signal, dev_signal, config.RISK_WEIGHTS)

    # ---- Flag decision: weighted score OR hard-rule override ----
    implied_speed_kmh = compute_implied_speed_kmh(geo_distance_km, time_since_last_min)
    geo_hard_triggered = geo_hard_rule(geo_distance_km, implied_speed_kmh)
    hard_rule_triggered = (
        geo_hard_triggered or
        amt_signal >= config.HARD_RULE_AMOUNT_THRESHOLD or
        v15 >= config.HARD_RULE_VELOCITY_THRESHOLD
    )
    is_flagged = (risk_score > config.RISK_FLAG_THRESHOLD) or hard_rule_triggered

    txn_ts_str = str(row["transaction_ts"]) if row["transaction_ts"] else None

    fd = {
        "snapshot_id":            str(uuid.uuid4()),
        "transaction_id":         row["transaction_id"],
        "user_id":                user_id,
        "user_surrogate_key":     baseline.get("surrogate_key", ""),
        "computed_at":            now.isoformat(),
        "transaction_ts":         txn_ts_str,
        "velocity_5min":          None,
        "velocity_15min":         velocity_15min,
        "velocity_1hr":           None,
        "velocity_24hr":          None,
        "txn_amount":             amount,
        "user_avg_amount":        avg_amt,
        "user_stddev_amount":     stddev_amt,
        "amount_zscore":          round(zscore, 4),
        "prev_transaction_city":  prev_city,
        "prev_transaction_ts":    prev_ts,
        "geo_distance_km":        round(geo_distance_km, 2) if geo_distance_km else None,
        "time_since_last_txn_min": round(time_since_last_min, 2) if time_since_last_min else None,
        "is_new_device":          is_new_device,
        "device_id":              device_id,
        "city":                   row["city"],
        "country":                row["country"],
        "latitude":               float(row["latitude"]) if row["latitude"] else None,
        "longitude":              float(row["longitude"]) if row["longitude"] else None,
        # Poisoning guard (GEO_FLAGGING_INVESTIGATION.md): a location
        # that itself tripped the geo hard rule must not become the
        # user's new baseline — RedisFeatureWriter skips the :location
        # update for suspect rows.
        "suspect_location":       geo_hard_triggered,
        "risk_score_raw":         risk_score,
        "is_flagged_for_review":  is_flagged,
        "is_synthetic_fraud":     row["is_synthetic_fraud"],
        "fraud_pattern":          row["fraud_pattern"],
    }

    if validate:
        validate_feature_row(fd)
    return fd


# Columns the Snowflake row carries (subset of the feature dict) —
# see FACT_FEATURE_SNAPSHOTS_INSERT_SQL / the schema contract.
_SNOWFLAKE_ROW_KEYS = [
    "snapshot_id", "transaction_id", "user_id", "user_surrogate_key",
    "velocity_5min", "velocity_15min", "velocity_1hr", "velocity_24hr",
    "txn_amount", "user_avg_amount", "user_stddev_amount", "amount_zscore",
    "prev_transaction_city", "prev_transaction_ts", "geo_distance_km",
    "time_since_last_txn_min", "is_new_device", "device_id",
    "risk_score_raw", "is_flagged_for_review", "is_synthetic_fraud", "fraud_pattern",
]


def to_snowflake_row(fd: dict, now: Optional[datetime] = None) -> dict:
    """Project a feature dict to the Snowflake insert row. computed_at
    is a fresh tz-aware UTC value (the connector writes it to the NTZ
    column); everything else is copied from the validated feature dict."""
    row = {k: fd[k] for k in _SNOWFLAKE_ROW_KEYS}
    row["computed_at"] = now or utc_now()
    return row
