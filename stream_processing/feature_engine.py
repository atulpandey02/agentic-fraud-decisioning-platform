# =============================================================
# FRAUD FEATURE ENGINE — Spark Structured Streaming
# =============================================================
# Consumes fraud-transactions topic from Kafka.
# Computes real-time features per transaction:
#   - Velocity: sliding windows (5min, 15min, 1hr, 24hr)
#   - Amount:   z-score vs user historical baseline
#   - Geo:      Haversine distance from last known location
#               (state stored in Redis, not Spark checkpoint)
#   - Device:   new device flag vs DIM_DEVICES
#   - Risk:     weighted heuristic composite score
#
# Dual sink:
#   - Redis     → online feature store (agent reads here)
#   - Snowflake → FEATURES.FACT_FEATURE_SNAPSHOTS (audit)
#
# Output mode: Complete (for learning/observability)
# Production upgrade: Update mode for Redis, Append for Snowflake
# =============================================================

import os
import io
import uuid
import json
import math
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional, List

import redis
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    BooleanType, TimestampType, IntegerType
)
from dotenv import load_dotenv

from window_config import (
    VELOCITY_WINDOWS,
    WATERMARK_DELAY as _WATERMARK_DELAY,
    PRIMARY_WINDOW_DURATION,
    PRIMARY_SLIDE_DURATION,
    VELOCITY_FLAG_THRESHOLD as _VELOCITY_FLAG_THRESHOLD,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================
# CONFIGURATION
# =============================================================
class FeatureEngineConfig:
    """
    All tunable parameters in one place.
    Change thresholds here without touching engine logic.
    """
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "fraud-transactions")
    KAFKA_STARTING_OFFSETS  = "earliest"   # process all backfill + future events
    KAFKA_GROUP_ID          = "spark-fraud-feature-engine"

    # Spark
    SPARK_APP_NAME          = "FraudFeatureEngine"
    SPARK_MASTER            = "local[*]"   # use all available cores on Mac
    TRIGGER_INTERVAL        = "10 seconds" # micro-batch frequency
        # reduced from 30s — the old bottleneck (row-by-row Snowflake
        # insert) is gone as of Day 3's S3/Snowpipe fix, and the N+1
        # Redis reads are now single MGET calls, so batches finish
        # well under 30s. No reason to wait that long between batches.
    CHECKPOINT_DIR          = "/tmp/fraud-feature-checkpoint"
    VELOCITY_CHECKPOINT_DIR = "/tmp/fraud-velocity-checkpoint"  # same path
        # velocity_engine.py uses — running the query from inside this
        # class instead of a separate process resumes the SAME
        # checkpoint state, no discontinuity from the merge

    # Sliding window definitions now live in window_config.py — shared
    # with velocity_engine.py so the two never silently drift apart on
    # duration or threshold (this was flagged as open duplication in
    # the Day 3 learning sheet; fixed here as part of the merge).
    # event time based — transaction_ts, not processing time
    # Why: fraud velocity = what the user DID, not when Spark read it
    WINDOWS = VELOCITY_WINDOWS
    WATERMARK_DELAY = _WATERMARK_DELAY  # wait up to 10 min for late events

    # Risk scoring weights (heuristic, pre-agent)
    # These are directionally correct but not ML-tuned
    RISK_WEIGHTS = {
        "velocity_15min":  0.30,   # high weight — most reliable fraud signal
        "amount_zscore":   0.25,   # strong signal for AMOUNT_ANOMALY
        "geo_distance":    0.25,   # strong signal for GEO_JUMP
        "new_device":      0.20,   # supporting signal, not standalone
    }
    RISK_FLAG_THRESHOLD = 0.60     # flag for agent review if score > 0.6

    # -----------------------------------------------------------
    # HARD-RULE SINGLE-SIGNAL OVERRIDES — added Day 4
    #
    # Diagnosed via precision_recall_check.sql on a clean 1M-row
    # dataset: GEO_JUMP (5.6%), AMOUNT_ANOMALY (6.7%), and
    # VELOCITY_SPIKE (8.5%) catch rates were all near-zero because
    # our fraud generator deliberately makes these SINGLE-SIGNAL
    # patterns — a pure geo-jump only elevates geo_distance and
    # leaves amount/device/velocity completely normal. A same-
    # weighted additive score maxing at 0.25 for any one signal
    # can never cross a 0.60 threshold alone, no matter how
    # extreme that one signal is. Real fraud engines solve this
    # with a two-tier structure: hard rules for individually
    # severe signals, plus a weighted score for combinations of
    # moderate signals. This is that fix.
    #
    # NEW_DEVICE deliberately does NOT get a hard-rule override.
    # Our own DIM_DEVICES generator (Day 1) marks some of a
    # user's real, previously-used secondary devices as
    # is_trusted=False — meaning is_new_device=True fires on
    # plenty of genuinely legitimate transactions too, not just
    # fraud. Auto-flagging on that signal alone would spike false
    # positives on ordinary behavior. It stays a WEIGHTED signal
    # only — it can help push a combined score over threshold,
    # but never triggers a flag by itself.
    #
    # GEO HARD RULE REVISED (see GEO_FLAGGING_INVESTIGATION.md):
    # the original geo override (geo_signal >= 0.95, i.e. distance
    # >= 475km) alone produced 75% of all flags at 17.3% precision
    # — 475km is DOMESTIC-travel distance, not impossible travel.
    # The production concept is implied SPEED (>900 km/h), which
    # this pipeline always computed but never used in scoring.
    # The rule is now composite:
    #   speed arm    — implied speed > 900 km/h, with a 100km
    #                  distance floor so meter-scale GPS jitter
    #                  over second-scale deltas can't fabricate
    #                  supersonic "speeds";
    #   distance arm — >= 3000km, true intercontinental-jump
    #                  scale (the generator's minimum injected
    #                  jump is 5000km; normal domestic travel
    #                  tops out ~500km — 3000 splits them with
    #                  margin), kept because 80% of rows lack a
    #                  usable time delta and the speed arm alone
    #                  would drop GEO_JUMP recall from 99% to 9%.
    # Measured on the full 1M-row history: flag rate 44.4% ->
    # 26.6%, precision 33.3% -> 52.6%, GEO_JUMP recall 99.2% ->
    # 98.9% — before counting the two state-hygiene guards (see
    # process_batch and RedisFeatureWriter), which remove the
    # corruption the simulation can't model.
    # -----------------------------------------------------------
    HARD_RULE_AMOUNT_THRESHOLD   = 0.95  # amt_signal >= this -> auto-flag
    HARD_RULE_VELOCITY_THRESHOLD = 1.0   # v15 fully saturated (>=5 txns/15min,
                                          # the actual JP Morgan threshold) -> auto-flag
    HARD_RULE_GEO_MIN_JUMP_KM    = 3000.0  # distance arm of the composite geo rule
    IMPOSSIBLE_TRAVEL_SPEED_KMH  = 900.0   # speed arm — commercial-jet ceiling,
                                            # the same figure the generator injects against
    SPEED_RULE_MIN_DISTANCE_KM   = 100.0   # floor under the speed arm (GPS-noise guard)

    # Velocity normalization for risk scoring
    # velocity_15min > 5 = JP Morgan flag threshold
    VELOCITY_MAX_NORMAL = 5.0

    # Geo normalization
    # > 900 km/h = impossible travel (commercial jet speed)
    GEO_MAX_NORMAL_KM = 500.0     # within 500km = normal domestic travel

    # Amount z-score normalization
    # z > 3 = 3 standard deviations = AMOUNT_ANOMALY
    ZSCORE_MAX_NORMAL = 3.0

    # Redis
    REDIS_HOST    = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB      = int(os.getenv("REDIS_DB", 0))
    REDIS_TTL_SEC = 86400          # 24hr TTL — stale features auto-expire

    # Snowflake
    SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DETECTION")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    @classmethod
    def for_testing(cls) -> "FeatureEngineConfig":
        """
        Alternate constructor — @classmethod, not @staticmethod,
        because it needs cls() to build an actual instance of
        whichever class it's called on. A staticmethod couldn't
        do that; it has no access to the class itself, only
        whatever arguments you pass it directly.

        Usage:
            config = FeatureEngineConfig.for_testing()
            # instead of:
            config = FeatureEngineConfig()

        Returns a config with a faster trigger interval and
        smaller flush thresholds, for quick local iteration
        without editing the production constants above or
        needing a second config file.
        """
        config = cls()
        config.TRIGGER_INTERVAL = "5 seconds"
        config.FLUSH_EVERY_N_BATCHES = 2
        config.FLUSH_MAX_ROWS = 5_000
        return config

    # -----------------------------------------------------------
    # S3 STAGING (replaces direct row-by-row Snowflake insert)
    # Feature rows are buffered in memory across micro-batches,
    # then flushed as a single Parquet file to S3. Snowpipe
    # (AUTO_INGEST, SQS-triggered) picks up the file and loads
    # it into FACT_FEATURE_SNAPSHOTS automatically — no polling,
    # no manual COPY INTO, no per-row Python connector overhead.
    #
    # Why buffer instead of writing every micro-batch immediately:
    #   Parquet is a columnar format with per-file overhead
    #   (footer, schema, compression setup). Writing a 3-row
    #   Parquet file every 30 seconds is wasteful — better to
    #   accumulate rows across several micro-batches and write
    #   fewer, larger files. This is the same "small files problem"
    #   that shows up in every columnar/data-lake system.
    # -----------------------------------------------------------
    S3_BUCKET           = os.getenv("S3_BUCKET", "fraud-detection-platform-staging")
    S3_PREFIX           = os.getenv("S3_PREFIX", "features")
    S3_REGION           = os.getenv("AWS_REGION", "ca-central-1")
    FLUSH_EVERY_N_BATCHES = 5       # flush accumulated rows every 5 micro-batches
    FLUSH_MAX_ROWS         = 50_000 # or flush early if buffer exceeds this many rows


# =============================================================
# TRANSACTION SCHEMA
# =============================================================
# Explicit schema for JSON deserialization from Kafka.
# Never use schema inference in streaming — it requires
# a full scan to infer types, which doesn't work on
# an unbounded stream and adds startup latency.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    True),
    StructField("user_id",           StringType(),    True),
    StructField("device_id",         StringType(),    True),
    StructField("amount",            FloatType(),     True),
    StructField("currency",          StringType(),    True),
    StructField("merchant_name",     StringType(),    True),
    StructField("merchant_category", StringType(),    True),
    StructField("city",              StringType(),    True),
    StructField("country",           StringType(),    True),
    StructField("latitude",          FloatType(),     True),
    StructField("longitude",         FloatType(),     True),
    StructField("transaction_ts",    StringType(),    True),
    StructField("is_synthetic_fraud",BooleanType(),   True),
    StructField("fraud_pattern",     StringType(),    True),
])


# =============================================================
# GEO CALCULATOR (UDF)
# =============================================================
def haversine_distance(lat1, lon1, lat2, lon2) -> float:
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

    Kept as a module-level pure function (like haversine_distance
    above, unlike the batch logic in process_batch) so the scoring
    rule is unit-testable without Spark, Redis, or Kafka.
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

    Reads thresholds from FeatureEngineConfig class attributes
    directly — this rule is part of the config's documented
    contract, and the tests pin those values on purpose: changing
    a threshold should require touching the config AND its test,
    never just one.
    """
    if distance_km is None:
        return False
    if distance_km >= FeatureEngineConfig.HARD_RULE_GEO_MIN_JUMP_KM:
        return True
    return (
        implied_speed_kmh is not None
        and implied_speed_kmh > FeatureEngineConfig.IMPOSSIBLE_TRAVEL_SPEED_KMH
        and distance_km >= FeatureEngineConfig.SPEED_RULE_MIN_DISTANCE_KM
    )


# =============================================================
# FEATURE SINK — abstract base class
# =============================================================
class FeatureSink(ABC):
    """
    Shared contract for every destination the pipeline writes
    feature rows to. Three concrete sinks implement this:
    RedisFeatureWriter, S3FeatureWriter, SnowflakeFeatureWriter.

    Why an ABC here, concretely:
        All three classes do conceptually the same job — accept a
        batch of rows, get them to their destination — but do it
        in completely different ways (Redis: dedupe + pipeline;
        S3: buffer + Parquet flush; Snowflake: row-by-row insert,
        kept only as a manual fallback). Without a shared base,
        FraudFeatureEngine would need to know each class's exact
        method names individually. With it, the engine can treat
        "a list of sinks" uniformly for anything that applies to
        all of them — the shutdown sequence below is the real,
        concrete example: every sink gets flush() called on it
        the same way, regardless of what flushing even means for
        that particular sink.

    write_batch is abstract — every subclass MUST implement it,
    enforced at class-definition time by Python itself (trying to
    instantiate a subclass that skips it raises TypeError before
    any code even runs). flush is NOT abstract — it has a default
    no-op body, because two of the three sinks (Redis, Snowflake)
    write immediately and have nothing to buffer. This is a
    legitimate, common pattern: an ABC guarantees a MINIMUM shared
    contract, not that every method means something for every
    subclass. A no-op override is a real, honest implementation,
    not a workaround.
    """

    @abstractmethod
    def write_batch(self, rows: List[Dict]) -> None:
        """Accept a batch of feature rows for this sink to handle."""
        ...

    def flush(self) -> Optional[str]:
        """
        Default: no-op. Sinks that write immediately (Redis,
        Snowflake) have nothing to flush — this base implementation
        covers them without either class needing to write empty
        boilerplate. S3FeatureWriter overrides this with real
        buffering logic, since it's the one sink that actually
        accumulates rows before writing.
        """
        return None

    def close(self) -> None:
        """
        Default: no-op. The other half of "open once, reuse, close
        when done" — sinks holding a real connection (currently
        SnowflakeFeatureWriter; S3FeatureWriter's boto3 client is
        stateless enough not to need this) override it to release
        that connection cleanly on shutdown.
        """
        return None


# =============================================================
# REDIS FEATURE WRITER
# =============================================================
class RedisFeatureWriter(FeatureSink):
    """
    Writes latest computed features per user to Redis.
    Also serves as the geo-state store — reads last known
    location for Haversine computation, writes new location.

    Key pattern:
        user:{user_id}:features  → JSON blob, full feature snapshot
        user:{user_id}:location  → JSON {lat, lon, city, ts}

    Why separate location key:
        The agent reads :features for fraud decisioning.
        The feature engine reads :location for geo-distance.
        Keeping them separate means geo-state reads don't
        pollute the agent's feature reads with internal state.
    """

    def __init__(self, config: FeatureEngineConfig):
        self._config = config
        self._client: Optional[redis.Redis] = None

    def connect(self) -> "RedisFeatureWriter":
        self._client = redis.Redis(
            host=self._config.REDIS_HOST,
            port=self._config.REDIS_PORT,
            db=self._config.REDIS_DB,
            decode_responses=True,
        )
        self._client.ping()
        logger.info(f"Redis connected at {self._config.REDIS_HOST}:{self._config.REDIS_PORT}")
        return self

    def get_last_location(self, user_id: str) -> Optional[Dict]:
        """Read last known location for geo-distance computation."""
        if not self._client:
            return None
        try:
            raw = self._client.get(f"user:{user_id}:location")
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning(f"Redis read failed for {user_id}: {e}")
            return None

    def get_last_locations_batch(self, user_ids: List[str]) -> Dict[str, Optional[Dict]]:
        """
        Read last known locations for MANY users in a SINGLE
        Redis round trip using MGET, instead of one GET per user.

        Why this matters (the N+1 problem):
            The original per-row loop called get_last_location()
            once per transaction inside the foreachBatch Python
            loop — 10,000 transactions/batch = 10,000 separate
            network round trips to Redis, each paying network
            latency even though the actual GET operation itself
            is sub-millisecond. MGET fetches all keys in exactly
            ONE round trip regardless of how many keys you ask for.
            This is the same fix already applied to Redis WRITES
            (write_batch dedupes + pipelines) — now applied to READS.

        Returns:
            dict mapping user_id -> location dict (or None if
            no prior location exists for that user yet)
        """
        if not self._client or not user_ids:
            return {}

        # De-duplicate — if the same user appears 50 times in this
        # batch, we only need to fetch their location ONCE, not 50x
        unique_user_ids = list(set(user_ids))
        keys = [f"user:{uid}:location" for uid in unique_user_ids]

        try:
            raw_values = self._client.mget(keys)  # ONE round trip
        except Exception as e:
            logger.warning(f"Redis MGET batch failed: {e}")
            return {uid: None for uid in unique_user_ids}

        result: Dict[str, Optional[Dict]] = {}
        for user_id, raw in zip(unique_user_ids, raw_values):
            if raw:
                try:
                    result[user_id] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    result[user_id] = None
            else:
                result[user_id] = None

        return result

    def get_velocity_batch(self, user_ids: List[str]) -> Dict[str, int]:
        """
        Read velocity_15min for MANY users in one Redis round trip.

        Same N+1 fix pattern as get_last_locations_batch — this is
        the READ side of the velocity-merge: the standalone
        VelocityEngine query (running concurrently, see
        FraudFeatureEngine.run()) writes user:{user_id}:velocity
        independently; this method is how the main feature pipeline
        picks that value up to fold into the risk score and the
        feature snapshot, without either query blocking the other.

        Returns:
            dict mapping user_id -> velocity_15min (0 if no recent
            velocity data exists yet for that user — e.g. their
            first transaction, or the velocity query hasn't caught
            up to this offset yet).
        """
        if not self._client or not user_ids:
            return {}

        unique_user_ids = list(set(user_ids))
        keys = [f"user:{uid}:velocity" for uid in unique_user_ids]

        try:
            raw_values = self._client.mget(keys)
        except Exception as e:
            logger.warning(f"Redis velocity MGET batch failed: {e}")
            return {uid: 0 for uid in unique_user_ids}

        result: Dict[str, int] = {}
        for user_id, raw in zip(unique_user_ids, raw_values):
            if raw:
                try:
                    payload = json.loads(raw)
                    result[user_id] = int(payload.get("velocity_15min", 0))
                except (json.JSONDecodeError, TypeError, ValueError):
                    result[user_id] = 0
            else:
                result[user_id] = 0

        return result

    def write_velocity_batch(self, velocity_rows: List[Dict], flag_threshold: int) -> int:
        """
        Write velocity window results to Redis in one pipelined call.
        Called by the velocity query's foreachBatch sink — kept as a
        proper method here (not a raw _client.pipeline() reach-in from
        outside the class) so RedisFeatureWriter stays the single
        place that knows the key naming scheme and connection details.

        Returns the count of users flagged over threshold, for logging.
        """
        if not self._client or not velocity_rows:
            return 0

        pipe = self._client.pipeline()
        flagged_count = 0
        for row in velocity_rows:
            is_flagged = row["velocity_15min"] > flag_threshold
            if is_flagged:
                flagged_count += 1
            payload = {
                "user_id": row["user_id"],
                "velocity_15min": row["velocity_15min"],
                "window_start": row["window_start"],
                "window_end": row["window_end"],
                "is_flagged": is_flagged,
            }
            pipe.setex(
                f"user:{row['user_id']}:velocity",
                self._config.REDIS_TTL_SEC,
                json.dumps(payload, default=str)
            )
        pipe.execute()
        return flagged_count

    def write_features(self, user_id: str, features: Dict) -> None:
        """Write full feature snapshot — agent reads this."""
        if not self._client:
            return
        try:
            pipe = self._client.pipeline()
            # Full feature blob for agent
            pipe.setex(
                f"user:{user_id}:features",
                self._config.REDIS_TTL_SEC,
                json.dumps(features, default=str)
            )
            # Location state for next geo computation — SKIPPED for
            # suspect locations (geo hard rule fired on this txn):
            # letting an impossible-jump location become the baseline
            # is how one fraud manufactures a chain of false flags on
            # the user's next legitimate purchases (poisoning guard,
            # GEO_FLAGGING_INVESTIGATION.md). Trade-off, accepted and
            # documented there: a user who genuinely relocates at jump
            # speed updates their baseline on their first NON-flagged
            # transaction instead of immediately.
            if not features.get("suspect_location"):
                pipe.setex(
                    f"user:{user_id}:location",
                    self._config.REDIS_TTL_SEC,
                    json.dumps({
                        "lat":  features.get("latitude"),
                        "lon":  features.get("longitude"),
                        "city": features.get("city"),
                        "ts":   features.get("transaction_ts"),
                    })
                )
            pipe.execute()
        except Exception as e:
            logger.error(f"Redis write failed for {user_id}: {e}")

    def write_batch(self, features_list: list) -> None:
        """
        Batch write LATEST features per user to Redis.

        Critical deduplication step:
            A micro-batch of 500K transactions across 10,000 users
            means ~50 transactions per user on average. Writing all
            500K to Redis would:
            1. Overwhelm the pipeline with redundant writes
               (499,990 of which immediately get overwritten)
            2. Hit Redis maxmemory because the pipeline holds all
               writes in memory before executing — even if LRU
               eviction is configured, it can't evict fast enough
               during a single pipeline.execute() call

            Fix: deduplicate to ONE entry per user — the latest
            transaction by transaction_ts. This is all Redis needs
            — it's an online store for CURRENT user state, not
            a history store. History goes to Snowflake.

            Result: 500K rows → 10,000 Redis writes per micro-batch
            instead of 500,000. 50x reduction in write volume.
        """
        if not self._client or not features_list:
            return

        # Deduplicate — keep only the latest feature per user
        # by sorting on transaction_ts and taking the last entry
        latest_per_user: Dict[str, dict] = {}
        for features in features_list:
            user_id = features.get("user_id")
            if not user_id:
                continue
            existing = latest_per_user.get(user_id)
            if existing is None:
                latest_per_user[user_id] = features
            else:
                # Keep whichever has the later transaction_ts
                try:
                    curr_ts = str(features.get("transaction_ts", ""))
                    prev_ts = str(existing.get("transaction_ts", ""))
                    if curr_ts > prev_ts:
                        latest_per_user[user_id] = features
                except Exception:
                    latest_per_user[user_id] = features

        # Now write one entry per user — manageable pipeline size
        pipe = self._client.pipeline()
        for user_id, features in latest_per_user.items():
            pipe.setex(
                f"user:{user_id}:features",
                self._config.REDIS_TTL_SEC,
                json.dumps(features, default=str)
            )
            # Poisoning guard — same rule as write_features(), see
            # the comment there. Dedup interaction, acknowledged: if
            # the user's LATEST txn in this batch is suspect, the
            # location update is skipped entirely for this batch even
            # when an earlier in-batch txn was clean — the state then
            # simply keeps its previous trusted value, which is the
            # guard's whole point.
            if not features.get("suspect_location"):
                pipe.setex(
                    f"user:{user_id}:location",
                    self._config.REDIS_TTL_SEC,
                    json.dumps({
                        "lat":  features.get("latitude"),
                        "lon":  features.get("longitude"),
                        "city": features.get("city"),
                        "ts":   features.get("transaction_ts"),
                    })
                )
        pipe.execute()
        logger.info(
            f"Redis batch write: {len(latest_per_user):,} users "
            f"(deduplicated from {len(features_list):,} transactions)"
        )


# =============================================================
# SNOWFLAKE FEATURE WRITER
# =============================================================
# =============================================================
# S3 FEATURE WRITER — buffered Parquet staging for Snowpipe
# =============================================================
class S3FeatureWriter(FeatureSink):
    """
    Accumulates feature rows across micro-batches, then flushes
    them as a single Parquet file to S3. Snowpipe (AUTO_INGEST,
    SQS-notified) picks up the file and loads it automatically.

    Why this replaces the row-by-row Snowflake executemany insert:
        executemany was doing ~10K individual row inserts via the
        Python connector, taking minutes per micro-batch and
        becoming the dominant bottleneck in the whole pipeline.
        Writing one Parquet file and letting Snowflake's own
        COPY-based ingestion (via Snowpipe) load it is the same
        pattern production systems use — the load happens via
        Snowflake's bulk loader, not a client-side loop.

    This still runs on the driver (not a Spark-native executor
    write) because we're working with plain Python dicts already
    collected for the Redis geo-state lookup — but going from
    "10K row-by-row INSERTs" to "1 Parquet file + Snowflake's own
    COPY" is the single biggest throughput fix available without
    re-architecting the geo/Redis lookup into a Spark UDF.
    """

    def __init__(self, config: FeatureEngineConfig):
        self._config = config
        self._s3_client = boto3.client("s3", region_name=config.S3_REGION)
        self._buffer: List[Dict] = []
        self._batches_since_flush = 0

    def write_batch(self, rows: List[Dict]) -> None:
        """
        FeatureSink interface implementation — this is what makes
        S3FeatureWriter satisfy the abstract contract. Delegates
        straight to add_batch(); kept as a separate method (not a
        rename of add_batch) because "write_batch" is the shared,
        caller-facing name every sink answers to, while "add_batch"
        is more accurate about what actually happens here — rows
        get buffered, not written yet. should_flush()/flush() stay
        as S3-specific extra API beyond the shared interface, for
        callers that need to control the buffering cadence
        explicitly (see FraudFeatureEngine's process_batch).
        """
        self.add_batch(rows)

    def add_batch(self, rows: List[Dict]) -> None:
        """Add a micro-batch's feature rows to the in-memory buffer."""
        self._buffer.extend(rows)
        self._batches_since_flush += 1

    def should_flush(self) -> bool:
        return (
            self._batches_since_flush >= self._config.FLUSH_EVERY_N_BATCHES
            or len(self._buffer) >= self._config.FLUSH_MAX_ROWS
        )

    def flush(self) -> Optional[str]:
        """
        Write the buffered rows to S3 as a single Parquet file.
        Returns the S3 key written, or None if buffer was empty.

        File naming includes a UUID + timestamp to guarantee
        uniqueness — Snowpipe processes each file exactly once
        by object key, so colliding names would silently drop data.
        """
        if not self._buffer:
            return None

        table = pa.Table.from_pylist(self._buffer)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_id = str(uuid.uuid4())[:8]
        key = f"{self._config.S3_PREFIX}/features_{timestamp}_{file_id}.parquet"

        self._s3_client.put_object(
            Bucket=self._config.S3_BUCKET,
            Key=key,
            Body=buf.getvalue(),
        )

        row_count = len(self._buffer)
        logger.info(
            f"S3: flushed {row_count:,} rows -> "
            f"s3://{self._config.S3_BUCKET}/{key} "
            f"({len(buf.getvalue()) / 1024:.1f} KB, snappy compressed)"
        )

        self._buffer = []
        self._batches_since_flush = 0
        return key


class SnowflakeFeatureWriter(FeatureSink):
    """
    Writes feature snapshots to FEATURES.FACT_FEATURE_SNAPSHOTS.
    Called once per micro-batch via foreachBatch.

    Why foreachBatch instead of native Snowflake sink:
        Spark doesn't have a native Snowflake streaming sink.
        foreachBatch gives us a regular DataFrame per trigger
        that we can write using the Snowflake Spark connector
        or direct JDBC — we use direct connector for control.

    Connection reuse — NOT a Singleton, and why that distinction
    matters:
        Each micro-batch used to open a brand new Snowflake
        connection and close it immediately after — real, avoidable
        overhead, since it's always the SAME instance being called
        sequentially by the main query's foreachBatch (Spark never
        runs two micro-batches of the same query concurrently).
        So this class now lazily caches one connection and reuses
        it across calls, reconnecting only if it's been closed or
        dropped.

        This is deliberately an INSTANCE-level cache, not a class-
        level Singleton. A Singleton would mean this one connection
        is shared globally, including by code that has no business
        touching it — dangerous the moment two things could call it
        concurrently (this process runs two concurrent Spark
        queries; if the velocity query ever needed Snowflake too,
        sharing this same connection object across both would risk
        corrupted results, since snowflake-connector-python
        connections aren't safe for concurrent use across threads).
        Keeping it as a private attribute on ONE instance, used by
        ONE caller, is what makes the reuse safe.
    """

    def __init__(self, config: FeatureEngineConfig):
        self._config = config
        self._conn = None  # lazily created, reused across calls

    def _get_connection(self):
        """
        Return the cached connection if it's still alive; open a
        fresh one otherwise. is_closed() catches the case where
        Snowflake itself dropped the session (idle timeout, network
        blip) — reconnecting here means one bad connection doesn't
        permanently break every future write for the rest of the run.
        """
        if self._conn is None or self._conn.is_closed():
            logger.info("Opening Snowflake connection (none cached, or previous one closed)")
            self._conn = snowflake.connector.connect(
                account=self._config.SNOWFLAKE_ACCOUNT,
                user=self._config.SNOWFLAKE_USER,
                password=self._config.SNOWFLAKE_PASSWORD,
                database=self._config.SNOWFLAKE_DATABASE,
                warehouse=self._config.SNOWFLAKE_WAREHOUSE,
                role=self._config.SNOWFLAKE_ROLE,
                schema="FEATURES",
            )
        return self._conn

    def test_connection(self) -> None:
        """
        Test Snowflake connectivity and table existence at startup.
        Fails fast with a clear error rather than silently failing
        on every micro-batch. Called once before stream starts.
        Deliberately does NOT close the connection afterward — this
        is the same cached connection write_batch() will reuse.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FEATURES.FACT_FEATURE_SNAPSHOTS")
        count = cursor.fetchone()[0]
        logger.info(f"Snowflake connection OK — FACT_FEATURE_SNAPSHOTS has {count} rows")

    def write_batch(self, features_list: list) -> None:
        """
        Batch insert feature snapshots into Snowflake.
        Reuses the cached connection — no open/close per call.
        """
        if not features_list:
            return

        sql = """
            INSERT INTO FEATURES.FACT_FEATURE_SNAPSHOTS (
                snapshot_id, transaction_id, user_id,
                user_surrogate_key, computed_at,
                velocity_5min, velocity_15min,
                velocity_1hr, velocity_24hr,
                txn_amount, user_avg_amount, user_stddev_amount,
                amount_zscore,
                prev_transaction_city, prev_transaction_ts,
                geo_distance_km, time_since_last_txn_min,
                is_new_device, device_id,
                risk_score_raw, is_flagged_for_review
            ) VALUES (
                %(snapshot_id)s, %(transaction_id)s, %(user_id)s,
                %(user_surrogate_key)s, %(computed_at)s,
                %(velocity_5min)s, %(velocity_15min)s,
                %(velocity_1hr)s, %(velocity_24hr)s,
                %(txn_amount)s, %(user_avg_amount)s, %(user_stddev_amount)s,
                %(amount_zscore)s,
                %(prev_transaction_city)s, %(prev_transaction_ts)s,
                %(geo_distance_km)s, %(time_since_last_txn_min)s,
                %(is_new_device)s, %(device_id)s,
                %(risk_score_raw)s, %(is_flagged_for_review)s
            )
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, features_list)
            conn.commit()
            logger.info(f"Snowflake: inserted {len(features_list)} feature snapshots")
        except Exception as e:
            conn.rollback()
            # Re-raise so the error surfaces in foreachBatch
            # and is visible in logs — not silently swallowed.
            # Deliberately does NOT close the connection here —
            # a query-level failure (bad SQL, constraint violation)
            # doesn't mean the connection itself is broken. Closing
            # it would force an unnecessary reconnect on the very
            # next batch. is_closed() in _get_connection() already
            # handles the case where the connection itself died.
            logger.error(f"Snowflake write FAILED: {e}")
            raise

    def close(self) -> None:
        """
        FeatureSink lifecycle hook — closes the cached connection
        on shutdown. This is the "close it once everyone's done"
        half of the pattern: safe here specifically because this
        instance has exactly one caller (the main query's
        foreachBatch, called sequentially), so there's no risk of
        another thread reaching for this connection after it's
        closed.
        """
        if self._conn is not None and not self._conn.is_closed():
            self._conn.close()
            logger.info("Snowflake connection closed.")
            self._conn = None


# =============================================================
# FRAUD FEATURE ENGINE
# =============================================================
class FraudFeatureEngine:
    """
    Orchestrates the full streaming feature computation pipeline.

    Architecture:
        Kafka → parse → velocity windows → amount z-score
              → geo features (Redis state) → risk score
              → dual sink (Redis + Snowflake)

    Usage:
        engine = FraudFeatureEngine()
        engine.run()   # blocking, runs until interrupted
    """

    def __init__(self):
        self._config = FeatureEngineConfig()
        self._spark: Optional[SparkSession] = None
        self._redis_writer = RedisFeatureWriter(self._config)
        self._sf_writer = SnowflakeFeatureWriter(self._config)   # kept for test_connection() / manual fallback
        self._s3_writer = S3FeatureWriter(self._config)          # primary path: buffered Parquet -> Snowpipe
        self._user_baselines: Dict = {}  # user_id → {avg, stddev, surrogate_key}
        self._trusted_devices: Dict = {} # user_id → set of device_ids

    # ----------------------------------------------------------
    # SPARK SESSION
    # ----------------------------------------------------------
    def _build_spark_session(self) -> SparkSession:
        """
        Build Spark session with Kafka connector JAR.

        spark.jars.packages pulls the Kafka connector from Maven
        at startup. This is fine for dev but in production you'd
        bake the JARs into the Docker image (avoids Maven at runtime
        — critical for air-gapped environments).

        Memory config:
            driver: 2g — runs on your Mac, manages the job
            executor: 4g — runs in Docker worker, does the compute
            4g executor with 10M rows + shuffle = deliberate
            pressure to force spill-to-disk behavior
        """
        logger.info("Building Spark session...")
        spark = (
            SparkSession.builder
            .appName(self._config.SPARK_APP_NAME)
            .master(self._config.SPARK_MASTER)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            # Shuffle partitions — default 200 is too many for our
            # 12-partition input. Match to Kafka partition count
            # to avoid unnecessary shuffle overhead.
            .config("spark.sql.shuffle.partitions", "12")
            # Enable adaptive query execution — Spark 3.x feature
            # that automatically coalesces small partitions after
            # shuffle, reducing the straggler problem from skew
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        # Suppress KAFKA-1894 warning — harmless but extremely noisy
        # Fires once per partition per micro-batch — purely cosmetic.
        # The warning is from Spark's Kafka010 connector running in
        # a non-interruptible thread context, not a real error.
        log4j = spark.sparkContext._jvm.org.apache.log4j
        log4j.LogManager.getLogger(
            "org.apache.spark.sql.kafka010.KafkaDataConsumer"
        ).setLevel(log4j.Level.ERROR)
        log4j.LogManager.getLogger(
            "org.apache.kafka"
        ).setLevel(log4j.Level.ERROR)
        logger.info("Spark session ready")
        return spark

    # ----------------------------------------------------------
    # LOAD USER BASELINES (broadcast variable)
    # ----------------------------------------------------------
    def _load_user_baselines(self) -> None:
        """
        Load DIM_USERS baseline from Snowflake at startup.
        Stored as a broadcast variable — distributed to all
        executors once, avoiding repeated Snowflake queries
        per row during z-score computation.

        Why broadcast and not a join:
            A streaming-to-static join works but Spark re-reads
            the static side every micro-batch by default. A
            broadcast variable is loaded once at startup and
            cached on every executor — much faster for our
            10,000-user lookup table that changes rarely.

        Production note: refresh this daily via a background
        thread or by restarting the streaming job nightly when
        new baseline stats are computed from the previous day's
        transaction history.
        """
        logger.info("Loading user baselines from Snowflake...")
        conn = snowflake.connector.connect(
            account=self._config.SNOWFLAKE_ACCOUNT,
            user=self._config.SNOWFLAKE_USER,
            password=self._config.SNOWFLAKE_PASSWORD,
            database=self._config.SNOWFLAKE_DATABASE,
            warehouse=self._config.SNOWFLAKE_WAREHOUSE,
            role=self._config.SNOWFLAKE_ROLE,
            schema="DIM",
        )
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    user_id,
                    surrogate_key,
                    avg_transaction_amt,
                    stddev_transaction_amt,
                    home_latitude,
                    home_longitude,
                    home_city,
                    risk_tier
                FROM DIM.DIM_USERS
                WHERE is_current = TRUE
            """)
            rows = cursor.fetchall()
            cols = [d[0].lower() for d in cursor.description]

            for row in rows:
                u = dict(zip(cols, row))
                self._user_baselines[u["user_id"]] = {
                    "surrogate_key":          u["surrogate_key"],
                    "avg_transaction_amt":    float(u["avg_transaction_amt"] or 0),
                    "stddev_transaction_amt": float(u["stddev_transaction_amt"] or 1),
                    "home_latitude":          float(u["home_latitude"] or 0),
                    "home_longitude":         float(u["home_longitude"] or 0),
                    "home_city":              u["home_city"],
                    "risk_tier":              u["risk_tier"],
                }

            logger.info(f"Loaded {len(self._user_baselines)} user baselines")

            # Load trusted devices
            cursor.execute("""
                SELECT user_id, device_id
                FROM DIM.DIM_DEVICES
                WHERE is_trusted = TRUE
            """)
            for row in cursor.fetchall():
                user_id, device_id = row
                self._trusted_devices.setdefault(user_id, set()).add(device_id)

            logger.info(f"Loaded trusted devices for {len(self._trusted_devices)} users")
        finally:
            conn.close()

    # ----------------------------------------------------------
    # READ KAFKA STREAM
    # ----------------------------------------------------------
    def _read_kafka_stream(self) -> DataFrame:
        """
        Read raw messages from Kafka topic.
        Returns DataFrame with: key (bytes), value (bytes),
        partition, offset, timestamp (Kafka ingestion time).

        starting_offsets="earliest": process all backfill messages
        first, then continue with new ones. In production you might
        use "latest" to skip historical backlog and only process
        new events going forward.
        """
        return (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self._config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", self._config.KAFKA_TOPIC)
            .option("startingOffsets", self._config.KAFKA_STARTING_OFFSETS)
            .option("kafka.group.id", self._config.KAFKA_GROUP_ID)
            # Max messages per partition per micro-batch
            # 500K per partition × 12 partitions = 6M max per trigger
            # Keeps micro-batches from being too large on backfill
            .option("maxOffsetsPerTrigger", 50_000)
            # raised from 10K — safe now that both the Redis N+1 fix
            # and the S3/Snowpipe write path are in place; the driver
            # Python loop (collect + amount/geo/device math) is the
            # only remaining per-row cost, and it's fast enough at
            # this size to not reintroduce the multi-minute-batch
            # problem from Day 3
            .load()
        )

    # ----------------------------------------------------------
    # PARSE TRANSACTIONS
    # ----------------------------------------------------------
    def _parse_transactions(self, raw_df: DataFrame) -> DataFrame:
        """
        Deserialize JSON value from Kafka bytes → typed columns.

        Two important transformations:
        1. transaction_ts: string → timestamp for event-time windowing
           Spark needs a proper TimestampType column to apply
           watermarks and sliding windows — can't window on strings.
        2. Cast amount to double for z-score math (float precision
           issues in aggregations — double is safer).
        """
        return (
            raw_df
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    TRANSACTION_SCHEMA
                ).alias("data")
            )
            .select("data.*")
            .withColumn(
                "transaction_ts",
                F.to_timestamp(F.col("transaction_ts"))
            )
            .withColumn("amount", F.col("amount").cast("double"))
            # Drop malformed rows — nulls in critical fields
            .filter(
                F.col("transaction_id").isNotNull() &
                F.col("user_id").isNotNull() &
                F.col("transaction_ts").isNotNull()
            )
        )

    # ----------------------------------------------------------
    # VELOCITY FEATURES (sliding windows)
    # ----------------------------------------------------------
    def _compute_velocity(self, parsed_df: DataFrame) -> DataFrame:
        """
        Compute transaction velocity per user using sliding windows.

        Event time windowing with watermark:
            withWatermark("transaction_ts", "10 minutes") tells Spark
            to wait up to 10 minutes for late-arriving events before
            closing a window. After the watermark passes, the window
            result is finalized and output.

        Why sliding, not tumbling:
            We need "count in the LAST 15 minutes from THIS moment"
            not "count in the current 15-minute bucket." A tumbling
            window would split events at 00:14 and 00:16 into separate
            buckets even though they're 2 minutes apart — breaking
            the velocity signal entirely.

        Shuffle note:
            groupBy(user_id, window) triggers a full shuffle — every
            row gets redistributed by hash(user_id) to the same
            executor to be counted together. With 10,000 users and
            10M rows, this is the most expensive operation in the job.
            You'll see this in the Spark UI as the largest shuffle
            read/write stage. The skew we saw in Kafka (partitions
            2,3,9 having 2.4x more data) propagates here — the
            executors handling those user_ids will take longer.
        """
        watermarked = parsed_df.withWatermark(
            "transaction_ts",
            self._config.WATERMARK_DELAY
        )

        # Compute each velocity window separately then join
        # Alternative: use pivot — but multiple separate windows
        # are cleaner and easier to reason about individually
        velocity_dfs = []
        for window_duration, slide_duration, col_name in self._config.WINDOWS:
            vel_df = (
                watermarked
                .groupBy(
                    F.col("user_id"),
                    F.window(
                        F.col("transaction_ts"),
                        window_duration,
                        slide_duration
                    )
                )
                .agg(F.count("*").alias(col_name))
                .select(
                    F.col("user_id"),
                    F.col("window.end").alias("window_end"),
                    F.col(col_name)
                )
            )
            velocity_dfs.append((col_name, vel_df))

        return velocity_dfs

    # ----------------------------------------------------------
    # FOREACHBATCH SINK
    # ----------------------------------------------------------
    def _make_foreachbatch_sink(self):
        """
        foreachBatch gives us a regular (non-streaming) DataFrame
        for each micro-batch trigger. We use it to:
        1. Collect rows to driver (small batches — feature snapshots)
        2. Compute geo features using Redis state
        3. Write to Redis + Snowflake

        Why foreachBatch instead of native sinks:
            Spark has no native Redis sink.
            Spark has no native Snowflake streaming sink.
            foreachBatch is the standard pattern for custom sinks
            in Structured Streaming — gives you full flexibility.

        Important: foreachBatch runs on the DRIVER, not executors.
            Keep batch sizes manageable — don't collect 10M rows
            to the driver. We use maxOffsetsPerTrigger to bound
            micro-batch size to ~6M rows max, but in practice
            the parsed + filtered DataFrame will be much smaller.
        """
        redis_writer = self._redis_writer
        sf_writer = self._sf_writer
        s3_writer = self._s3_writer
        user_baselines = self._user_baselines
        trusted_devices = self._trusted_devices
        config = self._config

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                logger.info(f"Batch {batch_id}: empty, skipping")
                return

            count = batch_df.count()
            logger.info(f"Batch {batch_id}: processing {count:,} transactions")

            # Collect to driver for Redis geo-state lookup
            # In production with very high throughput, you'd do this
            # in a distributed UDF instead — but for our scale this
            # is clear and debuggable
            rows = batch_df.collect()

            # ---- Batch-fetch ALL user locations in ONE Redis round trip ----
            # Fixes the N+1 problem: previously this loop called
            # redis_writer.get_last_location(user_id) once PER ROW —
            # 10,000 transactions/batch = 10,000 separate network
            # round trips. MGET fetches every unique user's last
            # location in a single call, then the loop below does
            # a plain in-memory dict lookup (microseconds) instead
            # of a network call (milliseconds) for every row.
            batch_user_ids = [row["user_id"] for row in rows]

            # Timed explicitly so the N+1 fix is directly OBSERVABLE
            # in logs, not just assumed to have worked. Compare this
            # timing against len(rows) — if this were still one GET
            # per row, this duration would scale linearly with batch
            # size. With MGET, it should stay roughly flat regardless
            # of batch size (dominated by one network round trip,
            # not by row count).
            _redis_fetch_start = time.time()
            last_locations = redis_writer.get_last_locations_batch(batch_user_ids)
            _redis_fetch_ms = (time.time() - _redis_fetch_start) * 1000

            unique_users = len(set(batch_user_ids))
            logger.info(
                f"Redis geo-fetch: {unique_users:,} unique users "
                f"(from {len(rows):,} transactions) in {_redis_fetch_ms:.1f}ms "
                f"— 1 MGET call, not {unique_users:,} individual GETs"
            )

            # ---- Velocity merge: read from the CONCURRENT velocity query ----
            # The velocity streaming query (started alongside this one in
            # run()) writes user:{user_id}:velocity independently, on its
            # own trigger cadence. This fetch is how the main pipeline
            # picks that value up — same MGET pattern, one round trip
            # regardless of batch size. Values may lag slightly behind
            # this exact micro-batch (the two queries are not lockstep)
            # — that lag is an accepted tradeoff, not a bug: velocity
            # is a 15-minute signal, a few seconds of staleness doesn't
            # change the fraud call.
            velocities = redis_writer.get_velocity_batch(batch_user_ids)

            features_list = []
            sf_rows = []

            for row in rows:
                user_id = row["user_id"]
                baseline = user_baselines.get(user_id, {})

                # ---- Amount z-score ----
                avg_amt    = baseline.get("avg_transaction_amt", 50.0)
                stddev_amt = baseline.get("stddev_transaction_amt", 20.0)
                amount     = float(row["amount"] or 0)
                zscore     = (amount - avg_amt) / stddev_amt if stddev_amt > 0 else 0.0

                # ---- Geo features from Redis state (in-memory lookup now) ----
                last_loc = last_locations.get(user_id)
                geo_distance_km      = None
                time_since_last_min  = None
                prev_city            = None
                prev_ts              = None

                if last_loc and row["latitude"] and row["longitude"]:
                    try:
                        geo_distance_km = haversine_distance(
                            last_loc["lat"], last_loc["lon"],
                            float(row["latitude"]), float(row["longitude"])
                        )
                        prev_city = last_loc.get("city")
                        prev_ts   = last_loc.get("ts")

                        if prev_ts:
                            txn_ts = row["transaction_ts"]
                            if txn_ts and prev_ts:
                                try:
                                    prev_dt = datetime.fromisoformat(str(prev_ts).replace("Z",""))
                                    curr_dt = txn_ts if isinstance(txn_ts, datetime) else datetime.fromisoformat(str(txn_ts))
                                    time_since_last_min = (curr_dt - prev_dt).total_seconds() / 60
                                except Exception:
                                    pass

                        # ---- Ordering guard (GEO_FLAGGING_INVESTIGATION.md) ----
                        # A non-positive delta means this event arrived out of
                        # event-time order and the "previous" location is one
                        # the user visits in the FUTURE — the distance is
                        # meaningless, not merely imprecise. Null the derived
                        # features (prev_city/prev_ts stay, as provenance of
                        # what the state contained). Measured: 80% of the
                        # false geo flags rode on exactly this corruption.
                        if time_since_last_min is not None and time_since_last_min <= 0:
                            geo_distance_km = None
                            time_since_last_min = None
                    except Exception as e:
                        logger.warning(f"Geo computation failed for {user_id}: {e}")

                # ---- New device flag ----
                device_id  = row["device_id"]
                user_devices = trusted_devices.get(user_id, set())
                is_new_device = device_id not in user_devices if device_id else False

                # ---- Velocity (from the concurrent velocity query) ----
                velocity_15min = velocities.get(user_id, 0)

                # ---- Risk score (weighted heuristic) ----
                # Each signal normalized to 0-1 before weighting.
                # velocity_15min is no longer a placeholder — it's the
                # real count fetched from Redis above, written by the
                # velocity streaming query running concurrently with
                # this one. VELOCITY_MAX_NORMAL (5) matches the JP
                # Morgan-style threshold researched Day 2: >5 txns in
                # 15 min is already suspicious, so 5 is where this
                # signal saturates to 1.0, not some arbitrary max.
                v15 = min(velocity_15min / config.VELOCITY_MAX_NORMAL, 1.0)
                amt_signal = min(abs(zscore) / config.ZSCORE_MAX_NORMAL, 1.0)
                geo_signal = min((geo_distance_km or 0) / config.GEO_MAX_NORMAL_KM, 1.0)
                dev_signal = 1.0 if is_new_device else 0.0

                risk_score = (
                    config.RISK_WEIGHTS["velocity_15min"] * v15 +
                    config.RISK_WEIGHTS["amount_zscore"]  * amt_signal +
                    config.RISK_WEIGHTS["geo_distance"]   * geo_signal +
                    config.RISK_WEIGHTS["new_device"]     * dev_signal
                )
                risk_score = round(min(risk_score, 1.0), 4)

                # ---- Flag decision: weighted score OR hard-rule override ----
                # A same-weighted additive score can never flag a pure
                # single-signal fraud pattern (max contribution from
                # any one signal is its own weight, e.g. 0.25 for geo
                # alone — never enough to cross 0.60 on its own, no
                # matter how extreme). The hard rules below catch what
                # the weighted score structurally cannot: an individual
                # signal that is severe enough to be suspicious with
                # zero corroboration needed. See HARD_RULE_* comments
                # in FeatureEngineConfig for why new_device is excluded,
                # and GEO_FLAGGING_INVESTIGATION.md for why geo's rule
                # is speed-plus-jump-distance rather than the graded
                # geo_signal it originally reused.
                implied_speed_kmh = compute_implied_speed_kmh(
                    geo_distance_km, time_since_last_min
                )
                geo_hard_triggered = geo_hard_rule(geo_distance_km, implied_speed_kmh)
                hard_rule_triggered = (
                    geo_hard_triggered or
                    amt_signal >= config.HARD_RULE_AMOUNT_THRESHOLD or
                    v15        >= config.HARD_RULE_VELOCITY_THRESHOLD
                )
                is_flagged = (risk_score > config.RISK_FLAG_THRESHOLD) or hard_rule_triggered

                txn_ts_str = str(row["transaction_ts"]) if row["transaction_ts"] else None

                feature_dict = {
                    "snapshot_id":            str(uuid.uuid4()),
                    "transaction_id":         row["transaction_id"],
                    "user_id":                user_id,
                    "user_surrogate_key":     baseline.get("surrogate_key", ""),
                    "computed_at":            datetime.utcnow().isoformat(),
                    "transaction_ts":         txn_ts_str,
                    # velocity — 15min comes from the concurrent velocity
                    # query (fetched via get_velocity_batch above). The
                    # other three windows (5min, 1hr, 24hr) are defined
                    # in window_config.VELOCITY_WINDOWS but not yet
                    # implemented as running queries — 15min was built
                    # first because it has a sourced production threshold
                    # (JP Morgan-style, Day 2) to validate against.
                    "velocity_5min":          None,
                    "velocity_15min":         velocity_15min,
                    "velocity_1hr":           None,
                    "velocity_24hr":          None,
                    # amount
                    "txn_amount":             amount,
                    "user_avg_amount":        avg_amt,
                    "user_stddev_amount":     stddev_amt,
                    "amount_zscore":          round(zscore, 4),
                    # geo
                    "prev_transaction_city":  prev_city,
                    "prev_transaction_ts":    prev_ts,
                    "geo_distance_km":        round(geo_distance_km, 2) if geo_distance_km else None,
                    "time_since_last_txn_min":round(time_since_last_min, 2) if time_since_last_min else None,
                    # device
                    "is_new_device":          is_new_device,
                    "device_id":              device_id,
                    # transaction location (for next geo computation)
                    "city":                   row["city"],
                    "country":                row["country"],
                    "latitude":               float(row["latitude"]) if row["latitude"] else None,
                    "longitude":              float(row["longitude"]) if row["longitude"] else None,
                    # Poisoning guard (GEO_FLAGGING_INVESTIGATION.md):
                    # a location that itself tripped the geo hard rule
                    # must not become the user's new baseline —
                    # RedisFeatureWriter skips the :location update for
                    # suspect rows, so one fraud in London can't make
                    # every later home purchase look like a jump back.
                    # Measured: 99% of users with false far-distance
                    # flags had exactly this contamination.
                    "suspect_location":       geo_hard_triggered,
                    # risk
                    "risk_score_raw":         risk_score,
                    "is_flagged_for_review":  is_flagged,
                    # ground truth (for eval)
                    "is_synthetic_fraud":     row["is_synthetic_fraud"],
                    "fraud_pattern":          row["fraud_pattern"],
                }

                features_list.append(feature_dict)

                # Snowflake row — subset of fields matching schema
                sf_rows.append({
                    "snapshot_id":              feature_dict["snapshot_id"],
                    "transaction_id":           feature_dict["transaction_id"],
                    "user_id":                  feature_dict["user_id"],
                    "user_surrogate_key":        feature_dict["user_surrogate_key"],
                    "computed_at":              datetime.utcnow(),
                    "velocity_5min":            feature_dict["velocity_5min"],
                    "velocity_15min":           feature_dict["velocity_15min"],
                    "velocity_1hr":             feature_dict["velocity_1hr"],
                    "velocity_24hr":            feature_dict["velocity_24hr"],
                    "txn_amount":               feature_dict["txn_amount"],
                    "user_avg_amount":          feature_dict["user_avg_amount"],
                    "user_stddev_amount":       feature_dict["user_stddev_amount"],
                    "amount_zscore":            feature_dict["amount_zscore"],
                    "prev_transaction_city":    feature_dict["prev_transaction_city"],
                    "prev_transaction_ts":      feature_dict["prev_transaction_ts"],
                    "geo_distance_km":          feature_dict["geo_distance_km"],
                    "time_since_last_txn_min":  feature_dict["time_since_last_txn_min"],
                    "is_new_device":            feature_dict["is_new_device"],
                    "device_id":                feature_dict["device_id"],
                    "risk_score_raw":           feature_dict["risk_score_raw"],
                    "is_flagged_for_review":    feature_dict["is_flagged_for_review"],
                    # Ground truth — added Day 4. Without these two
                    # fields, FACT_FEATURE_SNAPSHOTS has no way to
                    # measure whether is_flagged_for_review is actually
                    # correct. This was present in feature_dict (Redis)
                    # the whole time but never carried through to the
                    # Snowflake row until this fix.
                    "is_synthetic_fraud":       feature_dict["is_synthetic_fraud"],
                    "fraud_pattern":            feature_dict["fraud_pattern"],
                })

            # Write to Redis (online store — agent reads here, always
            # immediate — every micro-batch, no buffering, since the
            # agent needs current state, not eventually-consistent state)
            redis_writer.write_batch(features_list)

            # Stage to S3 for Snowpipe auto-ingestion (offline audit trail).
            # NOT written to Snowflake directly anymore — see S3FeatureWriter
            # docstring for why row-by-row executemany was the bottleneck.
            # Rows are buffered across FLUSH_EVERY_N_BATCHES micro-batches
            # (or until FLUSH_MAX_ROWS is hit) before being written as one
            # Parquet file. Snowpipe (AUTO_INGEST via SQS) picks it up from
            # there — no code in this process talks to Snowflake for this
            # write path at all once the file lands in S3.
            # write_batch() here is the shared FeatureSink interface —
            # same method name Redis just used above, even though the
            # underlying behavior is completely different (buffer,
            # don't write yet). This is what polymorphism buys us:
            # the caller doesn't need to know each sink's own naming.
            s3_writer.write_batch(sf_rows)
            if s3_writer.should_flush():
                s3_writer.flush()

            flagged = sum(1 for f in features_list if f["is_flagged_for_review"])
            logger.info(
                f"Batch {batch_id} complete: "
                f"{len(features_list):,} features computed | "
                f"{flagged:,} flagged for review"
            )

        return process_batch

    # ----------------------------------------------------------
    # VELOCITY QUERY — merged into this process, Day 4
    # ----------------------------------------------------------
    # This is the "merge" of velocity_engine.py into the main
    # pipeline: not combining the window math into the SAME query
    # (Spark's execution model still forbids mixing Complete-mode
    # windowed aggregation with foreachBatch on a non-aggregated
    # stream — established Day 2), but running BOTH queries inside
    # ONE Python process, off ONE SparkSession, so a single
    # `python feature_engine.py` starts everything instead of
    # needing two separate terminals.
    #
    # The two queries communicate through Redis, not through Spark:
    # this query writes user:{user_id}:velocity independently on
    # its own trigger cadence; the main foreachBatch sink (above)
    # reads it back via get_velocity_batch(). Same pattern as
    # velocity_engine.py, just running alongside instead of apart.
    def _compute_velocity_window(self, parsed_df: DataFrame) -> DataFrame:
        """
        Sliding 15-minute window count per user. Identical logic to
        VelocityEngine._compute_velocity() in velocity_engine.py —
        duplicated here rather than imported because velocity_engine.py
        builds its own SparkSession internally; sharing the DataFrame
        transformation logic isn't worth the coupling for one method.
        The WINDOW DURATIONS themselves are NOT duplicated — both
        pull from window_config.py, so a threshold change updates
        both places at once.
        """
        return (
            parsed_df
            .withWatermark("transaction_ts", _WATERMARK_DELAY)
            .groupBy(
                F.col("user_id"),
                F.window(F.col("transaction_ts"), PRIMARY_WINDOW_DURATION, PRIMARY_SLIDE_DURATION)
            )
            .agg(F.count("*").alias("velocity_15min"))
            .select(
                F.col("user_id"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("velocity_15min"),
            )
        )

    def _make_velocity_sink(self):
        """
        foreachBatch sink for the velocity query. Writes ONLY to
        Redis (user:{user_id}:velocity) — no Snowflake, no S3.
        Velocity is a live signal for the agent, not an audit
        record in its own right; it's already captured inside the
        main feature snapshot (velocity_15min field) once the main
        pipeline reads it back.
        """
        redis_writer = self._redis_writer
        threshold = _VELOCITY_FLAG_THRESHOLD

        def process_velocity_batch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                logger.info(f"Velocity batch {batch_id}: no window updates")
                return

            rows = batch_df.collect()
            velocity_rows = [
                {
                    "user_id":       row["user_id"],
                    "velocity_15min":row["velocity_15min"],
                    "window_start":  str(row["window_start"]),
                    "window_end":    str(row["window_end"]),
                }
                for row in rows
            ]

            flagged_count = redis_writer.write_velocity_batch(velocity_rows, threshold)

            logger.info(
                f"Velocity batch {batch_id}: {len(rows):,} window updates | "
                f"{flagged_count:,} users over threshold (>{threshold} txns/15min)"
            )

        return process_velocity_batch

    # ----------------------------------------------------------
    # RUN
    # ----------------------------------------------------------
    def run(self) -> None:
        """
        Wire everything together and start BOTH streaming queries.

        Flow:
            1. Build Spark session (ONE session, shared by both queries)
            2. Load user baselines from Snowflake → in-memory dict
            3. Connect to Redis
            4. Read Kafka stream, parse once
            5. Start the MAIN query: amount/geo/device/risk →
               Redis (every batch) + S3/Snowpipe (buffered)
            6. Start the VELOCITY query: sliding window count →
               Redis only
            7. awaitAnyTermination() — wait on whichever query
               finishes or fails first, since normally neither
               ever finishes (both run forever)

        Why parsed_df is built ONCE and reused for both queries:
            Both queries read the same Kafka topic. Spark supports
            multiple independent streaming queries derived from the
            same source DataFrame — each gets its own consumer
            group and its own checkpoint, so they don't interfere
            with each other's offset tracking, but they don't
            duplicate the Kafka read/parse code either.
        """
        logger.info("=" * 60)
        logger.info("FRAUD FEATURE ENGINE STARTING (main + velocity, merged)")
        logger.info("=" * 60)

        # Step 1 — Spark (shared by both queries)
        self._spark = self._build_spark_session()

        # Step 2 — Load baselines
        self._load_user_baselines()

        # Step 3 — Redis
        self._redis_writer.connect()

        # Step 3b — test Snowflake before starting stream
        logger.info("Testing Snowflake connectivity...")
        self._sf_writer.test_connection()
        logger.info("Snowflake OK — starting streams")

        # Step 4 — Kafka → parse (once, shared)
        raw_df    = self._read_kafka_stream()
        parsed_df = self._parse_transactions(raw_df)

        # Step 5 — Main query
        main_query = (
            parsed_df.writeStream
            .foreachBatch(self._make_foreachbatch_sink())
            .option("checkpointLocation", self._config.CHECKPOINT_DIR)
            .trigger(processingTime=self._config.TRIGGER_INTERVAL)
            .start()
        )
        logger.info(f"Main query started. Checkpoint: {self._config.CHECKPOINT_DIR}")

        # Step 6 — Velocity query (independent, same Kafka source)
        velocity_df = self._compute_velocity_window(parsed_df)
        velocity_query = (
            velocity_df.writeStream
            .foreachBatch(self._make_velocity_sink())
            .outputMode("update")
            .option("checkpointLocation", self._config.VELOCITY_CHECKPOINT_DIR)
            .trigger(processingTime=self._config.TRIGGER_INTERVAL)
            .start()
        )
        logger.info(f"Velocity query started. Checkpoint: {self._config.VELOCITY_CHECKPOINT_DIR}")

        logger.info("Both queries running concurrently. (Ctrl+C to stop)")
        logger.info("Monitor at: http://localhost:4040 (Spark UI)")

        try:
            self._spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Stopping feature engine (both queries)...")
            main_query.stop()
            velocity_query.stop()

            # ---- Polymorphic shutdown — the real payoff of FeatureSink ----
            # Every sink gets flush() called on it the SAME way, through
            # the shared FeatureSink interface, regardless of what
            # flushing means for that particular sink. Redis and
            # Snowflake write immediately, so their flush() is the
            # inherited no-op — calling it costs nothing and is
            # harmless. S3 is the one sink with something real to do
            # here (its overridden flush() writes any buffered rows
            # that haven't hit FLUSH_EVERY_N_BATCHES yet). The caller
            # doesn't need an if/else per sink type to know that —
            # that's what the shared base class buys us.
            sinks: List[FeatureSink] = [
                self._redis_writer,
                self._sf_writer,
                self._s3_writer,
            ]
            # Flush first, close second — flushing a sink that's
            # already closed would fail, so the order matters even
            # though each individual call is polymorphic.
            for sink in sinks:
                result = sink.flush()
                if result:
                    logger.info(f"Final flush on shutdown ({type(sink).__name__}): {result}")
            for sink in sinks:
                sink.close()

            logger.info("Feature engine stopped.")


# =============================================================
# ENTRY POINT
# =============================================================
if __name__ == "__main__":
    engine = FraudFeatureEngine()
    engine.run()