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
import uuid
import json
import math
import logging
from datetime import datetime
from typing import Dict, Optional

import redis
import snowflake.connector
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    BooleanType, TimestampType, IntegerType
)
from dotenv import load_dotenv

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
    TRIGGER_INTERVAL        = "30 seconds" # micro-batch frequency
    CHECKPOINT_DIR          = "/tmp/fraud-feature-checkpoint"

    # Sliding window definitions
    # event time based — transaction_ts, not processing time
    # Why: fraud velocity = what the user DID, not when Spark read it
    WINDOWS = [
        ("5 minutes",  "1 minute",  "velocity_5min"),
        ("15 minutes", "1 minute",  "velocity_15min"),
        ("1 hour",     "5 minutes", "velocity_1hr"),
        ("24 hours",   "30 minutes","velocity_24hr"),
    ]
    WATERMARK_DELAY = "10 minutes"  # wait up to 10 min for late events

    # Risk scoring weights (heuristic, pre-agent)
    # These are directionally correct but not ML-tuned
    RISK_WEIGHTS = {
        "velocity_15min":  0.30,   # high weight — most reliable fraud signal
        "amount_zscore":   0.25,   # strong signal for AMOUNT_ANOMALY
        "geo_distance":    0.25,   # strong signal for GEO_JUMP
        "new_device":      0.20,   # supporting signal, not standalone
    }
    RISK_FLAG_THRESHOLD = 0.60     # flag for agent review if score > 0.6

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


# =============================================================
# REDIS FEATURE WRITER
# =============================================================
class RedisFeatureWriter:
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
            # Location state for next geo computation
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
class SnowflakeFeatureWriter:
    """
    Writes feature snapshots to FEATURES.FACT_FEATURE_SNAPSHOTS.
    Called once per micro-batch via foreachBatch.

    Why foreachBatch instead of native Snowflake sink:
        Spark doesn't have a native Snowflake streaming sink.
        foreachBatch gives us a regular DataFrame per trigger
        that we can write using the Snowflake Spark connector
        or direct JDBC — we use direct connector for control.
    """

    def __init__(self, config: FeatureEngineConfig):
        self._config = config

    def _get_connection(self):
        return snowflake.connector.connect(
            account=self._config.SNOWFLAKE_ACCOUNT,
            user=self._config.SNOWFLAKE_USER,
            password=self._config.SNOWFLAKE_PASSWORD,
            database=self._config.SNOWFLAKE_DATABASE,
            warehouse=self._config.SNOWFLAKE_WAREHOUSE,
            role=self._config.SNOWFLAKE_ROLE,
            schema="FEATURES",
        )

    def test_connection(self) -> None:
        """
        Test Snowflake connectivity and table existence at startup.
        Fails fast with a clear error rather than silently failing
        on every micro-batch. Called once before stream starts.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM FEATURES.FACT_FEATURE_SNAPSHOTS")
            count = cursor.fetchone()[0]
            logger.info(f"Snowflake connection OK — FACT_FEATURE_SNAPSHOTS has {count} rows")
        finally:
            conn.close()

    def write_batch(self, features_list: list) -> None:
        """Batch insert feature snapshots into Snowflake."""
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
            # and is visible in logs — not silently swallowed
            logger.error(f"Snowflake write FAILED: {e}")
            raise
        finally:
            conn.close()


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
        self._sf_writer = SnowflakeFeatureWriter(self._config)
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
        # It fires once per partition per micro-batch because Spark's
        # Kafka consumer runs in a non-interruptible thread context.
        # Does not affect correctness — safe to suppress.
        log4j = spark.sparkContext._jvm.org.apache.log4j
        log4j.LogManager.getLogger("org.apache.kafka").setLevel(
            log4j.Level.ERROR
        )
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
            .option("maxOffsetsPerTrigger", 10_000)
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

                # ---- Geo features from Redis state ----
                last_loc = redis_writer.get_last_location(user_id)
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
                    except Exception as e:
                        logger.warning(f"Geo computation failed for {user_id}: {e}")

                # ---- New device flag ----
                device_id  = row["device_id"]
                user_devices = trusted_devices.get(user_id, set())
                is_new_device = device_id not in user_devices if device_id else False

                # ---- Risk score (weighted heuristic) ----
                # Each signal normalized to 0-1 before weighting
                v15 = 0.0  # velocity filled in when we join windows
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
                is_flagged = risk_score > config.RISK_FLAG_THRESHOLD

                txn_ts_str = str(row["transaction_ts"]) if row["transaction_ts"] else None

                feature_dict = {
                    "snapshot_id":            str(uuid.uuid4()),
                    "transaction_id":         row["transaction_id"],
                    "user_id":                user_id,
                    "user_surrogate_key":     baseline.get("surrogate_key", ""),
                    "computed_at":            datetime.utcnow().isoformat(),
                    "transaction_ts":         txn_ts_str,
                    # velocity — placeholder, filled after window join
                    "velocity_5min":          None,
                    "velocity_15min":         None,
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
                })

            # Write to Redis (online store — agent reads here)
            redis_writer.write_batch(features_list)

            # Write to Snowflake (offline store — audit trail)
            sf_writer.write_batch(sf_rows)

            flagged = sum(1 for f in features_list if f["is_flagged_for_review"])
            logger.info(
                f"Batch {batch_id} complete: "
                f"{len(features_list):,} features computed | "
                f"{flagged:,} flagged for review"
            )

        return process_batch

    # ----------------------------------------------------------
    # RUN
    # ----------------------------------------------------------
    def run(self) -> None:
        """
        Wire everything together and start the streaming job.

        Flow:
            1. Build Spark session
            2. Load user baselines from Snowflake → in-memory dict
            3. Connect to Redis
            4. Read Kafka stream
            5. Parse JSON → typed DataFrame
            6. Apply foreachBatch sink (geo + amount + device + risk)
            7. Start query, await termination

        Note on velocity:
            Velocity (sliding window) computation requires a separate
            streaming query because Complete output mode (needed for
            windows) can't be mixed with foreachBatch in the same query.
            Phase 1 implementation computes amount/geo/device features
            now and adds velocity as a second streaming query in the
            next step. This is intentional — understand one part fully
            before adding the next.
        """
        logger.info("=" * 60)
        logger.info("FRAUD FEATURE ENGINE STARTING")
        logger.info("=" * 60)

        # Step 1 — Spark
        self._spark = self._build_spark_session()

        # Step 2 — Load baselines
        self._load_user_baselines()

        # Step 3 — Redis
        self._redis_writer.connect()

        # Step 3b — test Snowflake before starting stream
        # Fail fast here with a clear error rather than silently
        # failing on every micro-batch with buried log messages
        logger.info("Testing Snowflake connectivity...")
        self._sf_writer.test_connection()
        logger.info("Snowflake OK — starting stream")

        # Step 4+5 — Kafka → parse
        raw_df    = self._read_kafka_stream()
        parsed_df = self._parse_transactions(raw_df)

        # Step 6 — Start streaming query
        query = (
            parsed_df.writeStream
            .foreachBatch(self._make_foreachbatch_sink())
            .option("checkpointLocation", self._config.CHECKPOINT_DIR)
            .trigger(processingTime=self._config.TRIGGER_INTERVAL)
            .start()
        )

        logger.info(f"Streaming query started. Trigger: {self._config.TRIGGER_INTERVAL}")
        logger.info(f"Checkpoint: {self._config.CHECKPOINT_DIR}")
        logger.info("Consuming from Kafka... (Ctrl+C to stop)")
        logger.info("Monitor at: http://localhost:4040 (Spark UI)")

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping feature engine...")
            query.stop()
            logger.info("Feature engine stopped.")


# =============================================================
# ENTRY POINT
# =============================================================
if __name__ == "__main__":
    engine = FraudFeatureEngine()
    engine.run()