# =============================================================
# VELOCITY ENGINE — Standalone Spark Structured Streaming query
# =============================================================
# Computes real-time transaction velocity per user using
# sliding windows, writes results to Redis.
#
# WHY THIS IS A SEPARATE QUERY FROM feature_engine.py:
#   Sliding window aggregations (groupBy + window()) require
#   Complete or Update output mode in Spark Structured Streaming.
#   foreachBatch — which feature_engine.py uses for Redis/S3
#   writes — only works cleanly with Append/Update mode on a
#   non-aggregated stream. Mixing a windowed aggregation with
#   foreachBatch in the SAME query hits Spark's execution model
#   restrictions. The clean solution: run velocity as its own
#   independent streaming query, reading the same Kafka topic,
#   writing to a DIFFERENT Redis key than feature_engine.py uses.
#
# Both queries can run concurrently against the same topic —
# Spark's consumer group handling means they don't conflict;
# each query tracks its own offsets independently.
#
# Output mode: Update — only emits rows whose window aggregate
# CHANGED since the last trigger, not the full result table
# every time. This keeps Redis writes proportional to actual
# activity, not to total user count.
# =============================================================

import os
import json
import logging
from typing import Optional

import redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType
)
from dotenv import load_dotenv

from window_config import (
    WATERMARK_DELAY as _WATERMARK_DELAY,
    PRIMARY_WINDOW_DURATION as _WINDOW_DURATION,
    PRIMARY_SLIDE_DURATION as _SLIDE_DURATION,
    VELOCITY_FLAG_THRESHOLD as _VELOCITY_FLAG_THRESHOLD,
    DEFAULT_TRIGGER_INTERVAL,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(), True),
    StructField("user_id",           StringType(), True),
    StructField("amount",            FloatType(),  True),
    StructField("transaction_ts",    StringType(), True),
    StructField("is_synthetic_fraud",BooleanType(),True),
    StructField("fraud_pattern",     StringType(), True),
])


class VelocityEngine:
    """
    Standalone streaming query: computes 15-minute sliding window
    transaction counts per user, writes to Redis under
    user:{user_id}:velocity — a DIFFERENT key than feature_engine.py
    uses for the full feature snapshot. feature_engine.py can read
    this key later to merge velocity into the complete picture,
    or the agent can read both keys independently.

    Deliberately scoped to ONE window (15 min) for this standalone
    proof — the 5min/1hr/24hr windows follow the identical pattern
    once this is verified working.
    """

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "fraud-transactions")
    CHECKPOINT_DIR          = "/tmp/fraud-velocity-checkpoint"
    # Window settings now come from window_config.py (shared with
    # feature_engine.py) instead of being duplicated here — see
    # that file's header comment for why this split exists.
    WATERMARK_DELAY         = _WATERMARK_DELAY
    WINDOW_DURATION         = _WINDOW_DURATION
    SLIDE_DURATION          = _SLIDE_DURATION
    VELOCITY_FLAG_THRESHOLD = _VELOCITY_FLAG_THRESHOLD  # >5 txns/15min = flag (JP Morgan-style, sourced Day 2)

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_TTL_SEC = 86400

    def __init__(self):
        self._spark: Optional[SparkSession] = None
        self._redis: Optional[redis.Redis] = None

    def _build_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("VelocityEngine")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "12")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        log4j = spark.sparkContext._jvm.org.apache.log4j
        log4j.LogManager.getLogger(
            "org.apache.spark.sql.kafka010.KafkaDataConsumer"
        ).setLevel(log4j.Level.ERROR)
        return spark

    def _connect_redis(self) -> None:
        self._redis = redis.Redis(
            host=self.REDIS_HOST, port=self.REDIS_PORT, decode_responses=True
        )
        self._redis.ping()
        logger.info(f"Redis connected at {self.REDIS_HOST}:{self.REDIS_PORT}")

    def _read_and_parse(self) -> DataFrame:
        raw_df = (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", self.KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("kafka.group.id", "spark-velocity-engine")
            .option("maxOffsetsPerTrigger", 10_000)
            .load()
        )
        return (
            raw_df
            .select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("transaction_ts", F.to_timestamp(F.col("transaction_ts")))
            .filter(F.col("user_id").isNotNull() & F.col("transaction_ts").isNotNull())
        )

    def _compute_velocity(self, parsed_df: DataFrame) -> DataFrame:
        """
        Sliding window count per user. Event-time based (transaction_ts),
        watermarked to bound how long we wait for late-arriving events —
        same reasoning as discussed on Day 2: fraud velocity is about
        when the user actually transacted, not when Spark read the event.
        """
        return (
            parsed_df
            .withWatermark("transaction_ts", self.WATERMARK_DELAY)
            .groupBy(
                F.col("user_id"),
                F.window(F.col("transaction_ts"), self.WINDOW_DURATION, self.SLIDE_DURATION)
            )
            .agg(F.count("*").alias("velocity_15min"))
            .select(
                F.col("user_id"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("velocity_15min"),
            )
        )

    def _make_sink(self):
        redis_client_getter = lambda: self._redis
        threshold = self.VELOCITY_FLAG_THRESHOLD

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                logger.info(f"Velocity batch {batch_id}: no window updates")
                return

            rows = batch_df.collect()
            r = redis_client_getter()

            pipe = r.pipeline()
            flagged_count = 0
            for row in rows:
                is_flagged = row["velocity_15min"] > threshold
                if is_flagged:
                    flagged_count += 1
                payload = {
                    "user_id": row["user_id"],
                    "velocity_15min": row["velocity_15min"],
                    "window_start": str(row["window_start"]),
                    "window_end": str(row["window_end"]),
                    "is_flagged": is_flagged,
                }
                pipe.setex(
                    f"user:{row['user_id']}:velocity",
                    self.REDIS_TTL_SEC,
                    json.dumps(payload, default=str)
                )
            pipe.execute()

            logger.info(
                f"Velocity batch {batch_id}: {len(rows):,} window updates | "
                f"{flagged_count:,} users over threshold (>{threshold} txns/15min)"
            )

        return process_batch

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("VELOCITY ENGINE — standalone sliding window query")
        logger.info("=" * 60)

        self._spark = self._build_spark_session()
        self._connect_redis()

        parsed_df = self._read_and_parse()
        velocity_df = self._compute_velocity(parsed_df)

        query = (
            velocity_df.writeStream
            .foreachBatch(self._make_sink())
            .outputMode("update")   # only emit CHANGED window aggregates
            .option("checkpointLocation", self.CHECKPOINT_DIR)
            .trigger(processingTime=DEFAULT_TRIGGER_INTERVAL)
            .start()
        )

        logger.info(f"Velocity query started. Output mode: update")
        logger.info(f"Writing to Redis key pattern: user:{{user_id}}:velocity")
        logger.info("Consuming from Kafka... (Ctrl+C to stop)")

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping velocity engine...")
            query.stop()
            logger.info("Velocity engine stopped.")


if __name__ == "__main__":
    VelocityEngine().run()