# =============================================================
# TRANSACTION STREAM GENERATOR
# =============================================================
# Produces continuous transaction events to Kafka.
# Embeds realistic fraud patterns with production-grade thresholds.
#
# Three modes running concurrently:
#   Mode 1 — Normal flow: 2 txns/sec across all users
#   Mode 2 — Burst injection: every ~5 min, 8-10 txns for one
#             user in 90 seconds → VELOCITY_SPIKE
#   Mode 3 — Probabilistic fraud: 15% of normal txns carry
#             a fraud pattern (GEO_JUMP, NEW_DEVICE, AMOUNT_ANOMALY)
#
# Key design: fraud signal must be REAL, not just labeled.
#   A VELOCITY_SPIKE must actually produce burst events.
#   A GEO_JUMP must actually compute impossible travel speed.
#   The agent detects real signal — not synthetic labels.
# =============================================================

import uuid
import json
import math
import random
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

from faker import Faker
from confluent_kafka import Producer as KafkaProducer
from dotenv import load_dotenv

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRANSACTIONS,
    TRANSACTIONS_PER_SECOND,
    FRAUD_RATE,
    BURST_INTERVAL_SECONDS,
    BURST_TRANSACTION_COUNT,
    BURST_WINDOW_SECONDS,
    IMPOSSIBLE_TRAVEL_SPEED_KMH,
    GEO_JUMP_MIN_DISTANCE_KM,
    CARD_TEST_AMOUNTS,
    THRESHOLD_EVASION_AMOUNTS,
    AMOUNT_ANOMALY_MULTIPLIER,
    NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER,
    MERCHANT_CATEGORIES,
    US_CITIES,
    INTERNATIONAL_CITIES,
    BACKFILL_NUM_TRANSACTIONS,
    BACKFILL_DAYS_BACK,
    BACKFILL_KAFKA_BATCH_SIZE,
    BACKFILL_NUM_BURSTS,
    BACKFILL_RECENT_WINDOW_DAYS,
    BACKFILL_RECENT_WEIGHT,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


class GeoCalculator:
    """
    Haversine distance and travel speed calculations.

    Why Haversine:
        Earth is a sphere. Euclidean distance on lat/lon
        coordinates is wrong — it doesn't account for curvature.
        Haversine gives the shortest great-circle distance
        between two points on a sphere. This is what real
        fraud systems use for impossible travel detection.

    Production threshold: >900 km/h = impossible travel
        Commercial jets cruise at ~925 km/h.
        Anything faster = physically impossible on Earth.
    """

    EARTH_RADIUS_KM = 6371.0

    @staticmethod
    def haversine_distance(
        lat1: float, lon1: float,
        lat2: float, lon2: float
    ) -> float:
        """
        Calculate great-circle distance between two points in km.
        Used for both geo-jump detection and feature computation.
        """
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (math.sin(dlat / 2) ** 2
             + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        return GeoCalculator.EARTH_RADIUS_KM * c

    @staticmethod
    def implied_speed_kmh(
        distance_km: float, time_minutes: float
    ) -> float:
        """
        Implied travel speed given distance and time.
        If speed > IMPOSSIBLE_TRAVEL_SPEED_KMH → geo-jump fraud.
        """
        if time_minutes <= 0:
            return float("inf")
        return distance_km / (time_minutes / 60)

    @staticmethod
    def is_impossible_travel(
        distance_km: float, time_minutes: float
    ) -> bool:
        speed = GeoCalculator.implied_speed_kmh(distance_km, time_minutes)
        return speed > IMPOSSIBLE_TRAVEL_SPEED_KMH


class TransactionStreamGenerator:
    """
    Produces synthetic transaction events to Kafka.

    Usage:
        generator = TransactionStreamGenerator()
        generator.run()   # blocking — runs until interrupted

    Or produce a single transaction (for testing/API):
        txn = generator.generate_normal_transaction(user_profile)
        generator._produce(txn)
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        user_map_path: Optional[str] = None,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._producer: Optional[KafkaProducer] = None
        self._fake = Faker()
        self._geo = GeoCalculator()

        # Last transaction location per user — for geo-jump detection
        # user_id → {"city": str, "lat": float, "lon": float, "ts": datetime}
        self._last_transaction: Dict[str, Dict] = {}

        # Load user map from JSON cache
        self._user_map = self._load_user_map(user_map_path)
        self._user_ids = list(self._user_map.keys())

        # Burst control
        self._burst_lock = threading.Lock()
        self._last_burst_time = datetime.utcnow()

        # Stats
        self._total_produced = 0
        self._fraud_produced = 0

    # ----------------------------------------------------------
    # SETUP
    # ----------------------------------------------------------
    def _load_user_map(self, path: Optional[str]) -> Dict:
        """Load user profiles from local JSON cache."""
        if path is None:
            path = Path(__file__).parent / "user_map.json"

        if not Path(path).exists():
            raise FileNotFoundError(
                f"user_map.json not found at {path}. "
                f"Run user_profile_generator.py first."
            )

        with open(path, "r") as f:
            user_map = json.load(f)

        logger.info(f"Loaded {len(user_map)} user profiles from {path}")
        return user_map

    def _build_producer(self) -> KafkaProducer:
        """
        Build Kafka producer with real idempotence (confluent-kafka).

        confluent-kafka wraps librdkafka (the C client), which
        implements the full idempotent producer protocol — the
        same one Java's client uses. Setting enable.idempotence
        automatically configures the safe combination underneath:
            acks=all, max.in.flight<=5 with ordering preserved,
            and aggressive retries — all handled by librdkafka.

        Why this matters for fraud detection specifically:
            If the broker writes a message but the ack to the
            producer is lost (network blip), the producer retries.
            Without idempotence, the broker now has two copies of
            the same transaction — which would inflate the velocity
            count Spark computes, falsely flagging a user as
            VELOCITY_SPIKE just because of a retried write, not
            real behavior. The broker uses a per-producer sequence
            number to detect and silently drop the duplicate.

        Note: confluent-kafka's produce() is fire-and-forget unless
        you poll() — delivery callbacks fire only when poll() runs.
        We call poll(0) after every produce() to keep callbacks
        flowing without blocking the main loop.
        """
        return KafkaProducer({
            "bootstrap.servers": self._bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        })

    def connect(self) -> "TransactionStreamGenerator":
        """Connect to Kafka."""
        logger.info(f"Connecting to Kafka at {self._bootstrap_servers}...")
        self._producer = self._build_producer()
        logger.info("Kafka producer ready.")
        return self

    def close(self) -> None:
        """Flush all pending deliveries before shutdown."""
        if self._producer:
            self._producer.flush()
            logger.info("Kafka producer flushed and closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ----------------------------------------------------------
    # MERCHANT HELPERS
    # ----------------------------------------------------------
    def _pick_merchant_category(self) -> str:
        categories = list(MERCHANT_CATEGORIES.keys())
        weights = list(MERCHANT_CATEGORIES.values())
        return random.choices(categories, weights=weights, k=1)[0]

    def _generate_merchant_name(self, category: str) -> str:
        merchant_map = {
            "GROCERY":       ["Whole Foods", "Trader Joe's", "Stop & Shop", "Market Basket"],
            "RESTAURANT":    ["Chipotle", "Panera Bread", "Local Kitchen", "Subway"],
            "GAS_STATION":   ["Shell", "BP", "ExxonMobil", "Chevron"],
            "RETAIL":        ["Target", "Walmart", "Macy's", "H&M"],
            "ELECTRONICS":   ["Best Buy", "Apple Store", "Micro Center", "B&H Photo"],
            "TRAVEL":        ["Delta Airlines", "Marriott", "Airbnb", "United Airlines"],
            "ENTERTAINMENT": ["AMC Theatres", "Spotify", "Netflix", "Ticketmaster"],
            "PHARMACY":      ["CVS", "Walgreens", "Rite Aid", "Duane Reade"],
        }
        return random.choice(merchant_map.get(category, ["Unknown Merchant"]))

    # ----------------------------------------------------------
    # NORMAL TRANSACTION
    # ----------------------------------------------------------
    def generate_normal_transaction(self, user: Dict) -> Dict:
        """
        Generate a legitimate transaction for a user.

        Amount generation: Gaussian distribution around user's avg.
        Real transactions are messy — $4.73 not $5.00.
        The messiness itself is part of the signal:
        perfectly round amounts ($1.00, $5.00) are fraud indicators.
        """
        # Gaussian amount — clamp to positive, add cents noise
        raw_amount = random.gauss(
            user["avg_transaction_amt"],
            user["stddev_transaction_amt"]
        )
        amount = round(max(1.50, raw_amount), 2)

        # Location — 80% near home, 20% another US city
        if random.random() < 0.80:
            city_data = self._get_home_city_data(user)
        else:
            city_data = random.choice(US_CITIES)

        # Device — pick from trusted devices
        device_id = random.choice(user["trusted_devices"]) if user["trusted_devices"] else str(uuid.uuid4())

        category = self._pick_merchant_category()
        now = datetime.utcnow()

        txn = {
            "transaction_id":    str(uuid.uuid4()),
            "user_id":           user["user_id"],
            "device_id":         device_id,
            "amount":            amount,
            "currency":          "USD",
            "merchant_name":     self._generate_merchant_name(category),
            "merchant_category": category,
            "city":              city_data["city"],
            "country":           city_data["country"],
            "latitude":          round(city_data["lat"] + random.uniform(-0.01, 0.01), 6),
            "longitude":         round(city_data["lon"] + random.uniform(-0.01, 0.01), 6),
            "transaction_ts":    now.isoformat(),
            "ingested_at":       now.isoformat(),
            "is_synthetic_fraud": False,
            "fraud_pattern":     None,
        }

        self._update_last_transaction(user["user_id"], txn)
        return txn

    # ----------------------------------------------------------
    # FRAUD TRANSACTIONS
    # ----------------------------------------------------------
    def generate_velocity_spike(self, user: Dict) -> List[Dict]:
        """
        Inject a burst of transactions for one user.

        Why this works:
            8-10 transactions in 90 seconds → Spark's 15-min
            sliding window sees velocity > 5 → VELOCITY_SPIKE.
            Real threshold from JP Morgan: >5 txns in 15 min.
            We fire 8-10 to be clearly above threshold.

        All transactions in the burst share the same fraud_pattern
        so the eval suite can verify the agent caught ALL of them.
        """
        count = random.randint(*BURST_TRANSACTION_COUNT)
        transactions = []

        logger.info(
            f"BURST: Injecting {count} transactions for "
            f"user {user['user_id'][:8]}... over {BURST_WINDOW_SECONDS}s"
        )

        for i in range(count):
            txn = self.generate_normal_transaction(user)
            txn["is_synthetic_fraud"] = True
            txn["fraud_pattern"] = "VELOCITY_SPIKE"
            # Small random delay between burst transactions
            # Makes it look like rapid card-testing, not a script artifact
            txn["transaction_ts"] = (
                datetime.utcnow() + timedelta(seconds=i * random.uniform(3, 12))
            ).isoformat()
            transactions.append(txn)

        return transactions

    def generate_geo_jump(self, user: Dict) -> Optional[Dict]:
        """
        Generate a transaction from an impossible location.

        Impossible travel = distance / time > 900 km/h.
        We place the fraud transaction internationally —
        Boston → London (5,265 km) is physically impossible
        in any reasonable time window.

        Haversine formula computes actual great-circle distance.
        This is exactly what Spark's feature engine computes
        for geo_distance_km in FACT_FEATURE_SNAPSHOTS.
        """
        last = self._last_transaction.get(user["user_id"])
        if not last:
            return None

        # Pick an international city far from home
        home_lat = user["home_latitude"]
        home_lon = user["home_longitude"]

        # Find a city that's genuinely far (>GEO_JUMP_MIN_DISTANCE_KM)
        far_cities = [
            c for c in INTERNATIONAL_CITIES
            if self._geo.haversine_distance(
                home_lat, home_lon, c["lat"], c["lon"]
            ) >= GEO_JUMP_MIN_DISTANCE_KM
        ]

        if not far_cities:
            far_cities = INTERNATIONAL_CITIES

        fraud_city = random.choice(far_cities)

        # Verify it's actually impossible travel
        last_ts = datetime.fromisoformat(last["ts"])
        minutes_since_last = (datetime.utcnow() - last_ts).total_seconds() / 60
        distance = self._geo.haversine_distance(
            last["lat"], last["lon"],
            fraud_city["lat"], fraud_city["lon"]
        )

        if not self._geo.is_impossible_travel(distance, max(minutes_since_last, 1)):
            # Not impossible enough — use a very recent timestamp
            minutes_since_last = 30  # force 30 min gap for calculation

        category = self._pick_merchant_category()
        now = datetime.utcnow()

        txn = {
            "transaction_id":     str(uuid.uuid4()),
            "user_id":            user["user_id"],
            "device_id":          random.choice(user["trusted_devices"]) if user["trusted_devices"] else str(uuid.uuid4()),
            "amount":             round(random.gauss(user["avg_transaction_amt"], user["stddev_transaction_amt"]), 2),
            "currency":           "USD",
            "merchant_name":      self._generate_merchant_name(category),
            "merchant_category":  category,
            "city":               fraud_city["city"],
            "country":            fraud_city["country"],
            "latitude":           round(fraud_city["lat"] + random.uniform(-0.01, 0.01), 6),
            "longitude":          round(fraud_city["lon"] + random.uniform(-0.01, 0.01), 6),
            "transaction_ts":     now.isoformat(),
            "ingested_at":        now.isoformat(),
            "is_synthetic_fraud": True,
            "fraud_pattern":      "GEO_JUMP",
        }

        logger.info(
            f"GEO_JUMP: user {user['user_id'][:8]}... | "
            f"{last['city']} → {fraud_city['city']} | "
            f"{distance:.0f} km in {minutes_since_last:.0f} min"
        )
        return txn

    def generate_new_device_fraud(self, user: Dict) -> Dict:
        """
        Transaction from an unknown device with high amount.

        Real threshold: new device alone = +0.3 risk weight.
        New device + amount > 3x avg = strong combined signal.
        Source: production weighted risk scoring systems.

        The new device_id is NOT in user's trusted_devices list.
        Spark's feature engine will flag is_new_device = True.
        """
        # Completely new device — not in trusted_devices
        new_device_id = str(uuid.uuid4())

        # High amount — 3-8x user average
        multiplier = random.uniform(
            NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER,
            NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER * 2.5
        )
        amount = round(user["avg_transaction_amt"] * multiplier, 2)

        category = "ELECTRONICS"  # common for high-value new-device fraud
        now = datetime.utcnow()
        city_data = self._get_home_city_data(user)

        txn = {
            "transaction_id":     str(uuid.uuid4()),
            "user_id":            user["user_id"],
            "device_id":          new_device_id,  # ← unknown device
            "amount":             amount,
            "currency":           "USD",
            "merchant_name":      self._generate_merchant_name(category),
            "merchant_category":  category,
            "city":               city_data["city"],
            "country":            city_data["country"],
            "latitude":           round(city_data["lat"] + random.uniform(-0.01, 0.01), 6),
            "longitude":          round(city_data["lon"] + random.uniform(-0.01, 0.01), 6),
            "transaction_ts":     now.isoformat(),
            "ingested_at":        now.isoformat(),
            "is_synthetic_fraud": True,
            "fraud_pattern":      "NEW_DEVICE",
        }

        logger.info(
            f"NEW_DEVICE: user {user['user_id'][:8]}... | "
            f"amount ${amount:.2f} ({multiplier:.1f}x avg) | "
            f"device {new_device_id[:8]}..."
        )
        return txn

    def generate_amount_anomaly(self, user: Dict) -> Dict:
        """
        Two sub-patterns from production fraud research:

        Pattern 1 — Card testing:
            Round amounts ($1.00, $5.00, $10.00).
            Fraudsters test stolen cards with micro-transactions.
            Real signal: coffee costs $4.73, not $5.00.

        Pattern 2 — Threshold evasion:
            Amounts just below round thresholds ($99.99, $499.99).
            Fraudsters know fraud teams flag round numbers.
            They stay just under $100 or $500 to avoid detection.

        Pattern 3 — Z-score anomaly:
            Amount far above user's historical average.
            z-score > 3 = flagged (3 standard deviations out).
        """
        pattern_choice = random.choice(["card_test", "threshold_evasion", "zscore"])

        if pattern_choice == "card_test":
            amount = random.choice(CARD_TEST_AMOUNTS)
        elif pattern_choice == "threshold_evasion":
            amount = random.choice(THRESHOLD_EVASION_AMOUNTS)
        else:
            # Z-score anomaly: 5-15x user average
            multiplier = random.uniform(
                AMOUNT_ANOMALY_MULTIPLIER,
                AMOUNT_ANOMALY_MULTIPLIER * 3
            )
            amount = round(user["avg_transaction_amt"] * multiplier, 2)

        category = self._pick_merchant_category()
        now = datetime.utcnow()
        city_data = self._get_home_city_data(user)

        txn = {
            "transaction_id":     str(uuid.uuid4()),
            "user_id":            user["user_id"],
            "device_id":          random.choice(user["trusted_devices"]) if user["trusted_devices"] else str(uuid.uuid4()),
            "amount":             amount,
            "currency":           "USD",
            "merchant_name":      self._generate_merchant_name(category),
            "merchant_category":  category,
            "city":               city_data["city"],
            "country":            city_data["country"],
            "latitude":           round(city_data["lat"] + random.uniform(-0.01, 0.01), 6),
            "longitude":          round(city_data["lon"] + random.uniform(-0.01, 0.01), 6),
            "transaction_ts":     now.isoformat(),
            "ingested_at":        now.isoformat(),
            "is_synthetic_fraud": True,
            "fraud_pattern":      "AMOUNT_ANOMALY",
        }

        logger.info(
            f"AMOUNT_ANOMALY ({pattern_choice}): "
            f"user {user['user_id'][:8]}... | "
            f"amount ${amount:.2f} | avg ${user['avg_transaction_amt']:.2f}"
        )
        return txn

    # ----------------------------------------------------------
    # PRODUCE TO KAFKA
    # ----------------------------------------------------------
    def _delivery_callback(self, err, msg) -> None:
        """
        confluent-kafka fires this after poll()/flush() processes
        a delivery report. Unlike kafka-python's send() which
        returns a future, confluent-kafka is callback-based —
        produce() only queues the message, it doesn't confirm
        delivery until poll() runs the event loop.
        """
        if err is not None:
            logger.error(f"Delivery failed for key={msg.key()}: {err}")

    def _produce(self, transaction: Dict) -> None:
        """
        Send transaction to Kafka.
        Key = user_id → ensures all events for same user
        land in the same partition → correct velocity computation.

        confluent-kafka requires manual serialization (no built-in
        key_serializer/value_serializer like kafka-python) — both
        key and value must be bytes before calling produce().

        poll(0) is called after every produce() to let librdkafka's
        internal event loop process delivery reports and trigger
        the callback without blocking the main generation loop.
        """
        if not self._producer:
            raise RuntimeError("Not connected. Call connect() first.")

        self._producer.produce(
            topic=KAFKA_TOPIC_TRANSACTIONS,
            key=transaction["user_id"].encode("utf-8"),
            value=json.dumps(transaction, default=str).encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

        self._total_produced += 1
        if transaction["is_synthetic_fraud"]:
            self._fraud_produced += 1

    def _produce_batch(self, transactions: List[Dict]) -> None:
        for txn in transactions:
            self._produce(txn)
        self._producer.flush()

    # ----------------------------------------------------------
    # FRAUD PATTERN SELECTOR
    # ----------------------------------------------------------
    def _generate_fraud_transaction(self, user: Dict) -> Optional[List[Dict]]:
        """
        Select a fraud pattern and generate transaction(s).
        Returns a list because VELOCITY_SPIKE produces multiple.
        """
        pattern = random.choices(
            ["VELOCITY_SPIKE", "GEO_JUMP", "NEW_DEVICE", "AMOUNT_ANOMALY"],
            weights=[0.25, 0.25, 0.25, 0.25],
            k=1
        )[0]

        if pattern == "VELOCITY_SPIKE":
            return self.generate_velocity_spike(user)
        elif pattern == "GEO_JUMP":
            txn = self.generate_geo_jump(user)
            return [txn] if txn else None
        elif pattern == "NEW_DEVICE":
            return [self.generate_new_device_fraud(user)]
        else:
            return [self.generate_amount_anomaly(user)]

    # ----------------------------------------------------------
    # BURST INJECTION (background thread)
    # ----------------------------------------------------------
    def _burst_injector(self) -> None:
        """
        Background thread: injects velocity spike bursts
        every BURST_INTERVAL_SECONDS (~5 min).

        Separate thread so bursts don't block the normal flow.
        """
        while True:
            time.sleep(BURST_INTERVAL_SECONDS)
            user_id = random.choice(self._user_ids)
            user = self._user_map[user_id]
            burst_txns = self.generate_velocity_spike(user)

            # Space burst transactions over BURST_WINDOW_SECONDS
            delay = BURST_WINDOW_SECONDS / len(burst_txns)
            for txn in burst_txns:
                self._produce(txn)
                time.sleep(delay)

            self._producer.flush()
            logger.info(
                f"Burst complete: {len(burst_txns)} txns for "
                f"user {user_id[:8]}..."
            )

    # ----------------------------------------------------------
    # BACKFILL MODE — bulk historical data for Spark stress testing
    # ----------------------------------------------------------
    def _random_backfill_timestamp(self, days_back: int) -> datetime:
        """
        Generate a random timestamp within the backfill range,
        weighted toward recent days.

        Why weighted, not uniform:
            Real transaction systems accumulate more recent
            activity — a user active today has more recent
            transactions than from 25 days ago. Uniform random
            would make every day look identical, which is not
            how real data behaves and would make CLUSTER BY
            pruning tests less realistic.

        Mechanics:
            BACKFILL_RECENT_WEIGHT (50%) of all transactions land
            in the most recent BACKFILL_RECENT_WINDOW_DAYS (7 days).
            The remaining 50% spread across the older days.
        """
        if random.random() < BACKFILL_RECENT_WEIGHT:
            days_offset = random.uniform(0, BACKFILL_RECENT_WINDOW_DAYS)
        else:
            days_offset = random.uniform(
                BACKFILL_RECENT_WINDOW_DAYS, days_back
            )

        seconds_offset = random.uniform(0, 86400)
        return (
            datetime.utcnow()
            - timedelta(days=days_offset)
            - timedelta(seconds=seconds_offset)
        )

    def _generate_normal_transaction_at(
        self, user: Dict, timestamp: datetime
    ) -> Dict:
        """
        Same logic as generate_normal_transaction() but with an
        explicit historical timestamp instead of datetime.utcnow().
        Used exclusively by backfill mode.
        """
        raw_amount = random.gauss(
            user["avg_transaction_amt"],
            user["stddev_transaction_amt"]
        )
        amount = round(max(1.50, raw_amount), 2)

        if random.random() < 0.80:
            city_data = self._get_home_city_data(user)
        else:
            city_data = random.choice(US_CITIES)

        device_id = (
            random.choice(user["trusted_devices"])
            if user["trusted_devices"] else str(uuid.uuid4())
        )
        category = self._pick_merchant_category()

        return {
            "transaction_id":     str(uuid.uuid4()),
            "user_id":            user["user_id"],
            "device_id":          device_id,
            "amount":             amount,
            "currency":           "USD",
            "merchant_name":      self._generate_merchant_name(category),
            "merchant_category":  category,
            "city":               city_data["city"],
            "country":            city_data["country"],
            "latitude":           round(city_data["lat"] + random.uniform(-0.01, 0.01), 6),
            "longitude":          round(city_data["lon"] + random.uniform(-0.01, 0.01), 6),
            "transaction_ts":     timestamp.isoformat(),
            "ingested_at":        datetime.utcnow().isoformat(),
            "is_synthetic_fraud": False,
            "fraud_pattern":      None,
        }

    def _generate_backfill_fraud_at(
        self, user: Dict, timestamp: datetime
    ) -> Optional[List[Dict]]:
        """
        Probabilistic fraud (GEO_JUMP, NEW_DEVICE, AMOUNT_ANOMALY)
        anchored at a historical timestamp. VELOCITY_SPIKE is
        handled separately via _generate_backfill_burst_at()
        because bursts need multiple closely-spaced timestamps,
        not a single point in time.
        """
        pattern = random.choices(
            ["GEO_JUMP", "NEW_DEVICE", "AMOUNT_ANOMALY"],
            weights=[1, 1, 1],
            k=1
        )[0]

        base_txn = self._generate_normal_transaction_at(user, timestamp)

        if pattern == "NEW_DEVICE":
            multiplier = random.uniform(
                NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER,
                NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER * 2.5
            )
            base_txn["device_id"] = str(uuid.uuid4())
            base_txn["amount"] = round(user["avg_transaction_amt"] * multiplier, 2)
            base_txn["merchant_category"] = "ELECTRONICS"
            base_txn["is_synthetic_fraud"] = True
            base_txn["fraud_pattern"] = "NEW_DEVICE"
            return [base_txn]

        elif pattern == "AMOUNT_ANOMALY":
            sub_pattern = random.choice(["card_test", "threshold_evasion", "zscore"])
            if sub_pattern == "card_test":
                base_txn["amount"] = random.choice(CARD_TEST_AMOUNTS)
            elif sub_pattern == "threshold_evasion":
                base_txn["amount"] = random.choice(THRESHOLD_EVASION_AMOUNTS)
            else:
                multiplier = random.uniform(
                    AMOUNT_ANOMALY_MULTIPLIER, AMOUNT_ANOMALY_MULTIPLIER * 3
                )
                base_txn["amount"] = round(user["avg_transaction_amt"] * multiplier, 2)
            base_txn["is_synthetic_fraud"] = True
            base_txn["fraud_pattern"] = "AMOUNT_ANOMALY"
            return [base_txn]

        else:  # GEO_JUMP
            home_lat, home_lon = user["home_latitude"], user["home_longitude"]
            far_cities = [
                c for c in INTERNATIONAL_CITIES
                if self._geo.haversine_distance(home_lat, home_lon, c["lat"], c["lon"])
                >= GEO_JUMP_MIN_DISTANCE_KM
            ] or INTERNATIONAL_CITIES
            fraud_city = random.choice(far_cities)

            base_txn["city"] = fraud_city["city"]
            base_txn["country"] = fraud_city["country"]
            base_txn["latitude"] = round(fraud_city["lat"] + random.uniform(-0.01, 0.01), 6)
            base_txn["longitude"] = round(fraud_city["lon"] + random.uniform(-0.01, 0.01), 6)
            base_txn["is_synthetic_fraud"] = True
            base_txn["fraud_pattern"] = "GEO_JUMP"
            return [base_txn]

    def _generate_backfill_burst_at(
        self, user: Dict, anchor_time: datetime
    ) -> List[Dict]:
        """
        VELOCITY_SPIKE anchored at a historical point in time.
        8-10 transactions within a 90-second window starting
        at anchor_time — same detectability logic as real-time
        bursts, just placed in the past instead of "now".
        """
        count = random.randint(*BURST_TRANSACTION_COUNT)
        transactions = []

        for i in range(count):
            txn_time = anchor_time + timedelta(seconds=i * random.uniform(3, 12))
            txn = self._generate_normal_transaction_at(user, txn_time)
            txn["is_synthetic_fraud"] = True
            txn["fraud_pattern"] = "VELOCITY_SPIKE"
            transactions.append(txn)

        return transactions

    def run_backfill(
        self,
        num_transactions: int = BACKFILL_NUM_TRANSACTIONS,
        days_back: int = BACKFILL_DAYS_BACK,
        num_bursts: int = BACKFILL_NUM_BURSTS,
    ) -> None:
        """
        Produce historical transaction volume as fast as Kafka
        will accept it — no real-time pacing, no time.sleep().

        Purpose: generate enough volume to actually stress Spark —
        observe partition skew, shuffle cost, windowing performance
        under load. 500K-1M events is the target range; a few
        thousand events tells you nothing about how Spark behaves
        at scale.

        Composition:
            - num_bursts VELOCITY_SPIKE bursts scattered across
              the date range (~8-10 txns each)
            - FRAUD_RATE of remaining volume: GEO_JUMP, NEW_DEVICE,
              AMOUNT_ANOMALY (single transactions)
            - Remainder: normal transactions

        Timestamps are historical (spread over days_back), but
        Kafka ingestion happens NOW, as fast as possible. This is
        intentional — we're backfilling history, not replaying it
        in real time.
        """
        logger.info("=" * 60)
        logger.info("BACKFILL MODE")
        logger.info(f"  Target transactions: {num_transactions:,}")
        logger.info(f"  Date range:          last {days_back} days")
        logger.info(f"  Velocity bursts:     {num_bursts}")
        logger.info("=" * 60)

        start_time = time.time()
        produced = 0
        fraud_count = 0
        batch: List[Dict] = []

        # ---- Step 1: generate burst events first ----
        # Bursts get priority slots in the transaction budget so
        # they're guaranteed to exist regardless of probabilistic
        # fraud draws later.
        burst_budget = 0
        for _ in range(num_bursts):
            user_id = random.choice(self._user_ids)
            user = self._user_map[user_id]
            anchor_time = self._random_backfill_timestamp(days_back)
            burst_txns = self._generate_backfill_burst_at(user, anchor_time)

            for txn in burst_txns:
                batch.append(txn)
                burst_budget += 1
                fraud_count += 1

                if len(batch) >= BACKFILL_KAFKA_BATCH_SIZE:
                    self._flush_backfill_batch(batch)
                    produced += len(batch)
                    self._log_backfill_progress(produced, num_transactions, fraud_count, start_time)
                    batch = []

        logger.info(f"Burst injection complete: {burst_budget:,} VELOCITY_SPIKE transactions")

        # ---- Step 2: fill remaining budget with normal + probabilistic fraud ----
        remaining = num_transactions - burst_budget

        for _ in range(remaining):
            user_id = random.choice(self._user_ids)
            user = self._user_map[user_id]
            ts = self._random_backfill_timestamp(days_back)

            if random.random() < FRAUD_RATE:
                txns = self._generate_backfill_fraud_at(user, ts)
                if txns:
                    batch.extend(txns)
                    fraud_count += len(txns)
            else:
                batch.append(self._generate_normal_transaction_at(user, ts))

            if len(batch) >= BACKFILL_KAFKA_BATCH_SIZE:
                self._flush_backfill_batch(batch)
                produced += len(batch)
                self._log_backfill_progress(produced, num_transactions, fraud_count, start_time)
                batch = []

        # Final flush for any remaining partial batch
        if batch:
            self._flush_backfill_batch(batch)
            produced += len(batch)

        elapsed = time.time() - start_time
        throughput = produced / elapsed if elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info(f"  Total produced:   {produced:,}")
        logger.info(f"  Fraud produced:   {fraud_count:,} ({fraud_count/produced*100:.1f}%)")
        logger.info(f"  Elapsed time:     {elapsed:.1f}s")
        logger.info(f"  Throughput:       {throughput:.0f} txns/sec")
        logger.info("=" * 60)

    def _flush_backfill_batch(self, batch: List[Dict]) -> None:
        """Produce a batch to Kafka and flush — no per-message delay."""
        for txn in batch:
            self._produce(txn)
        self._producer.flush()

    def _log_backfill_progress(
        self, produced: int, target: int, fraud_count: int, start_time: float
    ) -> None:
        elapsed = time.time() - start_time
        rate = produced / elapsed if elapsed > 0 else 0
        pct = (produced / target) * 100
        logger.info(
            f"Progress: {produced:,}/{target:,} ({pct:.1f}%) | "
            f"fraud: {fraud_count:,} | "
            f"rate: {rate:.0f} txns/sec | "
            f"elapsed: {elapsed:.0f}s"
        )

    # ----------------------------------------------------------
    # MAIN LOOP
    # ----------------------------------------------------------
    def run(self) -> None:
        """
        Main production loop.
        Runs until interrupted (Ctrl+C).

        Flow:
          - Start burst injector in background thread
          - Main thread: produce normal + probabilistic fraud
          - Log stats every 100 transactions
        """
        logger.info("Starting transaction stream generator...")
        logger.info(f"  Users loaded:    {len(self._user_ids)}")
        logger.info(f"  Rate:            {TRANSACTIONS_PER_SECOND} txn/sec")
        logger.info(f"  Fraud rate:      {FRAUD_RATE * 100:.0f}%")
        logger.info(f"  Burst interval:  {BURST_INTERVAL_SECONDS}s")

        # Start burst injector thread
        burst_thread = threading.Thread(
            target=self._burst_injector,
            daemon=True,
            name="burst-injector"
        )
        burst_thread.start()

        sleep_interval = 1.0 / TRANSACTIONS_PER_SECOND

        try:
            while True:
                user_id = random.choice(self._user_ids)
                user = self._user_map[user_id]

                if random.random() < FRAUD_RATE:
                    txns = self._generate_fraud_transaction(user)
                    if txns:
                        self._produce_batch(txns)
                else:
                    txn = self.generate_normal_transaction(user)
                    self._produce(txn)

                # Log stats every 100 transactions
                if self._total_produced % 100 == 0 and self._total_produced > 0:
                    fraud_pct = (self._fraud_produced / self._total_produced) * 100
                    logger.info(
                        f"Stats: {self._total_produced} produced | "
                        f"{self._fraud_produced} fraud ({fraud_pct:.1f}%)"
                    )

                time.sleep(sleep_interval)

        except KeyboardInterrupt:
            logger.info(
                f"\nStopped. Total: {self._total_produced} | "
                f"Fraud: {self._fraud_produced}"
            )

    # ----------------------------------------------------------
    # HELPERS
    # ----------------------------------------------------------
    def _get_home_city_data(self, user: Dict) -> Dict:
        """Get city data matching user's home city."""
        for city in US_CITIES:
            if city["city"] == user["home_city"]:
                return city
        return random.choice(US_CITIES)

    def _update_last_transaction(self, user_id: str, txn: Dict) -> None:
        """Track last known location per user for geo-jump detection."""
        self._last_transaction[user_id] = {
            "city": txn["city"],
            "lat":  txn["latitude"],
            "lon":  txn["longitude"],
            "ts":   txn["transaction_ts"],
        }


# -------------------------------------------------------------
# ENTRY POINT
# Usage:
#   python transaction_stream_generator.py              → real-time mode
#   python transaction_stream_generator.py --backfill    → backfill mode
#   python transaction_stream_generator.py --backfill --num 1000000
# -------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fraud transaction generator")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Run in backfill mode — fast historical bulk load"
    )
    parser.add_argument(
        "--num", type=int, default=BACKFILL_NUM_TRANSACTIONS,
        help="Number of transactions for backfill mode"
    )
    parser.add_argument(
        "--days", type=int, default=BACKFILL_DAYS_BACK,
        help="Days of history to spread backfill across"
    )
    parser.add_argument(
        "--bursts", type=int, default=None,
        help="Number of VELOCITY_SPIKE bursts. Default scales with --num so a "
             "small backfill isn't swamped by bursts (which would otherwise "
             "produce nearly all-fraud data)."
    )
    args = parser.parse_args()

    # Scale bursts to --num by default. BACKFILL_NUM_BURSTS is tuned for the
    # full BACKFILL_NUM_TRANSACTIONS run; using it verbatim on a small --num
    # lets bursts (~9 txns each) consume the entire budget and crowd out every
    # normal transaction — a 25K backfill with the default 3000 bursts came out
    # 100% fraud. Proportional scaling keeps the burst fraction constant, and
    # yields exactly BACKFILL_NUM_BURSTS at the full default --num.
    if args.bursts is not None:
        num_bursts = args.bursts
    else:
        num_bursts = max(1, round(args.num * BACKFILL_NUM_BURSTS / BACKFILL_NUM_TRANSACTIONS))

    with TransactionStreamGenerator() as generator:
        if args.backfill:
            generator.run_backfill(
                num_transactions=args.num,
                days_back=args.days,
                num_bursts=num_bursts,
            )
        else:
            generator.run()