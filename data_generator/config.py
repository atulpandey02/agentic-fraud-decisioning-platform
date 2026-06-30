# =============================================================
# FRAUD DETECTION PLATFORM — Generator Configuration
# =============================================================
# All thresholds sourced from production fraud systems:
# Stripe Radar, JP Morgan velocity rules, geo-velocity research
# Tune these without touching generator logic
# =============================================================

# -------------------------------------------------------------
# USER POOL
# Scaled up deliberately for Spark stress testing.
# 100 users capped any groupBy(user_id) shuffle to 100 distinct
# keys — not enough to show real distributed skew. 10,000 users
# gives a realistic key cardinality and a genuinely large
# shuffle when Spark computes per-user sliding window features.
# -------------------------------------------------------------
NUM_USERS = 10_000                 # synthetic user profiles to generate
NUM_DEVICES_PER_USER = (1, 3)      # each user has 1-3 known trusted devices

# -------------------------------------------------------------
# TRANSACTION STREAM
# -------------------------------------------------------------
TRANSACTIONS_PER_SECOND = 2        # normal flow rate
FRAUD_RATE = 0.15                  # 15% of transactions carry fraud signal

# -------------------------------------------------------------
# VELOCITY SPIKE (burst injection)
# Real threshold: >5 transactions in 15 min = flagged (JP Morgan)
# We fire 8-10 in 5 min = clearly above threshold, detectable
# -------------------------------------------------------------
BURST_INTERVAL_SECONDS = 300       # inject a burst every ~5 minutes
BURST_TRANSACTION_COUNT = (8, 10)  # transactions per burst
BURST_WINDOW_SECONDS = 90          # fire all burst txns within this window
VELOCITY_FLAG_THRESHOLD = 5        # >5 txns in 15 min = VELOCITY_SPIKE

# -------------------------------------------------------------
# GEO-JUMP (impossible travel)
# Real threshold: >900 km/h implied speed = impossible travel
# Source: commercial jet ~575 mph (925 km/h), 1000 km/h used
# in production to allow for VPN/geo-IP margin
# Haversine formula used to compute actual distance
# -------------------------------------------------------------
IMPOSSIBLE_TRAVEL_SPEED_KMH = 900  # flag if implied speed exceeds this
GEO_JUMP_MIN_DISTANCE_KM = 5000    # minimum jump distance for synthetic fraud
                                    # Boston → London = 5,265 km ✓
                                    # Boston → Mumbai = 12,540 km ✓

# Realistic geo-jump city pairs (source city, fraud city)
GEO_JUMP_PAIRS = [
    ("Boston", "London"),
    ("New York", "Tokyo"),
    ("Chicago", "Dubai"),
    ("Los Angeles", "Mumbai"),
    ("Miami", "Sydney"),
    ("Seattle", "Paris"),
    ("Austin", "Singapore"),
    ("Denver", "Shanghai"),
]

# -------------------------------------------------------------
# AMOUNT ANOMALY — two sub-patterns from production research
# Pattern 1 — Card testing: suspiciously round small amounts
#   Real cardholders almost never buy exactly $1.00
#   Coffee = $4.73, gas = $52.81 — real txns are messy
# Pattern 2 — Threshold evasion: just below round numbers
#   Fraudsters stay under $100, $500, $1000 to avoid flags
# -------------------------------------------------------------
CARD_TEST_AMOUNTS = [1.00, 5.00, 10.00, 0.50, 2.00]
THRESHOLD_EVASION_AMOUNTS = [99.99, 499.99, 999.99, 199.99, 299.99]

# Normal amount anomaly: amount > user_avg * this multiplier
AMOUNT_ANOMALY_MULTIPLIER = 5.0    # 5x user average = suspicious
AMOUNT_ZSCORE_THRESHOLD = 3.0      # z-score > 3 = flag

# -------------------------------------------------------------
# NEW DEVICE
# Real threshold: device not in user's last 10 sessions + high amt
# Source: production weighted risk scoring systems
# -------------------------------------------------------------
NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER = 3.0  # new device + 3x avg = flag

# -------------------------------------------------------------
# MERCHANT CATEGORIES (realistic distribution)
# Weighted toward common spend — grocery most frequent
# -------------------------------------------------------------
MERCHANT_CATEGORIES = {
    "GROCERY": 0.25,
    "RESTAURANT": 0.20,
    "GAS_STATION": 0.15,
    "RETAIL": 0.15,
    "ELECTRONICS": 0.08,
    "TRAVEL": 0.07,
    "ENTERTAINMENT": 0.05,
    "PHARMACY": 0.05,
}

# -------------------------------------------------------------
# USER PROFILE BASELINES
# Realistic spend ranges per risk tier
# LOW risk:    conservative spenders, predictable behavior
# MEDIUM risk: average consumers
# HIGH risk:   high spenders, more variance, travel frequently
# -------------------------------------------------------------
USER_SPEND_PROFILES = {
    "LOW":    {"avg": (15, 45),   "stddev": (5, 15),  "daily_txns": (1, 3)},
    "MEDIUM": {"avg": (40, 120),  "stddev": (15, 40), "daily_txns": (2, 6)},
    "HIGH":   {"avg": (100, 400), "stddev": (40, 120),"daily_txns": (3, 10)},
}

RISK_TIER_DISTRIBUTION = {
    "LOW": 0.50,     # 50% low risk users
    "MEDIUM": 0.35,  # 35% medium risk users
    "HIGH": 0.15,    # 15% high risk users
}

# -------------------------------------------------------------
# BACKFILL MODE
# Produces historical volume fast — no real-time pacing.
# Used to stress-test Spark: skew, shuffle, windowing under load.
# Timestamps spread across BACKFILL_DAYS_BACK, weighted toward
# recent days (real systems accumulate more recent activity).
#
# Sizing rationale (deliberate stress test, not a casual demo):
#   10M transactions x ~500 bytes raw = ~5GB in Kafka
#   With 10,000 users, groupBy(user_id) for sliding-window
#   features triggers a real shuffle — average 1,000 txns/user,
#   but skewed (some users see 5-10x more activity), which is
#   what actually forces Spark to spill shuffle data to disk
#   when worker memory is deliberately capped at 4GB.
#   500K rows at 100 users never came close to this — it fit
#   entirely in memory with room to spare, telling us nothing
#   about Spark's behavior under real pressure.
# -------------------------------------------------------------
BACKFILL_NUM_TRANSACTIONS = 10_000_000
BACKFILL_DAYS_BACK = 30
BACKFILL_KAFKA_BATCH_SIZE = 10_000  # flush every N messages
BACKFILL_NUM_BURSTS = 3_000         # velocity spikes scattered across the range
                                     # ~100/day average over 30 days, scaled
                                     # proportionally with the larger user pool

# Recency weighting — last 7 days get more weight than days 8-30
# Mirrors real transaction systems where recent activity dominates
BACKFILL_RECENT_WINDOW_DAYS = 7
BACKFILL_RECENT_WEIGHT = 0.5        # 50% of volume lands in last 7 days

# -------------------------------------------------------------
# KAFKA
# -------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_TRANSACTIONS = "fraud-transactions"
KAFKA_TOPIC_ALERTS = "fraud-alerts"

# -------------------------------------------------------------
# SNOWFLAKE
# -------------------------------------------------------------
SNOWFLAKE_DATABASE = "FRAUD_DETECTION"
SNOWFLAKE_DIM_SCHEMA = "DIM"
SNOWFLAKE_RAW_SCHEMA = "RAW"

# -------------------------------------------------------------
# CITIES WITH COORDINATES
# Used for home location assignment and geo-jump detection
# -------------------------------------------------------------
US_CITIES = [
    {"city": "Boston",       "country": "US", "lat": 42.3601, "lon": -71.0589},
    {"city": "New York",     "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "Chicago",      "country": "US", "lat": 41.8781, "lon": -87.6298},
    {"city": "Los Angeles",  "country": "US", "lat": 34.0522, "lon": -118.2437},
    {"city": "Miami",        "country": "US", "lat": 25.7617, "lon": -80.1918},
    {"city": "Seattle",      "country": "US", "lat": 47.6062, "lon": -122.3321},
    {"city": "Austin",       "country": "US", "lat": 30.2672, "lon": -97.7431},
    {"city": "Denver",       "country": "US", "lat": 39.7392, "lon": -104.9903},
    {"city": "Atlanta",      "country": "US", "lat": 33.7490, "lon": -84.3880},
    {"city": "San Francisco","country": "US", "lat": 37.7749, "lon": -122.4194},
]

INTERNATIONAL_CITIES = [
    {"city": "London",    "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tokyo",     "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"city": "Dubai",     "country": "AE", "lat": 25.2048, "lon": 55.2708},
    {"city": "Mumbai",    "country": "IN", "lat": 19.0760, "lon": 72.8777},
    {"city": "Sydney",    "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "Paris",     "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"city": "Singapore", "country": "SG", "lat": 1.3521,  "lon": 103.8198},
    {"city": "Shanghai",  "country": "CN", "lat": 31.2304, "lon": 121.4737},
]