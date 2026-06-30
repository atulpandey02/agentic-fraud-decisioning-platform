# =============================================================
# USER PROFILE GENERATOR
# =============================================================
# Generates synthetic user profiles and trusted devices.
# Runs ONCE to seed Snowflake DIM schema before streaming.
#
# Class design:
#   - UserProfileGenerator owns profile + device generation
#   - SnowflakeWriter owns all DB writes (injected dependency)
#   - No Snowflake logic in this class — clean separation
#   - FastAPI can call generator.run() without any DB config
# =============================================================

import uuid
import json
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from pathlib import Path

from faker import Faker
from dotenv import load_dotenv

from config import (
    NUM_USERS,
    NUM_DEVICES_PER_USER,
    USER_SPEND_PROFILES,
    RISK_TIER_DISTRIBUTION,
    US_CITIES,
)
from snowflake_writer import SnowflakeWriter

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


class UserProfileGenerator:
    """
    Generates synthetic user profiles and device records.

    Usage:
        generator = UserProfileGenerator(seed=42)
        users, devices = generator.generate()
        generator.save_user_map(users, devices)   # local JSON cache
        generator.write_to_snowflake(users, devices)

    Or use run() to do all of the above in one call:
        generator = UserProfileGenerator()
        generator.run()
    """

    # Device type distribution — weighted toward mobile (2025 reality)
    DEVICE_TYPES = ["MOBILE", "MOBILE", "MOBILE", "WEB", "TABLET"]
    DEVICE_OS_MAP = {
        "MOBILE":  ["iOS", "Android"],
        "WEB":     ["Windows", "macOS", "Linux"],
        "TABLET":  ["iOS", "Android"],
    }

    def __init__(self, num_users: int = NUM_USERS, seed: int = 42):
        """
        Args:
            num_users: Number of user profiles to generate
            seed: Random seed for reproducibility.
                  Same seed = same 100 users every run.
                  Critical for debugging — U1042 always has
                  the same avg_spend, same home city.
        """
        self.num_users = num_users
        self.seed = seed
        self._fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

    # ----------------------------------------------------------
    # RISK TIER SAMPLING
    # ----------------------------------------------------------
    def _pick_risk_tier(self) -> str:
        """
        Sample risk tier from configured distribution.
        50% LOW, 35% MEDIUM, 15% HIGH — reflects real population.
        HIGH risk users are rare but generate disproportionate fraud.
        """
        tiers = list(RISK_TIER_DISTRIBUTION.keys())
        weights = list(RISK_TIER_DISTRIBUTION.values())
        return random.choices(tiers, weights=weights, k=1)[0]

    # ----------------------------------------------------------
    # SPEND BASELINE
    # ----------------------------------------------------------
    def _generate_spend_baseline(
        self, risk_tier: str
    ) -> Tuple[float, float, float]:
        """
        Generate avg spend, stddev, and daily transaction count.

        Why stddev matters:
            Spark uses these for z-score computation:
                z = (txn_amount - avg) / stddev
            A z-score > 3 = flagged as AMOUNT_ANOMALY.
            Without a realistic stddev, everything looks anomalous
            or nothing does — the signal disappears.

        stddev is intentionally correlated with avg:
            High spenders have more variance in their behavior.
            A $300 avg user might vary $40-120 either way.
            A $25 avg user varies $5-15.
        """
        profile = USER_SPEND_PROFILES[risk_tier]
        avg = round(random.uniform(*profile["avg"]), 2)
        stddev = round(random.uniform(*profile["stddev"]), 2)
        daily_txns = round(random.uniform(*profile["daily_txns"]), 2)
        return avg, stddev, daily_txns

    # ----------------------------------------------------------
    # USER GENERATION
    # ----------------------------------------------------------
    def generate_users(self) -> List[Dict]:
        """
        Generate user profiles with SCD2 fields.

        SCD2 initial load pattern:
            valid_from = account_created_at  (when user joined)
            valid_to   = 9999-12-31          (sentinel = current)
            is_current = True

        Coordinate jitter:
            Users in the same city get slightly different lat/lon.
            Without jitter, all Boston users share coordinates,
            making geo-distance features meaningless within the city.
        """
        logger.info(f"Generating {self.num_users} user profiles...")
        users = []

        for _ in range(self.num_users):
            risk_tier = self._pick_risk_tier()
            avg_amt, stddev_amt, daily_txns = self._generate_spend_baseline(risk_tier)
            home_city_data = random.choice(US_CITIES)
            account_created_at = self._random_account_date()

            # Small coordinate jitter — same city, slightly different location
            lat_jitter = random.uniform(-0.05, 0.05)
            lon_jitter = random.uniform(-0.05, 0.05)

            users.append({
                "surrogate_key":           str(uuid.uuid4()),
                "user_id":                 str(uuid.uuid4()),
                "full_name":               self._fake.name(),
                "age":                     random.randint(21, 72),
                "home_city":               home_city_data["city"],
                "home_country":            home_city_data["country"],
                "home_latitude":           round(home_city_data["lat"] + lat_jitter, 6),
                "home_longitude":          round(home_city_data["lon"] + lon_jitter, 6),
                "avg_transaction_amt":     avg_amt,
                "stddev_transaction_amt":  stddev_amt,
                "avg_daily_txn_count":     daily_txns,
                "account_created_at":      account_created_at,
                "risk_tier":               risk_tier,
                "is_active":               True,
                # SCD2 fields
                "valid_from":              account_created_at,
                "valid_to":                datetime(9999, 12, 31, 0, 0, 0),
                "is_current":              True,
                "updated_at":              datetime.utcnow(),
            })

        logger.info(f"Generated {len(users)} user profiles")
        return users

    # ----------------------------------------------------------
    # DEVICE GENERATION
    # ----------------------------------------------------------
    def generate_devices(self, users: List[Dict]) -> List[Dict]:
        """
        Generate 1-3 trusted devices per user.

        Why this matters:
            The transaction generator checks if a device_id is
            in the user's trusted_devices list. If not, it flags
            the transaction as NEW_DEVICE — a real fraud signal.
            Without pre-seeding devices, every transaction would
            look like a new device.

        Device trust logic:
            First device = always trusted (registered at signup)
            Subsequent devices = randomly trusted or not
        """
        logger.info("Generating devices for all users...")
        devices = []

        for user in users:
            num_devices = random.randint(*NUM_DEVICES_PER_USER)

            for i in range(num_devices):
                device_type = random.choice(self.DEVICE_TYPES)
                device_os = random.choice(self.DEVICE_OS_MAP[device_type])

                # First device registered at account creation
                # Later devices registered days/months after
                days_offset = 0 if i == 0 else random.randint(30, 365)
                first_seen = user["account_created_at"] + timedelta(days=days_offset)

                devices.append({
                    "device_id":     str(uuid.uuid4()),
                    "user_id":       user["user_id"],
                    "device_type":   device_type,
                    "device_os":     device_os,
                    "first_seen_at": first_seen,
                    "is_trusted":    True if i == 0 else random.choice([True, False]),
                    "registered_at": first_seen,
                })

        logger.info(f"Generated {len(devices)} devices")
        return devices

    # ----------------------------------------------------------
    # LOCAL USER MAP (JSON cache)
    # ----------------------------------------------------------
    def save_user_map(
        self, users: List[Dict], devices: List[Dict]
    ) -> Dict:
        """
        Save user profiles to local JSON file.

        Why not just query Snowflake at runtime?
            The transaction generator runs in a tight loop
            producing 2 txns/sec. Hitting Snowflake on every
            transaction for user profile data would add 1-3s
            latency per event — killing throughput.
            JSON file read = microseconds.

        The transaction generator imports this map on startup
        and keeps it in memory for the lifetime of the process.
        """
        # Build device lookup: user_id → [device_ids]
        device_map: Dict[str, List[str]] = {}
        for d in devices:
            device_map.setdefault(d["user_id"], []).append(d["device_id"])

        user_map = {
            u["user_id"]: {
                "user_id":                u["user_id"],
                "full_name":              u["full_name"],
                "home_city":              u["home_city"],
                "home_country":           u["home_country"],
                "home_latitude":          u["home_latitude"],
                "home_longitude":         u["home_longitude"],
                "avg_transaction_amt":    u["avg_transaction_amt"],
                "stddev_transaction_amt": u["stddev_transaction_amt"],
                "avg_daily_txn_count":    u["avg_daily_txn_count"],
                "risk_tier":              u["risk_tier"],
                "trusted_devices":        device_map.get(u["user_id"], []),
            }
            for u in users
        }

        output_path = Path(__file__).parent / "user_map.json"
        with open(output_path, "w") as f:
            json.dump(user_map, f, indent=2, default=str)

        logger.info(f"User map saved → {output_path}")
        return user_map

    # ----------------------------------------------------------
    # SNOWFLAKE WRITE
    # ----------------------------------------------------------
    def write_to_snowflake(
        self, users: List[Dict], devices: List[Dict]
    ) -> None:
        """
        Write users and devices to Snowflake via SnowflakeWriter.
        Guards against accidental re-runs duplicating data.
        """
        with SnowflakeWriter() as writer:
            existing = writer.check_users_exist()
            if existing > 0:
                logger.warning(
                    f"DIM_USERS already has {existing} rows. "
                    f"Skipping Snowflake write to prevent duplicates. "
                    f"Truncate the table first if you want to re-seed."
                )
                return

            writer.write_users(users)
            writer.write_devices(devices)

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    def _print_summary(self, users: List[Dict], devices: List[Dict]) -> None:
        risk_counts: Dict[str, int] = {}
        risk_avg: Dict[str, List[float]] = {}

        for u in users:
            t = u["risk_tier"]
            risk_counts[t] = risk_counts.get(t, 0) + 1
            risk_avg.setdefault(t, []).append(u["avg_transaction_amt"])

        print("\n" + "=" * 60)
        print("GENERATION COMPLETE")
        print(f"  Users:          {len(users)}")
        print(f"  Devices:        {len(devices)}")
        print(f"  Risk dist:      {risk_counts}")
        for tier, amts in risk_avg.items():
            print(f"  Avg spend ({tier[:3]}): ${sum(amts)/len(amts):.2f}")
        print("=" * 60 + "\n")

    # ----------------------------------------------------------
    # ORCHESTRATOR
    # ----------------------------------------------------------
    def generate(self) -> Tuple[List[Dict], List[Dict]]:
        """Generate users and devices. Does not write anywhere."""
        users = self.generate_users()
        devices = self.generate_devices(users)
        return users, devices

    def run(self) -> None:
        """
        Full pipeline:
        1. Generate users + devices
        2. Save local JSON cache
        3. Write to Snowflake
        4. Print summary
        """
        logger.info("Starting user profile generation pipeline...")
        users, devices = self.generate()
        self.save_user_map(users, devices)
        self.write_to_snowflake(users, devices)
        self._print_summary(users, devices)

    # ----------------------------------------------------------
    # HELPERS
    # ----------------------------------------------------------
    def _random_account_date(self) -> datetime:
        """Account created 1-5 years ago."""
        days_ago = random.randint(365, 1825)
        return datetime.utcnow() - timedelta(days=days_ago)


# -------------------------------------------------------------
# ENTRY POINT
# Called directly: python user_profile_generator.py
# Called via API: UserProfileGenerator().run()
# -------------------------------------------------------------
if __name__ == "__main__":
    generator = UserProfileGenerator(num_users=NUM_USERS, seed=42)
    generator.run()