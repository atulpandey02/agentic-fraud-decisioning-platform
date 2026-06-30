# =============================================================
# SNOWFLAKE WRITER
# =============================================================
# Handles all Snowflake write operations for the data generator.
# Kept separate from business logic so:
#   1. Any class can import and use it without duplicating
#      connection logic
#   2. Easy to mock in unit tests — swap real Snowflake for
#      a fake writer without touching generator logic
#   3. When FastAPI calls generators, this is the only class
#      that knows Snowflake exists
# =============================================================

import os
import logging
from typing import List, Dict

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SnowflakeWriter:
    """
    Manages Snowflake connection and all DIM/RAW schema writes.
    Use as a context manager to ensure connection is always closed:

        with SnowflakeWriter() as writer:
            writer.write_users(users)
            writer.write_devices(devices)
    """

    def __init__(self):
        self._conn = None
        self._conn_params = {
            "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
            "user":      os.getenv("SNOWFLAKE_USER"),
            "password":  os.getenv("SNOWFLAKE_PASSWORD"),
            "database":  os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DETECTION"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "role":      os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "schema":    os.getenv("SNOWFLAKE_SCHEMA", "DIM"),
        }

    # ----------------------------------------------------------
    # CONNECTION MANAGEMENT
    # ----------------------------------------------------------
    def connect(self):
        """Open Snowflake connection."""
        if self._conn is None or self._conn.is_closed():
            logger.info("Connecting to Snowflake...")
            self._conn = snowflake.connector.connect(**self._conn_params)
            logger.info("Snowflake connection established.")
        return self

    def close(self):
        """Close Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Snowflake connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_cursor(self):
        if self._conn is None or self._conn.is_closed():
            raise RuntimeError("Not connected. Call connect() or use as context manager.")
        return self._conn.cursor()

    # ----------------------------------------------------------
    # DIM SCHEMA WRITES
    # ----------------------------------------------------------
    def write_users(self, users: List[Dict]) -> int:
        """
        Batch insert user profiles into DIM.DIM_USERS.
        SCD2 fields (valid_from, valid_to, is_current) are
        set by UserProfileGenerator before calling this.
        Returns row count inserted.
        """
        cursor = self._get_cursor()
        try:
            sql = """
                INSERT INTO DIM.DIM_USERS (
                    surrogate_key, user_id, full_name, age,
                    home_city, home_country, home_latitude, home_longitude,
                    avg_transaction_amt, stddev_transaction_amt,
                    avg_daily_txn_count, account_created_at,
                    risk_tier, is_active,
                    valid_from, valid_to, is_current, updated_at
                ) VALUES (
                    %(surrogate_key)s, %(user_id)s, %(full_name)s, %(age)s,
                    %(home_city)s, %(home_country)s,
                    %(home_latitude)s, %(home_longitude)s,
                    %(avg_transaction_amt)s, %(stddev_transaction_amt)s,
                    %(avg_daily_txn_count)s, %(account_created_at)s,
                    %(risk_tier)s, %(is_active)s,
                    %(valid_from)s, %(valid_to)s, %(is_current)s, %(updated_at)s
                )
            """
            cursor.executemany(sql, users)
            self._conn.commit()
            logger.info(f"Inserted {len(users)} users into DIM.DIM_USERS")
            return len(users)
        finally:
            cursor.close()

    def write_devices(self, devices: List[Dict]) -> int:
        """
        Batch insert device records into DIM.DIM_DEVICES.
        Returns row count inserted.
        """
        cursor = self._get_cursor()
        try:
            sql = """
                INSERT INTO DIM.DIM_DEVICES (
                    device_id, user_id, device_type, device_os,
                    first_seen_at, is_trusted, registered_at
                ) VALUES (
                    %(device_id)s, %(user_id)s, %(device_type)s, %(device_os)s,
                    %(first_seen_at)s, %(is_trusted)s, %(registered_at)s
                )
            """
            cursor.executemany(sql, devices)
            self._conn.commit()
            logger.info(f"Inserted {len(devices)} devices into DIM.DIM_DEVICES")
            return len(devices)
        finally:
            cursor.close()

    def write_transactions(self, transactions: List[Dict]) -> int:
        """
        Batch insert raw transactions into RAW.FACT_TRANSACTIONS.
        Called by Spark's dual sink — not the generator directly.
        Included here so all Snowflake writes go through one class.
        Returns row count inserted.
        """
        cursor = self._get_cursor()
        try:
            sql = """
                INSERT INTO RAW.FACT_TRANSACTIONS (
                    transaction_id, user_id, device_id,
                    amount, currency,
                    merchant_name, merchant_category,
                    city, country, latitude, longitude,
                    transaction_ts, ingested_at,
                    is_synthetic_fraud, fraud_pattern
                ) VALUES (
                    %(transaction_id)s, %(user_id)s, %(device_id)s,
                    %(amount)s, %(currency)s,
                    %(merchant_name)s, %(merchant_category)s,
                    %(city)s, %(country)s, %(latitude)s, %(longitude)s,
                    %(transaction_ts)s, %(ingested_at)s,
                    %(is_synthetic_fraud)s, %(fraud_pattern)s
                )
            """
            cursor.executemany(sql, transactions)
            self._conn.commit()
            logger.info(f"Inserted {len(transactions)} transactions into RAW.FACT_TRANSACTIONS")
            return len(transactions)
        finally:
            cursor.close()

    def check_users_exist(self) -> int:
        """
        Check if DIM_USERS already has data.
        Prevents accidental re-runs from duplicating profiles.
        """
        cursor = self._get_cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM DIM.DIM_USERS WHERE is_current = TRUE")
            return cursor.fetchone()[0]
        finally:
            cursor.close()