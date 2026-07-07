# =============================================================
# UNIT TESTS — data generator determinism (Priority 4 item 4)
# =============================================================
# Deterministic seeds + a FIXED CLOCK make generator output
# reproducible in tests. The seed already controls Faker/random;
# the clock is frozen here by monkeypatching the module's datetime,
# so timestamp-derived fields (account_created_at, updated_at) stop
# depending on wall-clock time and become assertable.
#
# Note surfaced by these tests: user_id/surrogate_key use uuid4,
# which the seed does NOT control — so identity fields are NOT
# reproducible across runs by design. The reproducible contract is
# the SEEDED behavioral fields (risk tier, spend, city, name, age).
# =============================================================

import datetime as _dt

import pytest

from fraud_platform.data_generator import user_profile_generator as upg
from fraud_platform.data_generator.user_profile_generator import UserProfileGenerator

FIXED = _dt.datetime(2026, 1, 1, 12, 0, 0)

# The seeded fields — everything the "same seed = same users" contract
# actually covers (excludes uuid4 identity + wall-clock timestamps).
SEEDED_FIELDS = (
    "full_name", "age", "home_city", "risk_tier",
    "avg_transaction_amt", "stddev_transaction_amt", "avg_daily_txn_count",
)


class _FakeDateTime:
    """A drop-in for the module's `datetime`: utcnow() is frozen, but
    ordinary construction (e.g. datetime(9999, 12, 31)) still works."""
    @staticmethod
    def utcnow():
        return FIXED

    def __new__(cls, *args, **kwargs):
        return _dt.datetime(*args, **kwargs)


@pytest.fixture
def frozen_clock(monkeypatch):
    monkeypatch.setattr(upg, "datetime", _FakeDateTime)


def _seeded(u):
    return {k: u[k] for k in SEEDED_FIELDS}


class TestSeedDeterminism:
    def test_same_seed_same_seeded_fields(self):
        a = UserProfileGenerator(num_users=25, seed=42).generate_users()
        b = UserProfileGenerator(num_users=25, seed=42).generate_users()
        assert [_seeded(u) for u in a] == [_seeded(u) for u in b]

    def test_different_seed_differs(self):
        a = UserProfileGenerator(num_users=25, seed=42).generate_users()
        b = UserProfileGenerator(num_users=25, seed=7).generate_users()
        assert [_seeded(u) for u in a] != [_seeded(u) for u in b]

    def test_count_respected(self):
        users = UserProfileGenerator(num_users=13, seed=1).generate_users()
        assert len(users) == 13

    def test_risk_tiers_are_valid_enum(self):
        users = UserProfileGenerator(num_users=50, seed=3).generate_users()
        assert {u["risk_tier"] for u in users} <= {"LOW", "MEDIUM", "HIGH"}


class TestFixedClock:
    def test_frozen_clock_makes_timestamps_reproducible(self, frozen_clock):
        users = UserProfileGenerator(num_users=10, seed=42).generate_users()
        # updated_at is exactly the frozen 'now'
        assert all(u["updated_at"] == FIXED for u in users)
        # account_created_at = frozen now minus a seeded offset -> in the past, bounded
        for u in users:
            assert u["account_created_at"] <= FIXED
            assert u["account_created_at"] > FIXED - _dt.timedelta(days=366 * 5)

    def test_same_seed_and_clock_reproduce_account_dates(self, frozen_clock):
        a = UserProfileGenerator(num_users=10, seed=42).generate_users()
        b = UserProfileGenerator(num_users=10, seed=42).generate_users()
        assert [u["account_created_at"] for u in a] == [u["account_created_at"] for u in b]


class TestDeviceGeneration:
    def test_devices_within_configured_range_and_first_trusted(self):
        gen = UserProfileGenerator(num_users=20, seed=42)
        users = gen.generate_users()
        devices = gen.generate_devices(users)
        by_user = {}
        for d in devices:
            by_user.setdefault(d["user_id"], []).append(d)
        for user_devices in by_user.values():
            assert 1 <= len(user_devices) <= 3
            # first device per user is always trusted (registered at signup)
            assert any(d["is_trusted"] for d in user_devices)
