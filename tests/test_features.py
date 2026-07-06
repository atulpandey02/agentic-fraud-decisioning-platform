# =============================================================
# UNIT TESTS — geo scoring rules (pure functions, no infra)
# =============================================================
# Written FIRST, against the fix approved in
# GEO_FLAGGING_INVESTIGATION.md, before the implementation —
# these tests define the contract the fix must satisfy. They
# import feature_engine directly (heavy imports, but nothing
# connects at import time) and touch no Redis/Kafka/Snowflake.
# =============================================================


import pytest

from fraud_platform.stream_processing.feature_engine import (  # noqa: E402
    FeatureEngineConfig,
    haversine_distance,
    compute_implied_speed_kmh,
    geo_hard_rule,
)


# -------------------------------------------------------------
# Haversine — reference distances (checked against the generator's
# own documented city pairs)
# -------------------------------------------------------------
class TestHaversine:
    def test_boston_to_london_is_intercontinental(self):
        # generator config documents Boston→London ≈ 5,265 km
        d = haversine_distance(42.3601, -71.0589, 51.5074, -0.1278)
        assert 5200 < d < 5350

    def test_zero_distance(self):
        assert haversine_distance(42.36, -71.06, 42.36, -71.06) == pytest.approx(0.0)

    def test_none_input_returns_none(self):
        assert haversine_distance(None, -71.06, 51.5, -0.13) is None


# -------------------------------------------------------------
# Implied speed — the time-aware half of the composite rule
# -------------------------------------------------------------
class TestImpliedSpeed:
    def test_boston_london_in_40_minutes_is_impossible(self):
        # the canonical GEO_JUMP case study: ~5265 km in ~40 min
        speed = compute_implied_speed_kmh(5265.0, 40.0)
        assert speed == pytest.approx(5265.0 / (40.0 / 60.0))
        assert speed > 900

    def test_domestic_trip_over_a_day_is_slow(self):
        speed = compute_implied_speed_kmh(1400.0, 24 * 60.0)
        assert speed < 100

    def test_negative_delta_returns_none(self):
        # out-of-order event: the "previous" location is in the
        # event-time future — no meaningful speed exists
        assert compute_implied_speed_kmh(5265.0, -12.0) is None

    def test_zero_delta_returns_none(self):
        assert compute_implied_speed_kmh(5265.0, 0.0) is None

    def test_none_inputs_return_none(self):
        assert compute_implied_speed_kmh(None, 40.0) is None
        assert compute_implied_speed_kmh(5265.0, None) is None


# -------------------------------------------------------------
# The composite hard rule from GEO_FLAGGING_INVESTIGATION.md:
# speed > 900 km/h (with a distance floor) OR distance >= 3000 km
# -------------------------------------------------------------
class TestGeoHardRule:
    def test_impossible_speed_fires(self):
        assert geo_hard_rule(distance_km=5265.0, implied_speed_kmh=7900.0) is True

    def test_intercontinental_distance_fires_without_speed(self):
        # 80% of backfill rows have no usable time delta — the
        # distance arm must still catch true jumps on its own
        assert geo_hard_rule(distance_km=5265.0, implied_speed_kmh=None) is True

    def test_domestic_travel_does_not_fire(self):
        # Boston→Chicago (~1,400 km) over a normal day: the exact
        # false-positive class the old >=475 km rule flagged
        assert geo_hard_rule(distance_km=1400.0, implied_speed_kmh=58.0) is False

    def test_old_threshold_distance_no_longer_fires(self):
        # 475-500 km with no time information was the bulk of the
        # 331,620 geo-only flags at 17.3% precision
        assert geo_hard_rule(distance_km=500.0, implied_speed_kmh=None) is False

    def test_gps_noise_speed_does_not_fire(self):
        # 2 km in ~5 seconds is a 1,440 km/h "speed" produced by
        # location jitter — the distance floor must suppress it
        assert geo_hard_rule(distance_km=2.0, implied_speed_kmh=1440.0) is False

    def test_fast_but_plausible_does_not_fire(self):
        # 800 km/h is commercial-jet speed — allowed by design
        assert geo_hard_rule(distance_km=800.0, implied_speed_kmh=800.0) is False

    def test_none_distance_never_fires(self):
        assert geo_hard_rule(distance_km=None, implied_speed_kmh=None) is False

    def test_thresholds_come_from_config(self):
        assert FeatureEngineConfig.HARD_RULE_GEO_MIN_JUMP_KM == 3000.0
        assert FeatureEngineConfig.IMPOSSIBLE_TRAVEL_SPEED_KMH == 900.0
        assert FeatureEngineConfig.SPEED_RULE_MIN_DISTANCE_KM == 100.0
