# =============================================================
# UNIT TESTS — Schedule model (structured data, validated in code)
# =============================================================
# No LLM, no APScheduler needed for the model + cron-kwargs tests
# (to_cron_trigger is exercised separately since it imports the lib).
# =============================================================

import pytest
from pydantic import ValidationError

from fraud_platform.workflow_engine.schedule import Schedule


class TestScheduleParsing:
    def test_daily_at_10pm_parses(self):
        s = Schedule(frequency="daily", hour=22, minute=0, timezone="America/New_York")
        assert s.trigger_type == "schedule"
        assert s.to_cron_kwargs() == {"hour": 22, "minute": 0, "timezone": "America/New_York"}
        assert "10:00 PM" in s.describe() and "America/New_York" in s.describe()

    def test_hourly_kwargs_have_no_hour(self):
        s = Schedule(frequency="hourly", minute=15, timezone="UTC")
        assert s.to_cron_kwargs() == {"minute": 15, "timezone": "UTC"}

    def test_weekly_requires_and_normalizes_weekday(self):
        s = Schedule(frequency="weekly", day_of_week="Monday", hour=9, minute=30, timezone="UTC")
        assert s.day_of_week == "mon"
        assert s.to_cron_kwargs() == {"day_of_week": "mon", "hour": 9, "minute": 30, "timezone": "UTC"}
        assert "Monday" in s.describe()


class TestScheduleValidation:
    def test_timezone_is_required(self):
        with pytest.raises(ValidationError):
            Schedule(frequency="daily", hour=22, minute=0)  # no timezone

    def test_empty_timezone_rejected(self):
        with pytest.raises(ValidationError) as ei:
            Schedule(frequency="daily", hour=22, timezone="   ")
        assert "timezone" in str(ei.value)

    def test_unknown_timezone_rejected(self):
        with pytest.raises(ValidationError) as ei:
            Schedule(frequency="daily", hour=22, timezone="Mars/Olympus_Mons")
        assert "unknown timezone" in str(ei.value)

    def test_daily_requires_hour(self):
        with pytest.raises(ValidationError) as ei:
            Schedule(frequency="daily", timezone="UTC")
        assert "requires an explicit hour" in str(ei.value)

    def test_weekly_requires_day_of_week(self):
        with pytest.raises(ValidationError) as ei:
            Schedule(frequency="weekly", hour=9, timezone="UTC")
        assert "requires a day_of_week" in str(ei.value)

    def test_bad_weekday_rejected(self):
        with pytest.raises(ValidationError):
            Schedule(frequency="weekly", day_of_week="funday", hour=9, timezone="UTC")

    def test_hour_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            Schedule(frequency="daily", hour=25, timezone="UTC")


class TestCronTrigger:
    def test_to_cron_trigger_builds_and_computes_next_fire(self):
        # Exercises the real APScheduler CronTrigger: a daily 22:00 NY
        # schedule must produce a concrete future fire time.
        from datetime import datetime
        from zoneinfo import ZoneInfo

        s = Schedule(frequency="daily", hour=22, minute=0, timezone="America/New_York")
        trigger = s.to_cron_trigger()
        now = datetime(2026, 8, 19, 9, 0, tzinfo=ZoneInfo("America/New_York"))
        nxt = trigger.get_next_fire_time(None, now)
        assert nxt is not None
        assert (nxt.hour, nxt.minute) == (22, 0)
        assert nxt.tzinfo is not None  # timezone-aware, never naive
