# =============================================================
# SCHEDULE — structured schedule data, validated + converted in CODE
# =============================================================
# The scheduling equivalent of the SQL guard: the LLM may INTERPRET
# "every day at 10 PM" into this structure, but it never emits a raw
# cron string and code never executes one the model wrote. A Schedule
# is plain validated data; Python (not the model) turns it into an
# APScheduler CronTrigger. Same "judgment from models, invariants
# from code" split as feasibility.py and the governance tiers.
#
# Timezone is REQUIRED and validated against the IANA database — a
# schedule without an explicit zone is ambiguous ("10 PM" where?),
# and an ambiguous schedule that silently picks the server's zone is
# exactly the kind of quiet wrong-default this platform refuses. A
# bad zone fails LOUD at validation time, not at 10 PM in production.
# =============================================================

from __future__ import annotations

from typing import Literal, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator

Frequency = Literal["hourly", "daily", "weekly"]

# Weekday NAMES only (not 0-6): the 0=Monday-vs-Sunday convention is a
# classic off-by-one footgun, so we accept unambiguous names and hand
# them straight to APScheduler (which also accepts them).
_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


class Schedule(BaseModel):
    """A recurring schedule as structured data. The LLM produces this
    from natural language; `validate` (pydantic) and `to_cron_kwargs`
    are the code that turns it into something executable — the model
    never touches cron syntax."""

    trigger_type: Literal["schedule"] = "schedule"
    frequency: Frequency = Field(description="hourly | daily | weekly")
    minute: int = Field(default=0, ge=0, le=59, description="minute of the hour, 0-59")
    hour: Optional[int] = Field(
        default=None, ge=0, le=23,
        description="hour of day 0-23 (required for daily/weekly)",
    )
    day_of_week: Optional[str] = Field(
        default=None,
        description="weekday name mon..sun (required for weekly)",
    )
    timezone: str = Field(
        description="IANA timezone, e.g. 'America/New_York' — REQUIRED and validated",
    )

    # ---- validators: reject an un-runnable schedule at construction ----
    @field_validator("timezone")
    @classmethod
    def _tz_must_be_valid(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("timezone is required (an IANA zone like 'America/New_York')")
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, OSError) as e:
            raise ValueError(f"unknown timezone {v!r}: {e}") from e
        return v

    @field_validator("day_of_week")
    @classmethod
    def _weekday_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        low = v.strip().lower()[:3]
        if low not in _WEEKDAYS:
            raise ValueError(f"day_of_week must be one of {_WEEKDAYS}, got {v!r}")
        return low

    @model_validator(mode="after")
    def _fields_required_by_frequency(self) -> "Schedule":
        if self.frequency in ("daily", "weekly") and self.hour is None:
            raise ValueError(f"{self.frequency} schedule requires an explicit hour (0-23)")
        if self.frequency == "weekly" and self.day_of_week is None:
            raise ValueError("weekly schedule requires a day_of_week (mon..sun)")
        return self

    # ---- conversion: structured data -> cron fields (pure, testable) ----
    def to_cron_kwargs(self) -> dict:
        """The APScheduler CronTrigger field mapping, as a plain dict.
        Pure and dependency-free so it is unit-testable without
        APScheduler; to_cron_trigger() feeds these straight in."""
        if self.frequency == "hourly":
            return {"minute": self.minute, "timezone": self.timezone}
        if self.frequency == "daily":
            return {"hour": self.hour, "minute": self.minute, "timezone": self.timezone}
        # weekly
        return {"day_of_week": self.day_of_week, "hour": self.hour,
                "minute": self.minute, "timezone": self.timezone}

    def to_cron_trigger(self):
        """Build an APScheduler CronTrigger. Imported lazily so this
        module (and the Schedule model) stay usable — and testable —
        without APScheduler installed."""
        from apscheduler.triggers.cron import CronTrigger
        return CronTrigger(**self.to_cron_kwargs())

    def describe(self) -> str:
        """Human-readable one-liner for the UI preview, e.g.
        'Every day at 10:00 PM America/New_York'. Deterministic — the
        plan the user approves says exactly when it will fire."""
        if self.frequency == "hourly":
            return f"Every hour at :{self.minute:02d} {self.timezone}"
        clock = self._12h(self.hour, self.minute)
        if self.frequency == "daily":
            return f"Every day at {clock} {self.timezone}"
        day = {"mon": "Monday", "tue": "Tuesday", "wed": "Wednesday", "thu": "Thursday",
               "fri": "Friday", "sat": "Saturday", "sun": "Sunday"}[self.day_of_week]
        return f"Every {day} at {clock} {self.timezone}"

    @staticmethod
    def _12h(hour: int, minute: int) -> str:
        suffix = "AM" if hour < 12 else "PM"
        h12 = hour % 12 or 12
        return f"{h12}:{minute:02d} {suffix}"
