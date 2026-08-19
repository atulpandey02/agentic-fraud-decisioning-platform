# =============================================================
# SCHEDULER — decides WHEN a stored workflow runs (nothing more)
# =============================================================
# The fourth responsibility, kept separate on purpose: the scheduler
# does not plan, does not author SQL, does not deliver. It converts a
# validated Schedule into an APScheduler cron job and, when that job
# fires, hands the SAME executor the SAME stored plan — with one thing
# added, the report window, computed in code from the fire time.
#
# Three reliability properties are built in, each a lesson this
# platform already learned once about fail-open behavior:
#   - RESTART RECOVERY: schedules live in SQLite, not in the
#     scheduler's memory. start()/reload() re-registers every
#     persisted, non-paused scheduled workflow, so a process restart
#     resumes them instead of losing them.
#   - NO DUPLICATE REPORTS: every fire is stamped with a
#     (workflow, period) key. run_exists_for_key short-circuits a
#     repeat, and the DB unique index is the backstop if a race slips
#     past the check — a duplicate fire is a no-op, never a second
#     Slack message.
#   - NO FAIL-OPEN: a fire that can't run (paused, missing plan)
#     returns without side effects; a step failure ends the run in
#     FAILED via the executor, exactly like a manual run.
# =============================================================

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from .schedule import Schedule
from .state import WorkflowState, WorkflowStore

logger = logging.getLogger(__name__)

# How far back a report looks, per frequency. The window is a RUNTIME
# parameter the scheduler supplies; the SQL itself never changes.
_PERIOD = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(days=1),
    "weekly": timedelta(weeks=1),
}


def compute_window(frequency: str, fire_dt: datetime) -> tuple[str, str]:
    """The [start, end) the report covers for a fire at `fire_dt`:
    end = the fire time, start = one period earlier. Returned as naive
    ISO strings (the catalog binds them as TIMESTAMP_NTZ literals)."""
    end = fire_dt.replace(tzinfo=None)
    start = end - _PERIOD.get(frequency, timedelta(days=1))
    return start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")


def run_key(frequency: str, fire_dt: datetime) -> str:
    """A stable idempotency key for one scheduled period. Two fires in
    the same period collapse to the same key, so only the first runs."""
    if frequency == "hourly":
        stamp = fire_dt.strftime("%Y-%m-%dT%H")
    elif frequency == "weekly":
        iso = fire_dt.isocalendar()
        stamp = f"{iso[0]}-W{iso[1]:02d}"
    else:  # daily (default)
        stamp = fire_dt.strftime("%Y-%m-%d")
    return f"{frequency}:{stamp}"


def arm_for_run(store: WorkflowStore, workflow_id: str) -> None:
    """Move a workflow to READY so a run can start — the scheduler's
    plain-Python equivalent of the API's _arm (no HTTP coupling)."""
    state = WorkflowState(store.get_workflow(workflow_id)["state"])
    if state == WorkflowState.READY:
        return
    if state in (WorkflowState.FEASIBLE, WorkflowState.COMPLETED, WorkflowState.FAILED):
        store.transition(workflow_id, WorkflowState.READY)
        return
    raise RuntimeError(f"workflow in state {state.value} cannot be armed for a scheduled run")


class SchedulerRuntime:
    """Wraps an APScheduler BackgroundScheduler. Construction is cheap
    and importable without a running scheduler; start() spins the
    background thread and reloads persisted jobs. `_fire` is public-ish
    (called by the scheduler thread) and is directly callable in tests
    with an explicit fire time, so scheduled execution can be verified
    with no wall clock and no infra."""

    def __init__(self, store: WorkflowStore, executor,
                 arm: Callable[[WorkflowStore, str], None] = arm_for_run,
                 misfire_grace_time: int = 3600) -> None:
        self._store = store
        self._executor = executor
        self._arm = arm
        self._misfire = misfire_grace_time
        self._sched = None

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler
        if self._sched is None:
            self._sched = BackgroundScheduler()
            self._sched.start()
        self.reload()

    def shutdown(self) -> None:
        if self._sched is not None:
            self._sched.shutdown(wait=False)
            self._sched = None

    def reload(self) -> int:
        """Restart recovery: (re)register a job for each persisted,
        non-paused scheduled workflow. Returns how many were loaded."""
        n = 0
        for wf in self._store.scheduled_workflows():
            try:
                schedule = Schedule.model_validate_json(wf["schedule_json"])
            except Exception as e:  # noqa: BLE001 — a bad stored schedule is skipped, not fatal
                logger.warning("skipping workflow %s: bad schedule_json (%s)", wf["id"], e)
                continue
            self._register(wf["id"], schedule)
            n += 1
        logger.info("scheduler reloaded %d scheduled workflow(s)", n)
        return n

    # ---------------------------------------------------------- registration
    def schedule_workflow(self, workflow_id: str, schedule: Schedule) -> Optional[str]:
        """Register (or replace) the cron job for a workflow and persist
        its next fire time. Returns the next fire time (ISO)."""
        self._register(workflow_id, schedule)
        return self._store.get_workflow(workflow_id).get("next_run_at")

    def _register(self, workflow_id: str, schedule: Schedule) -> None:
        # next_run comes from the cron trigger itself, so it is persisted
        # whether or not the background thread is running (the store, not
        # the scheduler's memory, is the source of truth).
        self._store.set_next_run(workflow_id, self._next_fire(schedule))
        if self._sched is None:
            return  # not started (planning-only / tests) — persistence still holds it
        self._sched.add_job(
            self._fire, trigger=schedule.to_cron_trigger(),
            args=[workflow_id, schedule.frequency], id=workflow_id,
            replace_existing=True, coalesce=True, max_instances=1,
            misfire_grace_time=self._misfire,
        )

    def pause(self, workflow_id: str) -> None:
        self._store.set_paused(workflow_id, True)
        if self._sched is not None and self._sched.get_job(workflow_id):
            self._sched.remove_job(workflow_id)
        self._store.set_next_run(workflow_id, None)

    def resume(self, workflow_id: str) -> None:
        self._store.set_paused(workflow_id, False)
        wf = self._store.get_workflow(workflow_id)
        self._register(workflow_id, Schedule.model_validate_json(wf["schedule_json"]))

    @staticmethod
    def _next_fire(schedule: Schedule) -> Optional[str]:
        nxt = schedule.to_cron_trigger().get_next_fire_time(None, datetime.now(timezone.utc))
        return nxt.isoformat() if nxt else None

    # ---------------------------------------------------------- firing
    def _fire(self, workflow_id: str, frequency: str,
              fire_dt: Optional[datetime] = None):
        """Execute one scheduled run. Idempotent per period; never
        fires a paused/rejected workflow; supplies the report window as
        the trigger payload so the stored plan's run_report_query step
        binds it as a runtime parameter."""
        wf = self._store.get_workflow(workflow_id)
        if wf is None or wf.get("schedule_paused") or wf.get("trigger_type") != "schedule":
            return None
        if not wf.get("plan_json"):
            logger.warning("scheduled workflow %s has no plan — skipping", workflow_id)
            return None

        fire_dt = fire_dt or datetime.now(timezone.utc)
        key = run_key(frequency, fire_dt)
        if self._store.run_exists_for_key(workflow_id, key):
            logger.info("scheduled fire for %s already ran this period (%s) — skipping",
                        workflow_id, key)
            return None

        from .planner import WorkflowPlan
        plan = WorkflowPlan.model_validate_json(wf["plan_json"])
        window_start, window_end = compute_window(frequency, fire_dt)
        payload = {"window_start": window_start, "window_end": window_end,
                   "fired_at": fire_dt.isoformat()}

        self._arm(self._store, workflow_id)
        try:
            result = self._executor.execute(workflow_id, plan, trigger_payload=payload,
                                            scheduled_run_key=key)
        except sqlite3.IntegrityError:
            # the unique index caught a racing duplicate fire — no-op.
            logger.info("duplicate scheduled fire for %s raced and was rejected", workflow_id)
            return None

        self._store.set_last_run(workflow_id, fire_dt.isoformat())
        self._store.set_last_report(workflow_id, self._extract_report(result))
        try:
            self._store.set_next_run(
                workflow_id, self._next_fire(Schedule.model_validate_json(wf["schedule_json"])))
        except Exception:  # noqa: BLE001 — next_run is advisory; a run already happened
            pass
        logger.info("scheduled run for %s finished: %s", workflow_id, result.status)
        return result

    @staticmethod
    def _extract_report(result) -> Optional[str]:
        """Pull the formatted report (title + metrics) out of a run for
        persistence, if a format_report step produced one."""
        for o in result.steps:
            if o.status == "ok" and isinstance(o.result, dict) and "metrics" in o.result:
                return json.dumps({"title": o.result.get("title"),
                                   "metrics": o.result.get("metrics"),
                                   "generated_at": o.result.get("generated_at")}, default=str)
        return None
