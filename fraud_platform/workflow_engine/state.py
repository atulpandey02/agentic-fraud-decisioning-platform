# =============================================================
# WORKFLOW STATE — the state machine + its SQLite persistence
# =============================================================
# The STATE MACHINE is real and its transitions are enforced in
# code (invariants from code, judgment from models — the same split
# as governance/policy_framework.py). The STORE is deliberately
# lightweight: SQLite, one file, zero infra. Production would be
# Postgres/Snowflake; only the store swaps, not the machine.
#
# Lifecycle:
#   SUBMITTED -> PLANNED -> FEASIBLE | REJECTED
#   FEASIBLE  -> (AWAITING_APPROVAL ->) READY -> RUNNING
#   RUNNING   -> COMPLETED | FAILED | PAUSED
#   PAUSED    -> RUNNING (resume)
#
# The approval gate is a STATE TRANSITION, not a prompt instruction:
# a plan that uses a requires_approval tool must pass through
# AWAITING_APPROVAL, and only an explicit approve() call moves it to
# READY. A model cannot talk its way past a state edge.
#
# Checkpointing: a step_results row is written after EVERY step, in
# the same transaction that touches the run — so a crash leaves a
# durable record of exactly how far execution got, and resume is
# "find the last completed step, continue from the next." No step is
# ever silently lost (the audited Redis fail-open lesson, applied to
# workflow state).
# =============================================================

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional

from . import config

logger = logging.getLogger(__name__)


class WorkflowState(str, Enum):
    SUBMITTED = "SUBMITTED"
    PLANNED = "PLANNED"
    FEASIBLE = "FEASIBLE"
    REJECTED = "REJECTED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"


# The ONLY legal edges. Anything not here raises — a transition is a
# law, not a suggestion. Terminal states (REJECTED/COMPLETED) have no
# outgoing edges.
ALLOWED_TRANSITIONS: dict[WorkflowState, set[WorkflowState]] = {
    WorkflowState.SUBMITTED: {WorkflowState.PLANNED},
    WorkflowState.PLANNED: {WorkflowState.FEASIBLE, WorkflowState.REJECTED},
    WorkflowState.FEASIBLE: {WorkflowState.AWAITING_APPROVAL, WorkflowState.READY},
    WorkflowState.AWAITING_APPROVAL: {WorkflowState.READY, WorkflowState.REJECTED},
    WorkflowState.READY: {WorkflowState.RUNNING},
    WorkflowState.RUNNING: {WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.PAUSED},
    WorkflowState.PAUSED: {WorkflowState.RUNNING, WorkflowState.FAILED},
    WorkflowState.COMPLETED: set(),
    WorkflowState.FAILED: set(),
    WorkflowState.REJECTED: set(),
}


class InvalidTransition(RuntimeError):
    """Raised on an illegal state edge — surfaced, never swallowed."""


def can_transition(frm: WorkflowState, to: WorkflowState) -> bool:
    return to in ALLOWED_TRANSITIONS.get(frm, set())


def assert_transition(frm: WorkflowState, to: WorkflowState) -> None:
    if not can_transition(frm, to):
        raise InvalidTransition(f"illegal transition {frm.value} -> {to.value}")


def _now() -> str:
    """UTC-aware ISO timestamp (the platform standardizes on UTC)."""
    return datetime.now(timezone.utc).isoformat()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    id           TEXT PRIMARY KEY,
    user_id      TEXT,
    instruction  TEXT NOT NULL,
    trigger      TEXT,
    plan_json    TEXT,
    state        TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id       TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS step_results (
    run_id       TEXT NOT NULL,
    step_number  INTEGER NOT NULL,
    tool_name    TEXT NOT NULL,
    args_json    TEXT,
    result_json  TEXT,
    status       TEXT NOT NULL,
    ts           TEXT NOT NULL,
    PRIMARY KEY (run_id, step_number)
);
CREATE TABLE IF NOT EXISTS outbox (
    id           TEXT PRIMARY KEY,
    connector    TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ts           TEXT NOT NULL
);
"""


class WorkflowStore:
    """SQLite-backed persistence + the transition guard. One store per
    process is fine for the demo; the connection is opened with
    check_same_thread=False so the FastAPI layer can share it."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._conn = sqlite3.connect(db_path or config.WORKFLOW_DB_PATH,
                                     check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---------------------------------------------------------- workflows
    def create_workflow(self, user_id: Optional[str], instruction: str,
                        trigger: Optional[str] = None, plan_json: Optional[str] = None,
                        state: WorkflowState = WorkflowState.SUBMITTED) -> str:
        wid = str(uuid.uuid4())
        now = _now()
        self._conn.execute(
            "INSERT INTO workflows (id, user_id, instruction, trigger, plan_json, "
            "state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (wid, user_id, instruction, trigger, plan_json, state.value, now, now),
        )
        self._conn.commit()
        return wid

    def get_workflow(self, workflow_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflows WHERE id = ?", (workflow_id,)
        ).fetchone()
        return dict(row) if row else None

    def set_plan(self, workflow_id: str, plan_json: str) -> None:
        self._conn.execute(
            "UPDATE workflows SET plan_json = ?, updated_at = ? WHERE id = ?",
            (plan_json, _now(), workflow_id),
        )
        self._conn.commit()

    def transition(self, workflow_id: str, to_state: WorkflowState) -> None:
        """Validated state change. Reads the current state, checks the edge
        against ALLOWED_TRANSITIONS, and only then writes."""
        wf = self.get_workflow(workflow_id)
        if wf is None:
            raise KeyError(f"no workflow {workflow_id!r}")
        assert_transition(WorkflowState(wf["state"]), to_state)
        self._conn.execute(
            "UPDATE workflows SET state = ?, updated_at = ? WHERE id = ?",
            (to_state.value, _now(), workflow_id),
        )
        self._conn.commit()

    def list_workflows(self, limit: int = 50) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM workflows ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def workflows_for_trigger(self, trigger: str) -> list[dict]:
        """Workflows whose trigger matches an incoming event, restricted to
        states from which a run can start. This is what the simulated
        webhook (`POST /events/{type}`) dispatches on."""
        rows = self._conn.execute(
            "SELECT * FROM workflows WHERE trigger = ? AND state IN (?, ?)",
            (trigger, WorkflowState.READY.value, WorkflowState.COMPLETED.value),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------- runs
    def start_run(self, workflow_id: str) -> str:
        run_id = str(uuid.uuid4())
        self._conn.execute(
            "INSERT INTO workflow_runs (run_id, workflow_id, started_at, status) "
            "VALUES (?, ?, ?, ?)",
            (run_id, workflow_id, _now(), WorkflowState.RUNNING.value),
        )
        self._conn.commit()
        return run_id

    def checkpoint_step(self, run_id: str, step_number: int, tool_name: str,
                        args: dict, result: object, status: str) -> None:
        """Persist ONE step's outcome and touch the run — in a single
        transaction — before the next step runs. Uses INSERT OR REPLACE so
        a resumed re-run of the same step overwrites cleanly."""
        with self._conn:  # atomic: both statements commit together or not at all
            self._conn.execute(
                "INSERT OR REPLACE INTO step_results "
                "(run_id, step_number, tool_name, args_json, result_json, status, ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_id, step_number, tool_name, json.dumps(args, default=str),
                 json.dumps(result, default=str), status, _now()),
            )
            self._conn.execute(
                "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                (WorkflowState.RUNNING.value, run_id),
            )

    def finish_run(self, run_id: str, status: str) -> None:
        self._conn.execute(
            "UPDATE workflow_runs SET status = ?, finished_at = ? WHERE run_id = ?",
            (status, _now(), run_id),
        )
        self._conn.commit()

    def last_completed_step(self, run_id: str) -> Optional[int]:
        """Highest step_number with status 'ok' — the resume anchor.
        Resume continues from this + 1."""
        row = self._conn.execute(
            "SELECT MAX(step_number) AS n FROM step_results "
            "WHERE run_id = ? AND status = 'ok'", (run_id,)
        ).fetchone()
        return row["n"] if row and row["n"] is not None else None

    def steps_for_run(self, run_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM step_results WHERE run_id = ? ORDER BY step_number", (run_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_run(self, run_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return dict(row) if row else None

    def latest_run(self, workflow_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_runs WHERE workflow_id = ? "
            "ORDER BY started_at DESC LIMIT 1", (workflow_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---------------------------------------------------------- outbox
    def write_outbox(self, connector: str, payload: dict) -> str:
        oid = str(uuid.uuid4())
        self._conn.execute(
            "INSERT INTO outbox (id, connector, payload_json, ts) VALUES (?, ?, ?, ?)",
            (oid, connector, json.dumps(payload, default=str), _now()),
        )
        self._conn.commit()
        return oid

    def outbox(self, limit: int = 50) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM outbox ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def outbox_writer(self) -> Callable[[str, dict], None]:
        """The connectors' injected writer, bound to this store — this is
        what wires Phase 1's connectors to persistence."""
        return lambda connector, payload: self.write_outbox(connector, payload)
