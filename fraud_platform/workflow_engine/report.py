# =============================================================
# REPORT — a structured ReportResult + deterministic formatting
# =============================================================
# The rule: NEVER send raw SQL tuples to Slack/email. A query step
# returns columns + rows; this module turns that into a structured
# ReportResult and renders it two ways — a COMPACT summary for Slack
# and a DETAILED body for email. The formatting is deterministic
# code, not an LLM: the same numbers produce the same report every
# night, and the audit line ("SQL used") is always carried through.
#
# `format_report` is the workflow TOOL body. It takes the previous
# step's result dict ({columns, rows, sql}) and returns a plain dict
# (JSON-serializable, so it flows through the executor + outbox)
# containing the ReportResult fields PLUS pre-rendered `slack_text`
# and `email_subject`/`email_body`. The notify tools stay unchanged —
# they still take a text string; the report just supplies it.
# =============================================================

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

# Rows beyond this are summarized, not dumped — a report is a summary,
# not a data export (the BI page is where you go for the full table).
_MAX_REPORT_ROWS = 50


@dataclass
class ReportResult:
    """The structured result of a report query. `metrics` is the
    single-row case (label -> value); `rows`/`columns` carry the
    tabular case. `sql_used` is the audit trail — always present."""
    title: str
    summary: str
    metrics: dict
    columns: list
    rows: list
    generated_at: str
    sql_used: Optional[str] = None
    row_count: int = 0


def _humanize(col: str) -> str:
    return col.replace("_", " ").strip().capitalize()


def _fmt_value(col: str, value) -> str:
    """Deterministic display formatting. A '*rate*' float renders as a
    percentage; other numbers get thousands separators. No locale, no
    surprises — the report reads the same on every machine."""
    if value is None:
        return "—"
    low = col.lower()
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        if "rate" in low or "pct" in low or "percent" in low or "share" in low:
            return f"{value * 100:.2f}%"
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def build_report(title: str, data: dict) -> ReportResult:
    """Turn a query step's {columns, rows, sql} into a ReportResult.
    Single-row results become labeled metrics; multi-row results keep
    the (capped) table and a row-count summary."""
    columns = list(data.get("columns") or [])
    rows = list(data.get("rows") or [])
    sql_used = data.get("sql")
    generated_at = datetime.now(timezone.utc).isoformat()
    row_count = len(rows)

    metrics: dict = {}
    lines: list[str] = []

    if row_count == 1 and columns:
        # the named-report shape: one row of named aggregates -> metrics
        row = rows[0]
        for col, val in zip(columns, row):
            metrics[col] = val
            lines.append(f"{_humanize(col)}: {_fmt_value(col, val)}")
        summary = "\n".join(lines)
    elif row_count == 0:
        summary = "No rows matched the report window."
    else:
        metrics = {"row_count": row_count}
        summary = f"{row_count:,} rows over the report window."

    return ReportResult(
        title=title, summary=summary, metrics=metrics,
        columns=columns, rows=rows[:_MAX_REPORT_ROWS],
        generated_at=generated_at, sql_used=sql_used, row_count=row_count,
    )


def render_slack(report: ReportResult) -> str:
    """COMPACT: title + the summary lines. Slack messages are read at a
    glance, so no table, no SQL."""
    return f"*{report.title}*\n{report.summary}"


def render_email(report: ReportResult) -> tuple[str, str]:
    """DETAILED: subject + a body that includes the summary, a small
    table for multi-row results, and the audited SQL. Returns
    (subject, body)."""
    subject = report.title
    parts = [report.title, "", report.summary]

    # a compact fixed-width table for the multi-row case
    if report.row_count > 1 and report.columns:
        parts += ["", " | ".join(str(c) for c in report.columns)]
        for r in report.rows:
            parts.append(" | ".join("—" if v is None else str(v) for v in r))
        if report.row_count > len(report.rows):
            parts.append(f"... ({report.row_count - len(report.rows)} more rows)")

    parts += ["", f"Generated at: {report.generated_at}"]
    if report.sql_used:
        parts += ["", "SQL used:", report.sql_used]
    return subject, "\n".join(parts)


def format_report(title: str, data: dict) -> dict:
    """WORKFLOW TOOL body. Build the ReportResult and pre-render both
    delivery forms, so a downstream Slack step reads `$step.slack_text`
    and an email step reads `$step.email_subject` / `$step.email_body`
    — the connectors stay text-in and unchanged."""
    report = build_report(title, data)
    subject, body = render_email(report)
    out = asdict(report)
    out["slack_text"] = render_slack(report)
    out["email_subject"] = subject
    out["email_body"] = body
    return out
