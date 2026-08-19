# =============================================================
# REPORTS CATALOG — named, parameterized, code-authored SQL
# =============================================================
# Requirement #4, realized: a recurring report must NOT re-invoke the
# LLM every night to regenerate the same SQL. So the standard reports
# live here as fixed, human-authored, parameterized SQL over the SAME
# two allowlisted tables the BI surface uses. The only thing that
# changes per run is the time window — a runtime parameter, not a new
# query.
#
# The window is bound by CODE-GENERATED timestamp literals, never user
# or LLM text: run_validated_report parses window_start/window_end as
# real datetimes (junk raises, so nothing arbitrary can reach the SQL),
# renders them as TO_TIMESTAMP_NTZ() literals, and then RE-VALIDATES
# the fully-substituted SQL through the exact same AST guard the BI
# page uses. Two validations bracket the substitution: the template is
# vetted, and the final executed SQL is vetted again. The guard, not
# this file, is load-bearing.
#
# Open-ended scheduled questions that don't match a named report fall
# back to persist-and-replay: the once-validated LLM SQL is stored as
# a template and re-run verbatim (documented in run_report_query's
# tools_bridge wrapper). Named reports are preferred because they are
# deterministic and parameterized; the LLM only MAPS intent onto them.
# =============================================================

from __future__ import annotations

from datetime import datetime
from typing import Callable, Optional

# Named templates use :window_start / :window_end placeholders — the
# colon form sqlglot parses cleanly (pyformat %(name)s does not). The
# LEFT JOIN to features supplies the ground-truth fraud label and the
# pattern; MODE() picks the most frequent identified pattern in one
# row so the whole report is a single-row set of named aggregates
# (which report.build_report turns straight into labeled metrics).
FRAUD_PERFORMANCE_REPORT = """
SELECT
    COUNT(*)                                             AS transactions_reviewed,
    COUNT_IF(d.decision = 'BLOCK')                       AS block_count,
    COUNT_IF(d.decision = 'ESCALATE')                    AS escalate_count,
    COUNT_IF(d.decision = 'ALLOW')                       AS allow_count,
    ROUND(COUNT_IF(f.is_synthetic_fraud) / NULLIF(COUNT(*), 0), 4) AS fraud_rate,
    MODE(d.identified_pattern)                           AS top_fraud_pattern
FROM DECISIONS.FACT_DECISIONS d
LEFT JOIN FEATURES.FACT_FEATURE_SNAPSHOTS f
    ON d.snapshot_id = f.snapshot_id
WHERE d.decided_at >= :window_start
  AND d.decided_at <  :window_end
""".strip()


# name -> (title, parameterized SQL). The title is what the ReportResult
# is headed with; the SQL is what run_validated_report executes.
REPORT_CATALOG: dict[str, tuple[str, str]] = {
    "fraud_performance": ("Daily Fraud Operations Report", FRAUD_PERFORMANCE_REPORT),
}


class MissingParameter(ValueError):
    """A required runtime window parameter was absent or unparseable."""


def _as_ts_literal(value, name: str) -> str:
    """Coerce a window bound to a Snowflake TIMESTAMP_NTZ literal built
    from a REAL datetime. A string must be ISO-8601; anything else
    raises MissingParameter — so only a validated datetime, formatted
    by code, is ever spliced into the SQL. No user/LLM text reaches it."""
    if value is None or value == "":
        raise MissingParameter(f"missing runtime parameter {name!r}")
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except ValueError as e:
            raise MissingParameter(f"parameter {name!r} is not an ISO datetime: {value!r}") from e
    # NTZ: drop any tzinfo, second precision — the value is code-owned.
    iso = dt.replace(tzinfo=None).isoformat(timespec="seconds")
    return f"TO_TIMESTAMP_NTZ('{iso}')"


def _default_validator():
    from fraud_platform.bi_dashboard import config as bi_config
    from fraud_platform.bi_dashboard.sql_guard import SQLValidator
    return SQLValidator(allowed_tables=bi_config.BI_ALLOWED_TABLES, max_rows=bi_config.BI_MAX_ROWS)


def run_validated_report(
    sql_template: str,
    window_start=None,
    window_end=None,
    validator=None,
    connect: Optional[Callable] = None,
) -> dict:
    """Execute a parameterized report template for one time window.

    Pipeline (two validations bracketing a code-only substitution):
      1. validate the TEMPLATE structurally (SELECT-only, allowlist);
      2. substitute :window_start/:window_end with code-generated
         TIMESTAMP literals;
      3. RE-VALIDATE the fully-substituted SQL through the same guard;
      4. execute via the least-privilege BI connection.

    `validator` and `connect` are injected so this is unit-testable
    with a fake connection and no Snowflake. Returns
    {report_sql (template), sql (executed), columns, rows}."""
    validator = validator or _default_validator()

    # 1. the template must itself be a legal read (structure is fixed).
    validator.validate(sql_template)

    # 2. code-only window substitution — ONLY for the placeholders the
    #    template actually contains. A parameterized report requires its
    #    window (a missing one raises); a placeholder-free stored template
    #    (immediate one-shot delivery) runs as-is with no window needed.
    concrete = sql_template
    if ":window_start" in sql_template:
        concrete = concrete.replace(":window_start", _as_ts_literal(window_start, "window_start"))
    if ":window_end" in sql_template:
        concrete = concrete.replace(":window_end", _as_ts_literal(window_end, "window_end"))

    # 3. re-validate the ACTUAL SQL that will run — the guard is the
    #    thing that's trusted, so the executed statement passes it too.
    safe_sql = validator.validate(concrete)

    # 4. execute under BI_ROLE (secondary roles off), same door as the BI page.
    conn = connect() if connect is not None else _open_bi()
    try:
        cur = conn.cursor()
        try:
            cur.execute(safe_sql)
            columns = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()
    return {"report_sql": sql_template, "sql": safe_sql, "columns": columns, "rows": rows}


def _open_bi():
    from fraud_platform.bi_dashboard.connection import open_bi_connection
    return open_bi_connection()
