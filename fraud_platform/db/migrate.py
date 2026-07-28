# =============================================================
# MIGRATION RUNNER — versioned, idempotent, tracked
# =============================================================
# Priority 2 item 1. Applies snowflake/migrations/VNNN__*.sql in
# order, records each in FRAUD_DETECTION.PUBLIC.SCHEMA_MIGRATIONS,
# and skips anything already applied. Re-running is a no-op — the
# tracking table AND the migrations' own IF-NOT-EXISTS / idempotent
# DDL both guarantee it, belt and suspenders.
#
# Model: snowflake/schema.sql is the fresh-database BASELINE (run
# once). This runner owns the INCREMENTAL changes after that. The
# two are kept from drifting by tests/test_schema_contract.py, which
# checks the schema against db/schema_contract.py, and by the
# migrations being small and reviewable.
#
# ORDER on a fresh account: schema.sql -> rbac.sql -> this runner.
# rbac.sql must precede the migrations because V002/V004 GRANT
# privileges to PIPELINE_ROLE/AGENT_ROLE/BI_ROLE and fail with
# "Role does not exist" if those roles haven't been created yet.
#
# Split into pure functions (discover / pending — unit-tested with
# no database) and the I/O apply step (verified live), so the part
# that decides WHAT to run is testable without credentials.
#
# Lives in db/ (not snowflake/) on purpose: a module under a dir
# named `snowflake/` would shadow snowflake-connector-python.
# =============================================================

import os
import re
import sys
import logging
from typing import List, Tuple, Set

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("migrate")

# fraud_platform/db/migrate.py -> the SQL migration files live at the
# repo root under snowflake/migrations (SQL, not Python, so kept
# outside the package tree) — two levels up from this module.
_MIGRATIONS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "snowflake", "migrations")
)
_MIGRATION_RE = re.compile(r"^V(\d+)__([A-Za-z0-9_]+)\.sql$")

_TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS FRAUD_DETECTION.PUBLIC.SCHEMA_MIGRATIONS (
    version     INTEGER      NOT NULL,
    filename    VARCHAR(200) NOT NULL,
    applied_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (version)
)
"""


# -------------------------------------------------------------
# PURE — testable without a database
# -------------------------------------------------------------
def discover_migrations(migrations_dir: str = _MIGRATIONS_DIR) -> List[Tuple[int, str, str]]:
    """
    Return [(version, filename, fullpath)] sorted by version. Raises
    on a duplicate version number — two migrations claiming the same
    slot is a merge accident that must fail loudly, not apply one
    arbitrarily.
    """
    found = []
    seen = set()
    for name in sorted(os.listdir(migrations_dir)):
        m = _MIGRATION_RE.match(name)
        if not m:
            continue
        version = int(m.group(1))
        if version in seen:
            raise ValueError(f"Duplicate migration version {version:03d} ({name})")
        seen.add(version)
        found.append((version, name, os.path.join(migrations_dir, name)))
    return sorted(found, key=lambda t: t[0])


def pending(all_migrations: List[Tuple[int, str, str]], applied: Set[int]) -> List[Tuple[int, str, str]]:
    """The migrations not yet applied, in ascending version order."""
    return [m for m in all_migrations if m[0] not in applied]


# -------------------------------------------------------------
# I/O — verified against the live database
# -------------------------------------------------------------
def _connect():
    # Imported lazily so the pure functions above can be unit-tested
    # without snowflake-connector or a .env present.
    from dotenv import load_dotenv
    import snowflake.connector

    load_dotenv()
    role = os.getenv("MIGRATION_SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"))
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DETECTION"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=role,
    )


def applied_versions(conn) -> Set[int]:
    conn.execute_string(_TRACKING_DDL)
    cur = conn.cursor()
    try:
        cur.execute("SELECT version FROM FRAUD_DETECTION.PUBLIC.SCHEMA_MIGRATIONS")
        return {int(r[0]) for r in cur.fetchall()}
    finally:
        cur.close()


def apply_all(conn, migrations_dir: str = _MIGRATIONS_DIR, dry_run: bool = False) -> List[int]:
    """
    Apply every pending migration in order. Each migration runs as its
    own unit and is recorded only after its statements succeed, so a
    failure stops the run with a clear "applied up to N" state rather
    than a half-recorded mess. Returns the versions applied this run
    (empty when everything was already applied — the idempotent case).
    """
    all_m = discover_migrations(migrations_dir)
    done = applied_versions(conn)
    todo = pending(all_m, done)

    if not todo:
        logger.info("No pending migrations — schema is up to date (%d applied).", len(done))
        return []

    applied_now = []
    for version, filename, path in todo:
        with open(path) as f:
            sql = f.read()
        logger.info("%s migration V%03d (%s)", "Would apply" if dry_run else "Applying", version, filename)
        if dry_run:
            continue
        # execute_string runs all statements in the file; the Snowflake
        # connector splits on top-level semicolons for us.
        conn.execute_string(sql)
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO FRAUD_DETECTION.PUBLIC.SCHEMA_MIGRATIONS (version, filename) "
                "VALUES (%(v)s, %(f)s)",
                {"v": version, "f": filename},
            )
            conn.commit()
        finally:
            cur.close()
        applied_now.append(version)
        logger.info("Applied V%03d.", version)

    return applied_now


def main():
    dry_run = "--dry-run" in sys.argv
    conn = _connect()
    try:
        applied = apply_all(conn, dry_run=dry_run)
        logger.info("Migration run complete. Applied this run: %s", applied or "none")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
