# =============================================================
# SQL GUARD — AST-based validation of LLM-generated SQL
# =============================================================
# Replaces the original regex/string guardrail in nl2sql_agent.py.
# The trust model is unchanged and worth restating, because it is
# WHY this file exists: the LLM is an UNTRUSTED SQL author. The
# prompt asks for good behavior (a quality measure); this validator
# ENFORCES it (the security boundary). Only the validator is
# load-bearing — same judgment-from-models, invariants-from-code
# split as the Phase 4 router guardrails and the Phase 5
# governance rules.
#
# Why AST, not regex — the concrete failure of the old approach:
#   A regex denylist of keywords is a blocklist, and blocklists
#   lose. `SELECT ... FROM IDENTIFIER('RAW.FACT_TRANSACTIONS')`
#   reaches the PII table with no forbidden keyword in sight;
#   `SELECT ... UNION SELECT ... FROM RAW...` hides the exfil in a
#   second branch; a table function `TABLE(FLATTEN(...))` is a data
#   source the keyword scan never modeled. Parsing to an AST and
#   walking it turns the guard into an ALLOWLIST over query
#   STRUCTURE — every table reference, in any branch or subquery,
#   is enumerated and checked, and anything the grammar can't be
#   parsed into is rejected outright. We validate what the query
#   IS, not what strings it happens to contain.
#
# A second, quieter benefit: we execute the RE-SERIALIZED AST
# (validator returns node.sql()), not the raw LLM text. Comment
# tricks, odd whitespace, and unicode lookalikes are normalized
# away by the parse/generate round trip before the query ever
# reaches Snowflake.
# =============================================================

import logging
from typing import List

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class QueryRejected(Exception):
    """
    Raised when generated SQL fails validation. A distinct type
    (kept here, the guard's home, and re-exported by nl2sql_agent)
    so the UI can render "the platform refused this" differently
    from a Snowflake execution error — a rejection is policy
    working, not a bug.
    """


# Statement node types that are permitted as the single top-level
# node. Everything else — Update, Insert, Delete, Merge, Create,
# Drop, Alter, Command (USE/CALL/COPY/GRANT/…), TruncateTable — is
# rejected by absence. An allowlist of shapes, not a denylist of
# keywords: a new destructive statement type Snowflake adds next
# year is rejected automatically because it simply isn't in here.
_ALLOWED_TOP_LEVEL = (exp.Select, exp.Union, exp.Except, exp.Intersect)

# Function names that, used anywhere, defeat static table analysis:
# IDENTIFIER() resolves a relation from a runtime string (so the
# real target is invisible to the AST), TABLE() wraps a table
# function as a data source. Both are rejected outright.
_FORBIDDEN_FUNCTIONS = {"IDENTIFIER", "TABLE"}

_INFORMATION_SCHEMA = "INFORMATION_SCHEMA"


class SQLValidator:
    """
    Stateless, dependency-free SQL policy check. Constructed with
    the allowlist and row cap (injected from config, not read from
    it here) so tests — and any future per-role policy — can build
    a stricter validator without touching module globals. No I/O:
    it never connects to anything, which is what lets the bypass
    suite run in CI with no credentials.
    """

    def __init__(
        self,
        allowed_tables: List[str],
        max_rows: int,
        dialect: str = "snowflake",
    ):
        self._max_rows = max_rows
        self._dialect = dialect
        # Normalize the allowlist to upper-case "SCHEMA.TABLE" keys —
        # Snowflake identifiers are case-insensitive unless quoted,
        # and every comparison below is done upper-cased so casing
        # can never be a bypass.
        self._allowed = set()
        for t in allowed_tables:
            parts = t.upper().split(".")
            if len(parts) != 2:
                raise ValueError(
                    f"allowed_tables entries must be SCHEMA.TABLE, got {t!r}"
                )
            self._allowed.add((parts[0], parts[1]))

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    def validate(self, sql: str) -> str:
        """
        Vet `sql` and return a safe, canonical, row-capped statement
        to execute. Raises QueryRejected (with a human-readable
        reason) on any violation. Fails CLOSED: anything that does
        not parse, or parses into a shape not explicitly permitted,
        is rejected.
        """
        if not sql or not sql.strip():
            raise QueryRejected("The agent produced no SQL for this question.")

        # 1. Parse. A parse failure is a rejection, not an excuse to
        #    fall back to string handling — if we can't understand
        #    the query's structure, we don't run it.
        try:
            statements = [s for s in sqlglot.parse(sql, read=self._dialect) if s is not None]
        except Exception as e:
            raise QueryRejected(f"Could not parse SQL: {e}")

        # 2. Exactly one statement — kills `SELECT ...; DROP ...`
        #    stacking at the structural level, not by hunting for
        #    semicolons (which appear legitimately inside string
        #    literals too).
        if len(statements) != 1:
            raise QueryRejected(
                f"Exactly one statement allowed; found {len(statements)}."
            )
        stmt = statements[0]

        # 3. Top-level shape must be a read.
        if not isinstance(stmt, _ALLOWED_TOP_LEVEL):
            raise QueryRejected(
                f"Only SELECT/CTE/set-operation queries are allowed "
                f"(got {type(stmt).__name__.upper()})."
            )

        # 4. No runtime-resolved relations or table functions anywhere.
        for fn in stmt.find_all(exp.Anonymous, exp.Func):
            name = (fn.name or getattr(fn, "sql_name", lambda: "")()) or ""
            if name.upper() in _FORBIDDEN_FUNCTIONS:
                raise QueryRejected(
                    f"Forbidden construct: {name.upper()}() cannot be used "
                    f"(dynamic identifiers and table functions are disallowed)."
                )

        # 5. Every table reference — in any branch, join, subquery, or
        #    CTE body — must be a fully-qualified, allowlisted relation
        #    (or a reference to a CTE defined in this same query).
        cte_names = {c.alias.upper() for c in stmt.find_all(exp.CTE) if c.alias}
        for table in stmt.find_all(exp.Table):
            self._check_table(table, cte_names)

        # 6. Clamp the row cap — never trust the LLM's LIMIT.
        capped = self._apply_row_cap(stmt)

        safe_sql = capped.sql(dialect=self._dialect)
        logger.info("SQL validated and capped:\n%s", safe_sql)
        return safe_sql

    # ----------------------------------------------------------
    # INTERNALS
    # ----------------------------------------------------------
    def _check_table(self, table: exp.Table, cte_names: set) -> None:
        name = (table.name or "").upper()
        schema = (table.db or "").upper()
        catalog = (table.catalog or "").upper()

        # Stage references (@stage) and table-function residue parse
        # to a Table with an empty or @-prefixed name — neither is a
        # real relation we allow as a source.
        if not name or name.startswith("@"):
            raise QueryRejected("Stage or table-function sources are not allowed.")

        # A bare CTE reference (no schema, name matches a WITH alias)
        # is legitimate — it resolves to an already-validated subquery,
        # not an external relation.
        if not schema and name in cte_names:
            return

        if not schema:
            raise QueryRejected(
                f"Unqualified relation '{name}' — every table must be "
                f"written as SCHEMA.TABLE."
            )

        if schema == _INFORMATION_SCHEMA:
            raise QueryRejected("INFORMATION_SCHEMA access is not allowed.")

        # If a database (catalog) part is present, it must be the one
        # our allowlisted schemas actually live in — otherwise a
        # right-shaped SCHEMA.TABLE in the WRONG database would pass.
        if catalog and catalog != "FRAUD_DETECTION":
            raise QueryRejected(
                f"Relation in non-allowlisted database '{catalog}'."
            )

        if (schema, name) not in self._allowed:
            raise QueryRejected(
                f"Relation {schema}.{name} is not in the allowlist "
                f"{sorted('.'.join(a) for a in self._allowed)}."
            )

    def _apply_row_cap(self, stmt: exp.Expression) -> exp.Expression:
        """
        Enforce max_rows. If the query has no LIMIT, add one; if it
        asks for MORE than the cap, clamp it down; if it asks for
        fewer, respect that. Item 4 of the hardening spec: the
        configured cap wins over whatever the LLM wrote.

        `.limit()` on a sqlglot Query sets or replaces the LIMIT and
        works uniformly for a plain SELECT and for a set operation
        (the cap then binds to the whole UNION result), so no
        special-casing per shape is needed.
        """
        existing = stmt.args.get("limit")
        if existing is not None:
            try:
                current = int(existing.expression.name)
            except (AttributeError, ValueError):
                current = None
            if current is not None and current <= self._max_rows:
                return stmt  # user asked for fewer rows — honor it
        return stmt.limit(self._max_rows)
