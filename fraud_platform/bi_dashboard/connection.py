# =============================================================
# BI DATABASE ACCESS — least-privilege connection, verified
# =============================================================
# The BI application's ONLY door to Snowflake. It exists because
# "connect with role=BI_ROLE" is NOT, by itself, a security
# boundary on this account — a fact established with live command
# output during Priority 1:
#
#   connect(role="BI_ROLE") -> CURRENT_ROLE() == BI_ROLE, but
#   CURRENT_SECONDARY_ROLES() == ALL, which still included the
#   operator's ACCOUNTADMIN. RAW.FACT_TRANSACTIONS (the PII table
#   BI_ROLE has no grant on) was readable through that secondary
#   role. Setting the primary role restricted nothing.
#
# So this helper does three things the raw connector will not:
#   1. Refuses to run under an admin role (defense against a
#      misconfigured BI_SNOWFLAKE_ROLE) — fail loud, never silently
#      hand the NL2SQL surface superuser rights.
#   2. Disables secondary roles for the session (USE SECONDARY
#      ROLES NONE), collapsing the session's authority to exactly
#      BI_ROLE's grants.
#   3. VERIFIES the resulting session really is single-role BI_ROLE
#      before returning the connection — a startup assertion, so a
#      future Snowflake default change that re-enables secondary
#      roles breaks CI, not production silently.
# =============================================================

import logging

import snowflake.connector

from . import config

logger = logging.getLogger(__name__)


class InsecureConnectionError(RuntimeError):
    """
    Raised when a BI connection cannot be established at the
    intended least privilege. A hard startup failure, deliberately:
    a BI app that cannot prove it is confined to BI_ROLE must not
    run at all — degrading to "whatever role we got" is exactly the
    silent-over-privilege this whole module exists to prevent.
    """


def assert_role_allowed(role: str) -> None:
    """
    Pure guard: reject an empty/admin role BEFORE opening any
    connection. Separated from open_bi_connection() so the refusal
    logic is unit-testable without a live Snowflake (the network
    call is not the part that can be wrong here — the policy is).
    """
    if not role or not role.strip():
        raise InsecureConnectionError("BI_SNOWFLAKE_ROLE is empty — refusing to connect.")
    if role.strip().upper() in config.BI_FORBIDDEN_ROLES:
        raise InsecureConnectionError(
            f"BI application refuses to run as '{role}': it is an "
            f"administrative role. The BI surface must use a read-only, "
            f"least-privilege role (BI_ROLE). Fix BI_SNOWFLAKE_ROLE."
        )


def open_bi_connection():
    """
    Open a Snowflake connection confined to BI_ROLE, or raise
    InsecureConnectionError. This is the only sanctioned way the
    BI application reaches the database.

    Note it does NOT read config.SNOWFLAKE_ROLE — that shared
    setting still defaults to ACCOUNTADMIN for the pipeline/agent
    paths, and letting it leak into the BI path is precisely the
    "ACCOUNTADMIN default in an application path" the hardening
    plan calls out. The BI role comes from its own dedicated
    setting, defaulting to BI_ROLE.
    """
    role = config.BI_SNOWFLAKE_ROLE
    assert_role_allowed(role)

    conn = snowflake.connector.connect(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        database=config.SNOWFLAKE_DATABASE,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        role=role,
    )

    try:
        cur = conn.cursor()
        # Collapse authority to the single primary role. Without this,
        # the operator's other granted roles ride along as secondary
        # roles and the least-privilege intent is fiction.
        cur.execute("USE SECONDARY ROLES NONE")

        cur.execute("SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()")
        current_role, secondary = cur.fetchone()

        if (current_role or "").upper() != role.strip().upper():
            raise InsecureConnectionError(
                f"Expected to run as {role}, but the session's role is "
                f"{current_role!r}."
            )
        # CURRENT_SECONDARY_ROLES() returns a JSON blob; an empty
        # value string means no secondary roles are active.
        if secondary and '"value":"ALL"' in secondary.replace(" ", ""):
            raise InsecureConnectionError(
                "Secondary roles are still ALL after USE SECONDARY ROLES "
                "NONE — refusing to run over-privileged."
            )
        cur.close()
    except Exception:
        conn.close()
        raise

    logger.info("BI connection established as %s with secondary roles disabled", role)
    return conn
