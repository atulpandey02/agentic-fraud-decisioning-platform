# =============================================================
# LANGSMITH CONFIG — dev-time LLM telemetry, opt-in by env
# =============================================================
# Why LangSmith AND FACT_AGENT_TRACES, not one or the other:
#   They answer different questions for different audiences.
#   FACT_AGENT_TRACES (audit_logger.py) is the platform's OWN
#   record — which agent acted, in what order, saying what —
#   stored next to the decisions it explains, queryable with the
#   same SQL, owned outright. LangSmith captures what our own
#   log deliberately does not: the full prompt/response of every
#   LLM call, per-call token counts and latency, retries — the
#   raw material for debugging prompt quality. Compliance asks
#   "who decided and why": Snowflake. Engineering asks "why was
#   that call slow/wrong": LangSmith. Persisting full prompts to
#   the audit table would bloat it with vendor-format payloads a
#   reviewer never reads; skipping LangSmith would mean rebuilding
#   per-call capture by hand for zero benefit during development.
#
# Why env vars and not an SDK call: LangChain's tracer activates
# by READING the environment at call time — there is no
# `enable_tracing()` API to call. This helper exists so exactly
# one place sets the right variables (and both naming families,
# LANGSMITH_* and legacy LANGCHAIN_*, which differ across
# langchain-core versions) instead of every runner exporting
# shell variables and drifting.
# =============================================================

import os
import logging

from . import config

logger = logging.getLogger(__name__)


def configure_langsmith(project_name: str = None) -> bool:
    """
    Enable LangSmith tracing for this process if an API key is
    configured; no-op (returning False) otherwise. Must run BEFORE
    the first LLM client is created — the tracer wraps calls as
    they happen; it cannot retroactively capture earlier ones.

    Graceful degradation is deliberate: tracing is a development
    aid, and a missing key should never break a decision run. The
    durable audit trail does not depend on this in any way.
    """
    if not config.LANGSMITH_API_KEY:
        logger.info("LANGSMITH_API_KEY not set — LangSmith tracing disabled (audit trail unaffected)")
        return False

    project = project_name or config.LANGSMITH_PROJECT

    # Both naming families: langchain-core 0.3 read LANGCHAIN_*,
    # 1.x prefers LANGSMITH_* — setting both costs four env vars
    # and removes a whole class of "tracing silently off" surprises.
    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGSMITH_API_KEY"] = config.LANGSMITH_API_KEY
    os.environ["LANGCHAIN_API_KEY"] = config.LANGSMITH_API_KEY
    os.environ["LANGSMITH_PROJECT"] = project
    os.environ["LANGCHAIN_PROJECT"] = project

    logger.info(f"LangSmith tracing enabled — project '{project}'")
    return True
