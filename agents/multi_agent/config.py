# =============================================================
# MULTI-AGENT CONFIGURATION — Phase 4
# =============================================================
import os
import importlib.util

# -------------------------------------------------------------
# CONFIG LAYERING — re-export Phase 3's config, don't copy it
#
# This phase reuses Phase 3's connection settings (Groq, Redis,
# Snowflake, Weaviate) and its tool implementations. Copying the
# constants here would work today and silently drift tomorrow —
# the exact failure window_config.py exists to prevent on the
# streaming side. But a plain `import config` can't reach the
# Phase 3 file either: both phases name their module `config`
# (the established one-config-py-per-phase convention), and
# Python caches modules by NAME, so whichever loads first wins —
# the same collision tools.py already had to solve against
# retrieval/config.py.
#
# importlib sidesteps the name entirely: load Phase 3's config
# by FILE PATH under a private, unique module name, then lift
# its UPPERCASE constants into this namespace. Downstream code
# does `import config` and sees one flat namespace; where a
# value actually lives is this file's implementation detail.
# Phase 3's config also calls load_dotenv() at import, so
# executing it here loads the repo-root .env as a side effect —
# no second load needed.
# -------------------------------------------------------------
_PHASE3_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "single_agent", "config.py")
)


def _load_config_by_path(path: str, unique_name: str):
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_phase3 = _load_config_by_path(_PHASE3_CONFIG_PATH, "_single_agent_config")

# Lift every UPPERCASE name — the constants convention makes
# "public config value" mechanically detectable, so new Phase 3
# settings propagate here automatically instead of requiring a
# matching edit in two files.
globals().update({k: v for k, v in vars(_phase3).items() if k.isupper()})

# -------------------------------------------------------------
# ORCHESTRATION
# -------------------------------------------------------------
ORCHESTRATOR_MAX_STEPS = 6  # hard cap on router turns — same job as
                            # Phase 3's MAX_REASONING_STEPS, one level
                            # up: a confused router must not bounce
                            # between specialists forever. 6 allows
                            # every specialist once (4) plus slack for
                            # a routing correction, nothing more.

# The four specialists the orchestrator can route to. decision_agent
# is deliberately last-resort-and-terminal: routing there ends the
# run, so the router must not reach for it until the evidence the
# other specialists produce actually exists.
SPECIALIST_AGENTS = [
    "feature_agent",
    "risk_agent",
    "policy_agent",
    "decision_agent",
]
