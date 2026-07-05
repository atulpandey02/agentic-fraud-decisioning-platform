# =============================================================
# OBSERVABILITY CONFIGURATION — Phase 6
# =============================================================
import os
import importlib.util

# -------------------------------------------------------------
# CONFIG LAYERING — third layer of the chain established in
# Phase 4: observability ⊃ governance ⊃ multi_agent ⊃
# single_agent. Load the layer directly beneath by file path,
# lift its UPPERCASE constants; everything transitively included.
# -------------------------------------------------------------
_PHASE5_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "governance", "config.py")
)


def _load_config_by_path(path: str, unique_name: str):
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_phase5 = _load_config_by_path(_PHASE5_CONFIG_PATH, "_governance_config")
globals().update({k: v for k, v in vars(_phase5).items() if k.isupper()})

# -------------------------------------------------------------
# LANGSMITH
# Dev-time tracing only — see langsmith_config.py for the
# LangSmith-vs-FACT_AGENT_TRACES division of labor.
# -------------------------------------------------------------
LANGSMITH_API_KEY = os.getenv("LANGSMITH_API_KEY")
LANGSMITH_PROJECT = os.getenv("LANGSMITH_PROJECT", "fraud-decisioning-platform")

# -------------------------------------------------------------
# EVALUATION
# -------------------------------------------------------------
EVAL_SAMPLE_SIZE = 6          # total transactions per eval run — deliberately
                              # small: each one costs ~5 Groq calls plus a
                              # judge call, and the point of THIS phase is a
                              # working measurement pipeline, not a big-N
                              # benchmark. Scale the N once the pipeline is
                              # trusted, not before.
EVAL_STRATIFY = True          # half fraud / half legit. A random draw from
                              # FACT_FEATURE_SNAPSHOTS' flagged rows is ~2/3
                              # legitimate (the hard-rule over-flagging noted
                              # in PROJECT_STATUS.md), so an unstratified
                              # sample would mostly measure false-positive
                              # handling and barely touch true-fraud recall.

# LLM-as-judge reuses the same Groq model as the agents. Honest
# limitation, documented rather than hidden: a same-family judge
# can exhibit self-preference bias. In production you'd use a
# stronger or at least different model family for judging; here
# the judge's value is the PIPELINE (scores + notes persisted per
# decision), which is model-swappable via this one constant.
JUDGE_MODEL_NAME = os.getenv("JUDGE_MODEL", os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile"))
JUDGE_TEMPERATURE = 0.0       # zero, unlike the agents' 0.1 — the judge makes
                              # no tool calls (the reliability reason agents
                              # avoid zero), and scoring should be maximally
                              # repeatable run to run
