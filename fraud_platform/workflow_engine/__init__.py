# =============================================================
# WORKFLOW ENGINE — natural-language workflow automation
# =============================================================
# A vendor-facing automation layer built ON TOP of the fraud
# platform. A fraud-ops user describes an automation in plain
# English; the system checks feasibility, decomposes it into an
# ordered plan, executes each step through a tool registry,
# persists state, and reports results.
#
# Pattern: PLAN-AND-EXECUTE — the whole plan is produced up front,
# then executed without re-planning. This is deliberately a THIRD
# agent pattern next to the platform's existing ReAct single agent
# (interleaved think/act) and supervisor multi-agent (dynamic
# routing). Three patterns in one repo; the contrast is the point.
# =============================================================
