# Pattern Identification Accuracy Investigation

*2026-07-05. Companion to GEO_FLAGGING_INVESTIGATION.md — three of the four
mislabels below trace to the same geo corruption that document fixes.*

## Symptom

Phase 6's first eval batch: the agent's `identified_pattern` matched ground
truth on only **2 of 6** transactions, against 5/6 decision accuracy (of
non-escalated) and a 0.88 mean judge score. The agents decide well but
attribute poorly.

## The four misses, individually diagnosed

| truth | agent said | diagnosis |
|---|---|---|
| FRAUD / NEW_DEVICE | AMOUNT_ANOMALY | **Overlap by construction** — the generator's NEW_DEVICE fraud is "new device + 3× average amount" (`NEW_DEVICE_HIGH_AMOUNT_MULTIPLIER = 3.0`), which necessarily elevates `amount_zscore` too. AMOUNT_ANOMALY is a *defensible* reading of the same evidence; exact-match scoring calls it wrong. |
| legit / NONE | GEO_JUMP | **Corrupted geo feature** — a poisoned/out-of-order `geo_distance_km` (5,000 km-scale) handed to an agent with *no time context*. The agent read the evidence it was given honestly. |
| legit / NONE | GEO_JUMP (escalated) | Same as above. |
| legit / NONE | GEO_JUMP,NEW_DEVICE (escalated) | Same, plus the agent emitted a multi-label string, which exact-match can never score. |

## Root causes

1. **Upstream feature corruption (primary).** Fixed at the source in
   GEO_FLAGGING_INVESTIGATION.md — for *new* data. Historical snapshots keep
   their corrupted geo values, so agents evaluating historical rows still see
   them; cause 2 is what lets agents defend themselves against that.
2. **Input gap: agents never see `time_since_last_txn_min`.** The pipeline
   computes it, FACT_FEATURE_SNAPSHOTS stores it, and no fetch selects it. A
   GEO_JUMP judgment is a *speed* judgment (distance over time); the agents
   were structurally unable to make it — 1,335 km reads as a jump when you
   can't see it happened over three days (or over *negative* time, the
   out-of-order tell).
3. **Patterns overlap by construction, but eval demands exact match.**
   NEW_DEVICE fraud elevates amount; a velocity burst can shift location.
   The generator's label records *what was injected*; the agent reports
   *which signal dominates the evidence*. Those are different questions, and
   a single-label exact match penalizes the difference.

## Fixes

**Implemented in this commit (cause 2):**
- `time_since_last_txn_min` now flows to both agent architectures: added to
  the Snowflake fetches (`single_agent/run_demo.py`, `multi_agent/run_demo.py`,
  `governance/run_demo.py`, `observability/eval_runner.py`), both state
  schemas, the transaction prompt in `single_agent/agent.py`, and the
  multi-agent input in `orchestrator.py` / `feature_agent.py`.
- The feature specialist's prompt now explains how to use it: GEO_JUMP is an
  implied-speed judgment, and a large distance with a **negative or missing**
  time delta is unreliable state, not travel evidence.

**Recommended, deliberately NOT implemented here:**
1. *(cause 3)* Score pattern attribution as *set membership* (agent's pattern
   ∈ plausible patterns given the injected signals) or record the generator's
   secondary elevated signals. Belongs with the metrics overhaul (hardening
   Priority 5 item 6) — changing the metric and the inputs at once would blur
   which change moved the number.
2. *(new, surfaced by validation below)* Bias the **decision agent** toward
   ESCALATE when the specialists report the primary flag evidence is
   *unreliable* (e.g. out-of-order geo). Absence of exculpation is not proof
   of innocence — a flagged transaction whose flag-reason you can no longer
   verify should go to a human, not be auto-cleared. Drafted and reverted
   here: it is a behavioral (LLM-prompt) change and the Groq daily token
   budget was exhausted mid-validation, so it ships only once it can be
   demonstrated on a fresh eval run.

## Validation (measured)

Re-ran the Phase 6 eval (6 stratified transactions — **historical rows, still
carrying the corrupted geo values**, since the pipeline fix only affects new
data) after the input fix. Full output in the prerequisite report; the
relevant shift:

- **Before the input fix** (Phase 6's first batch): the two legitimate rows
  with corrupted geo were confidently mislabeled `GEO_JUMP`.
- **After the input fix**: agents given `time_since_last_txn_min` now read the
  negative/absent delta and stop asserting GEO_JUMP — every legit row in the
  batch returned pattern `NONE` and was correctly ALLOWed (pattern-ID 3/6, up
  from 2/6; escalation 0/6; judge 0.86).

That same run exposed the *opposite* failure the input fix cannot address
alone: two **fraud** rows carrying corrupted geo were ALLOWed — the agent
correctly saw the geo was unreliable, but then cleared a transaction that had
still been flagged for a reason it could no longer verify. That is exactly
what recommendation 2 above targets; it is the honest reason the decision-
agent change is deferred rather than claimed, per the "no success without
command output" rule. On hygiene-clean (post-fix) data this row class does
not arise, because the geo values reaching the agent are no longer corrupt.
