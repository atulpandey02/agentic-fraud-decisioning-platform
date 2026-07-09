# Build Report — docs site, round 2: tech stack + agentic architecture deep dive

**Branch:** `docs/interactive-website` · **Date:** 2026-07-09 · **Status:** committed locally, NOT pushed (awaiting human review, as instructed)

This report covers only this round's additions. `docs/index.html` (supervision-loop stepper, the three interactive widgets, timeline, fidelity notes) was **not** rewritten or regenerated — its only change is two new nav links (a 2-line diff, verified with `git diff`).

---

## What was added

### 1. `docs/stack.html` — tech stack page (new, ~22 KB, self-contained)

One entry per tool actually used: Kafka, Spark Structured Streaming, Redis, Snowflake, S3 + Snowpipe, Weaviate, LangGraph, Groq (llama-3.3-70b-versatile), sqlglot, Docker Compose. Each entry has:

- **Role in THIS system** — concrete job, not a generic blurb (e.g. Kafka keyed by `user_id` so per-user windowed aggregation is shuffle-friendly; Redis as the TTL'd online feature store the agent tools read).
- **Why chosen, as recorded** — comparisons appear ONLY where the repo/narrative actually recorded one:
  - Weaviate over Pinecone (from PROJECT_NARRATIVE.md "Design decisions", plus the comparison docstring in `retrieval/weaviate_client.py`).
  - LangGraph over CrewAI (from PROJECT_NARRATIVE.md).
  - Snowpipe over `executemany` and sqlglot over regex are presented as *historical replacements the repo itself documents*, not as bake-offs.
  - For Kafka, Spark, Redis, Snowflake, Groq, and Docker the page says plainly that **no alternative was recorded** rather than inventing a plausible comparison.
- **One real code snippet** from the repo, quoted verbatim with its source path (e.g. `feature_engine`'s `_read_kafka_stream`, the Redis `MGET` core, `S3FeatureWriter.flush`, `hybrid_search`, `sql_guard.validate`, the single-agent Groq config block, the docker-compose KRaft header comments).

Ends with its own fidelity note.

### 2. `docs/architecture.html` — agentic architecture deep dive (new, ~58 KB, self-contained)

Four parts:

- **Part A — "How LangGraph works" primer.** Four concept cards (nodes as partial-state-update functions; unconditional vs conditional edges and cycles; state reducers, specifically `add_messages` and `operator.add`; compile/checkpointing), each ending with where the concept appears in this repo. Includes a verbatim excerpt of the actual `AgentState`.
- **Part B — the single agent's ACTUAL StateGraph**, hand-drawn as SVG from `_build_graph()` in `fraud_platform/agents/single_agent/agent.py`: START → agent → conditional → (tools → agent loop) → finalize → END. Every node is clickable and loads the **verbatim** function behind it into a code panel: `_call_model`, `_should_continue` (with the `MAX_REASONING_STEPS = 8` cap), `finalize` (the `with_structured_output(DecisionOutput)` restatement), plus the `ToolNode` wiring.
- **Part C — step-by-step trace replay** (10 steps) of one real recorded run: the Phase 3 Run 1 transaction that the agent BLOCKed as GEO_JUMP at 0.9 confidence and that ground truth later showed legitimate — the same case index.html's story centers on. Each step drives a highlight on the diagram and shows only real artifacts: verbatim tool docstrings from `tools.py`, the verbatim Redis-empty fallback string, the verbatim policy-result format string, recorded run facts (4 Groq calls, all 3 tools fired, Redis TTL expiry). **No dialogue is fabricated**: every step carries provenance chips — `recorded` / `mechanics` (guaranteed by the code shown) / `not recorded` (e.g. the model's own free-text, which the trace never captured).
- **Part D — the Phase 4 supervisor orchestrator**, same treatment: clickable SVG of START → orchestrator → conditional → four specialists (→ back to orchestrator) → decision_agent → END, with verbatim `_route` (both guardrail overrides), `RouteDecision`, the blackboard `MultiAgentState`, and the router's hard stops. Includes the verbatim log of the recorded 2026-07-06 governance run ($32.17 → ALLOW/0.8, risk_agent skipped, AUTO_APPROVE) and an 8-row table of **mechanical** divergences from the single agent (messages-as-memory vs typed blackboard, model-emitted tool_calls vs constructor-injected deterministic calls, prompt invariants vs code-enforced invariants, MAX_REASONING_STEPS=8 vs ORCHESTRATOR_MAX_STEPS=6, etc.).

### 3. `docs/index.html` — nav links only

Two `<a>` tags added to the existing nav (`Stack`, `Architecture`). Nothing else touched.

---

## Fidelity discipline applied this round

- **Every "(verbatim)" block was substring-checked against its source file** by script, including line breaks: two router-prompt quotes initially FAILed because I had re-wrapped source lines; they were reflowed to the source's actual line breaks and relabeled "(verbatim, source line breaks preserved)". All per-line quote checks pass.
- **Typographic-apostrophe sweep:** 11 code-quoting lines used `’` where the source uses ASCII `'`; all repaired (the repair itself briefly broke the page's JS by unescaping string delimiters — caught by `node --check` and by live-clicking the replay widget, then fixed and re-verified).
- **Disclosed rather than invented**, on-page:
  - The model's free-text reasoning per step was never recorded — replay steps that would need it are tagged `not recorded`.
  - The 4-Groq-calls vs 3-tools accounting is reconciled explicitly (parallel tool_calls) instead of asserting a call-by-call script.
  - `retrieval/policy_docs/geo_jump.md` is **stale**: it still describes the old 0.95-signal hard rule that the prereq fix replaced — noted on the page, not silently corrected.
  - The `MemorySaver` comment in `agent.py` still says durable tracing is "not yet done" though Phase 6 built it — noted as a stale comment.
  - The supervisor run log is quoted verbatim **including** the router's unverified "velocity count below 3" policy claim, flagged as such.
  - Diagrams are hand-drawn from `_build_graph()`, not generated by LangGraph's renderer — stated on the page.

## Verification performed

- Static: HTML tag-balance check on all three pages; `node --check` on architecture.html's extracted script (pass); scripted verbatim-quote checks against `agent.py`, `tools.py`, `orchestrator.py`, `state.py` (all pass).
- Live (preview server, `python3 -m http.server 8710 --directory docs`): all 6 single-graph and all 7 multi-graph nodes clicked — each populates the correct code panel; trace replay stepped 1→10 — progress counter, per-step provenance chips, diagram pulse-highlighting, artifacts, and restart-wrap all correct; stack.html shows all 10 tool entries with 10 code blocks and correct nav; index.html sliders still respond and its nav gained exactly the two new links; **zero console errors on all three pages**.

## Not done, per instructions

- Not pushed to GitHub — stopped for human review.
- No changes to the stale repo artifacts found while building (geo_jump.md, the MemorySaver comment); they are disclosed on the page and listed here, but fixing them is outside this docs round.
