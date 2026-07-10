# Build Report — docs site, round 3: phases.html (what / tools / why / what-if-not, per phase)

**Branch:** `docs/interactive-website` · **Date:** 2026-07-09 · **Status:** committed locally, NOT pushed (awaiting human review, as instructed)

This section covers only round 3. No existing page was rewritten or regenerated: the only changes to
`index.html`, `stack.html`, and `architecture.html` are one nav link each (plus one `id="divergence"`
anchor attribute on architecture.html's existing table, added so Phase 4's go-deeper link can land on it —
a 5-insertion/2-deletion total diff across all three, verified with `git diff`).

## What was added

### `docs/phases.html` (new, ~33 KB, self-contained, no JS)

The whole project organized by phase, 1–7, every phase answering the **same four questions as separate
labeled blocks** — WHAT (2–3 sentences), TOOLS (chips listing only that phase's tools), WHY THIS, NOT THAT
(the recorded reasoning, quoted from where it was written), WHAT IF NOT (what the rejected alternative
would have cost, only as far as the record supports).

Phase-specific requirements met:

- **Phase 3** carries three explicitly separated decisions: (1) the **ReAct vs Plan-and-Execute vs
  Reflection** comparison quoted **in full** from PROJECT_NARRATIVE.md's "Design decisions" — the reasoning
  that existed but never appeared on architecture.html; (2) **LangGraph vs CrewAI** (also quoted in full);
  (3) the narrower **hand-built StateGraph vs `create_react_agent`** question (verbatim from agent.py's
  header), labeled explicitly as the different question architecture.html answers. A fidelity note
  discloses that the record contains no Reflection-specific critique beyond the shared
  variable-tool-count rationale.
- **Phase 4** states WHY a supervisor pattern was introduced at all — task decomposition into specialists
  and structural enforcement of invariants (policy-before-decision: a prompt rule in Phase 3, structurally
  impossible to violate in Phase 4) — quoting orchestrator.py's header verbatim (marked "reflowed from
  comment lines"), plus the recorded fixed-pipeline vs pure-LLM-routing reasoning. The HOW-it-differs
  material is linked to architecture.html's divergence table, not duplicated. A disclosure notes that no
  standalone "Day 1 blueprint" document exists in the repo — the recorded framing lives in orchestrator.py
  and PROJECT_STATUS.md, and the surviving Day-1 references in code are the AGENT_ROLE boundary (tools.py)
  and the agent_name trace column (multi_agent/state.py).
- **WHAT IF NOT** covers every genuinely recorded alternative: Weaviate vs Pinecone, LangGraph vs CrewAI,
  ReAct vs Plan-and-Execute/Reflection, **A2A protocol declined**, **Factory pattern declined** (both
  narrative bullets quoted in full, with a disclosure that the narrative does not date them to a phase),
  plus the two recorded in-repo replacements (Snowpipe over row-by-row inserts; sqlglot AST guard over the
  regex guard, with sql_guard.py's attack-string reasoning quoted verbatim). For Kafka, Spark, Redis,
  Snowflake, the embedding model, Streamlit/Plotly, and the judge model, the page says plainly that no
  alternative was recorded — consistent with stack.html's existing disclosures.
- **Go deeper** links for Phase 3 (architecture.html#single, #replay) and Phase 4 (#multi, #divergence)
  instead of re-explaining mechanics.

### Nav wiring

`Phases` link added to the nav of index.html, stack.html, and architecture.html; phases.html's own nav
links all pages plus per-phase anchors 1–7.

## Fidelity checks performed

- All 7 blockquotes script-verified against their sources (PROJECT_NARRATIVE.md with markdown bold
  stripped; orchestrator.py with comment markers stripped) — whitespace-normalized substring match, all
  PASS. Both verbatim `pre` blocks (agent.py header, sql_guard.py excerpt) verified **line-by-line** against
  the source files, PASS. Seven shorter quoted fragments (system-prompt rule, "arbiter of its own
  trustworthiness", "rule citation, not a probability", Day-1 references, etc.) verified, all PASS.
- HTML tag-balance PASS on all four pages; all go-deeper anchors exist in architecture.html; all
  cross-page links resolve (scripted).
- Live: page loads with 7 phase sections, each with exactly the four blocks in template order; Phase 3/4
  are the only ones with go-deeper blocks; Phase 4 carries both fidelity disclosures; zero console errors;
  index.html's widgets still respond and its nav gained exactly the Phases link. (Preview-tool caveat: the
  screenshot capture rendered black frames when scrolled mid-page this session — a capture artifact, not a
  page defect; DOM inspection at those scroll positions confirmed the correct elements, and top-of-page
  screenshots render fully.)

## Not done, per instructions

- Not pushed to GitHub — stopped for human review.

---

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
