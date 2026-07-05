# PROJECT STATUS — Agentic Fraud Decisioning Platform

*Written 2026-07-05, after a full read-through of the repository, the docstrings'
design rationale, and live verification against the Snowflake account.*

---

## 1. Phase-by-phase status

### Phase 1 — Streaming foundation + feature store — **DONE, working**

**Built and working:**
- `data_generator/` — user profile generator (10K users, SCD2 DIM_USERS,
  DIM_DEVICES with deliberate untrusted-secondary-device noise), transaction
  stream generator with 4 injectable fraud patterns, 10M-row backfill mode,
  Kafka + Snowflake writers. All committed, all substantive.
- `stream_processing/feature_engine.py` (1,606 lines) — the full pipeline:
  Kafka → parse → amount z-score / geo Haversine (Redis state) / new-device /
  risk score → dual sink. Redis N+1 fixed (MGET batching), S3 Parquet +
  Snowpipe replaces row-by-row inserts, velocity query merged into the same
  process (two concurrent Spark queries, one session, communicating via Redis).
  Hard-rule single-signal overrides added Day 4 with documented reasoning.
- `stream_processing/velocity_engine.py` — standalone velocity query, kept as
  the original proof; its logic now also runs inside feature_engine. Both pull
  window definitions from `window_config.py` (shared, no drift).
- **Verified live:** `FEATURES.FACT_FEATURE_SNAPSHOTS` has **999,878 rows**,
  **443,805 flagged**, all flagged rows carry ground truth
  (`is_synthetic_fraud`, `fraud_pattern`). The pipeline demonstrably ran at scale.

**Intentionally unfinished (per docstrings — do not "fix"):**
- Only the 15-min velocity window is live; `velocity_5min/1hr/24hr` are written
  as NULL. `window_config.py` and the feature_dict comment both say the other
  three windows follow once 15-min is validated. Deliberate.
- `SnowflakeFeatureWriter` is kept only as a manual fallback / `test_connection()`
  probe; S3→Snowpipe is the primary write path. Deliberate.
- `feature_store/redis_client.py` and `feature_store/snowflake_client.py` are
  **0-byte stubs**. Not dead code — tools.py's docstring explains the agent uses
  its own lightweight readers instead, and the writers live inside
  feature_engine.py. The empty files are just scaffolding that was never needed.

**Observation (not a bug to fix now):** 443K flagged out of ~1M (44%) is far
above the 15% injected fraud rate. The hard-rule geo threshold
(`geo_signal >= 0.95` ⇒ geo_distance ≥ 475 km) combined with randomized
backfill timestamps likely over-fires GEO_JUMP on legitimate rows. Worth a
precision/recall pass in Phase 6 — the eval phase exists for exactly this.

### Phase 2 — Retrieval / RAG layer — **DONE, code complete**

**Built:**
- `retrieval/chunker.py` — structure-aware H2 chunking with the prefix-overhead
  budget fix (documented as a real bug caught in testing).
- `retrieval/embedder.py` — all-MiniLM-L6-v2 wrapper with explicit
  tokenizer-based truncation guard.
- `retrieval/weaviate_client.py` — `FraudKnowledgeBase`: idempotent schema,
  batch upsert, native hybrid search with optional pattern filter.
- `retrieval/ingest.py` + `query_test.py` — ingestion and retrieval-quality
  harness.
- Content: 4 policy docs + 4 case studies, one per fraud pattern, authored for
  retrieval (H1 = pattern name, H2 sections).

**Verified in Step 2:** Docker (Redis, Weaviate) was down when this analysis
started. After restarting the containers, the `weaviate_data` volume still held
the ingested `FraudPolicyChunk` collection — hybrid search worked immediately,
no re-ingest needed. Redis, by contrast, was **empty**: the 24-hour feature TTL
expired everything since the pipeline last ran (Jul 3–4). That is documented,
intended behavior ("stale features auto-expire"), and the agent's
`get_transaction_features` tool handles it gracefully; rerunning
`feature_engine.py` repopulates it whenever fresh online state is wanted.

### Phase 3 — Single ReAct agent — **NOW RUNNING end to end** (was: scaffolded, never run; three real blockers found and fixed in Step 2)

**Built (uncommitted working-tree code):**
- `agents/single_agent/state.py` — AgentState TypedDict with add_messages reducer.
- `agents/single_agent/agent.py` — explicit StateGraph ReAct loop (agent ⇄ tools
  → finalize), MAX_REASONING_STEPS cap, structured `DecisionOutput` via a
  dedicated finalize call, MemorySaver checkpointer.
- `agents/single_agent/tools.py` — 3 tools: lightweight Redis `FeatureReader`,
  lightweight Snowflake `UserHistoryReader` (DIM-only, honoring the AGENT_ROLE
  boundary by discipline), and `get_policy_context` reusing Phase 2's
  Embedder + FraudKnowledgeBase.
- `agents/single_agent/config.py`, `run_demo.py` — untracked new files.

**Was broken — three blockers found at analysis time, all fixed in Step 2 (see §4):**
1. **`config` module name collision.** `tools.py` inserts `retrieval/` into
   `sys.path` and imports `weaviate_client` / `embedder`, but those modules do
   `from config import WEAVIATE_COLLECTION_NAME, ...` — and by then
   `sys.modules["config"]` is already the *agent's* config.py (imported first
   by agent.py/tools.py), which lacks `WEAVIATE_COLLECTION_NAME`,
   `HYBRID_SEARCH_ALPHA`, `EMBEDDING_MODEL_NAME`, `CHUNK_MAX_WORDS`, etc.
   ⇒ guaranteed `ImportError` before the graph is even built. This can never
   have worked; it's a Phase 3 bug, not a Phase 2 bug.
2. **`.env` never loaded.** `feature_engine.py` and `velocity_engine.py` both
   call `load_dotenv()`; nothing under `agents/single_agent/` does. `config.py`
   reads `os.getenv("GROQ_API_KEY")` etc., so unless the shell happens to export
   them, the agent dies at construction with "GROQ_API_KEY not set" and the
   Snowflake readers get `None` credentials.
3. **Dependencies not installed.** The venv has Phase 1 + Phase 2 packages but
   **no langgraph / langchain-core / langchain-groq / langsmith** — the
   `requirements_agents.txt` install genuinely never completed. (Its header
   comment documents the two failed pin guesses; the pins are now relaxed.)

**Verified after the Step 2 fixes:** `run_demo.py` completed end to end twice,
each time pulling a real random flagged row from FACT_FEATURE_SNAPSHOTS and
driving the full loop — agent → tools (all three fired: Redis feature read,
Weaviate hybrid policy search, Snowflake DIM user history) → agent → finalize —
in 4 Groq calls, producing a structured decision. Run 1: BLOCK / GEO_JUMP /
confidence 0.9. Run 2: ESCALATE / GEO_JUMP / confidence 0.6 (appropriately
lower on more ambiguous signals). Both draws happened to be ground-truth
legitimate transactions the hard rules had flagged — live confirmation of the
flag-rate observation under Phase 1, and exactly the accuracy signal
`run_demo.py`'s pattern-match check was built to surface for Phase 6.

### Phase 4 — Multi-agent orchestrator — **DONE, verified end to end** (built 2026-07-05)

**Architecture — supervisor pattern with code guardrails:**
`agents/multi_agent/` — an orchestrator node routes between four specialists,
each a focused single-LLM-call worker, over a shared typed blackboard
(`MultiAgentState`):

- `feature_agent` — deterministic feature fetch + LLM read of which signals
  are elevated (machine-readable `elevated_patterns` drives everything downstream)
- `risk_agent` — user-baseline fetch + per-user anomaly read; sets
  `is_borderline`, which biases the final decision toward ESCALATE
- `policy_agent` — one Weaviate hybrid search *per elevated pattern* (using
  Phase 2's pattern filter), LLM condenses to applicable rules with thresholds quoted
- `decision_agent` — terminal; synthesizes the blackboard, no tools by design
  (evidence-gathering and deciding are deliberately separated concerns)

**The key design decisions to learn from (full "why" docstrings in each file):**
1. **Specialists are not mini-ReAct loops.** Each runs its tool
   unconditionally in code, then makes ONE focused LLM call. Autonomy lives
   only in the orchestrator (whether a specialist runs at all) — cheaper and
   more reliable than asking a model to decide to do the only thing it exists to do.
2. **LLM routing + code invariants.** The router LLM chooses order and skips;
   code enforces what must always hold: each specialist at most once, *no
   decision without policy grounding* (Phase 3 had this as a prompt rule the
   model could ignore; here it is structurally impossible to violate), and an
   `ORCHESTRATOR_MAX_STEPS` cap. Same philosophy as Phase 1's two-tier scoring.
3. **Config layering via importlib.** `multi_agent/config.py` loads Phase 3's
   config *by file path* under a private module name and re-exports its
   constants — reuse without either renaming the per-phase `config.py`
   convention or copying values that would drift.
4. **One tool bridge, constructor injection.** orchestrator.py is the single
   place that imports Phase 3's three tools; specialists get them injected —
   no cross-phase import mechanics in specialist files, trivially stubable.

**Verified:** two end-to-end runs with genuinely different routing. Run 1
(clear-cut): feature → policy → decision, risk_agent *skipped*, ALLOW at 0.8 —
correctly allowing a ground-truth-legit flagged row (Phase 3's monolith blocked
a comparable one). Run 2 (borderline GEO_JUMP): all four specialists,
ESCALATE at 0.6. Also fixed a real comparison bug found during verification:
both demos compared the agent's `"NONE"` pattern string against SQL `NULL`
ground truth, mis-reporting every correct no-pattern verdict as a mismatch.
### Phase 5 — Governance / HITL — **DONE, verified end to end** (built 2026-07-05)

**The core idea:** the agent decides *what* should happen (ALLOW/BLOCK/ESCALATE);
governance decides *how much autonomy* that decision gets. The two judgments are
deliberately separated — the agent should not be the arbiter of its own
trustworthiness, and the autonomy rules are deterministic code (not another LLM
call) because "why did this auto-execute?" must be answerable with a rule
citation, not a probability.

**Built:**
- `governance/policy_framework.py` — `GovernancePolicyFramework.assign_tier()`
  maps (decision, confidence, amount) → tier, reasoning from asymmetric error
  costs: a false ALLOW is unrecoverable money (so silence is capped by value —
  `AUTO_ALLOW_MAX_AMOUNT`), a false BLOCK is a recoverable inconvenience (so it
  executes but is always surfaced), and human attention is the scarce resource
  (so the SUGGEST queue holds only escalations and low-confidence calls —
  `GOVERNANCE_CONFIDENCE_FLOOR = 0.75`).
- `governance/hitl_handler.py` — `HITLHandler`, the **single owner of all
  DECISIONS.FACT_DECISIONS writes**: `persist_decision()` (a decision row is
  born complete, WITH its tier — persistence lives in governance, not in the
  agents, so no partial-row window exists), `pending_reviews()` (age-ordered
  SUGGEST queue), `record_review()` (human CONFIRMED/OVERRIDDEN/ESCALATED
  verdicts — which accumulate as free human-labeled eval data for Phase 6).
- `governance/config.py` — second layer of the importlib config chain
  (governance ⊃ multi_agent ⊃ single_agent).
- `governance/run_demo.py` — full loop: orchestrate → tier → persist → queue →
  simulated human review.

**This is the phase where decisions first became durable** — until now every
decision died with its process (MemorySaver's documented limitation).

**Verified:** run 1 — confident BLOCK (0.95, GEO_JUMP) → NOTIFY_ONLY, persisted,
bypassed the queue correctly. Run 2 — ESCALATE (0.6) → SUGGEST → appeared in the
queue → review recorded → queue drained to 0. Read back from Snowflake: both
rows complete with tier, latency_ms, and the reviewed row carrying
human_outcome=CONFIRMED.

**Open item:** applying `rbac.sql` (creating AGENT_ROLE / BI_ROLE) was blocked
by the execution environment's permission policy — account-level role creation
needs the owner to run it. Run `snowflake/rbac.sql` in a Snowflake worksheet as
ACCOUNTADMIN to close it; the code already honors the intended boundaries by
discipline (governance touches only the DECISIONS schema).
### Phase 6 — Observability + evaluation — **DONE** (built 2026-07-05)

**Built:**
- `observability/audit_logger.py` — `AgentTraceWriter`, the writer
  FACT_AGENT_TRACES waited for since Day 1. Implements exactly the mapping
  Phase 3's `AgentState` docstring designed: step_number = position in the
  messages list; step_type from the message role (INPUT / TOOL_CALL /
  TOOL_OUTPUT / REASONING / HANDOFF — HANDOFF being the orchestrator's routing
  messages, which is what `agent_name` was designed for). Uses the
  `INSERT ... SELECT PARSE_JSON(...) FROM VALUES` form because Snowflake
  VARIANT columns can't take PARSE_JSON inside a bulk VALUES clause — the one
  canonical example of the correct VARIANT-load pattern in this codebase.
- `observability/langsmith_config.py` — env-var-based LangSmith enablement
  with an explicit division of labor: FACT_AGENT_TRACES is the *owned*
  compliance record ("who decided and why", queryable next to the decisions);
  LangSmith is *vendor* dev-telemetry ("why was that prompt slow/wrong",
  full prompts + per-call tokens). Graceful no-op without an API key.
- `observability/eval_runner.py` — the full-stack integration point: for each
  sampled transaction it runs Phase 4 orchestration, Phase 5 tiering +
  persistence, writes the Phase 6 trace, then scores two *orthogonal* things:
  1. **Outcome accuracy** (`eval_correct`): decision vs. synthetic ground
     truth. ESCALATE deliberately scores NULL — a deferral is neither right
     nor wrong, and scoring it either way would teach the wrong lesson
     (always-escalate games accuracy; punishing it kills calibrated humility).
     Escalation *rate* is reported as its own number instead.
  2. **Reasoning quality** (`llm_judge_score/notes`): an LLM judge checks
     groundedness/consistency/completeness of the reasoning against the
     evidence the team actually gathered — grading free text is exactly what
     rules can't do (the mirror image of governance's rules-not-LLM choice).
     Judge is same-model-family (documented self-preference caveat), temp 0.
- Sampling is **stratified** half-fraud/half-legit — an unstratified draw from
  the flagged pool (~2:1 legit, per the hard-rule over-flagging) would mostly
  test false-positive handling and give almost no recall signal.
- `observability/config.py` — third layer of the importlib config chain.

**Verified (first eval batch, 2026-07-05):** 6 stratified transactions (3 fraud
/ 3 legit) ran the full stack. Results — read these as the platform's first
honest scorecard, not a victory lap:
- **Decision accuracy 3/4** (excluding escalations): all 3 true frauds were
  BLOCKed; 1 legit transaction was wrongly BLOCKed as GEO_JUMP — the same
  hard-rule geo over-flagging noted in the Phase 1 observation, now visible as
  a measured agent error instead of a hunch. This is the eval loop doing its job.
- **Escalation rate 2/6** — both escalations were legit transactions the agent
  (correctly) didn't feel confident clearing; both landed in the SUGGEST queue.
- **Pattern identification 2/6** — weakest number; the agent often picks a
  plausible-but-wrong pattern (e.g. AMOUNT_ANOMALY when the ground truth was
  NEW_DEVICE with a high amount — the patterns overlap by construction).
- **Mean judge score 0.88**; mean latency ~19.5s/decision — dominated by Groq
  free-tier rate-limit retries, not by the architecture.
- **FACT_AGENT_TRACES: 46 rows across 6 decisions**, read back with correct
  step ordering: INPUT → HANDOFF (orchestrator, with routing rationale) →
  REASONING (named specialist) → ... → decision. FACT_DECISIONS eval columns
  populated: `eval_correct` TRUE/FALSE/NULL exactly per the deferral rule,
  judge scores + notes on every row.
- One fix found during verification: INPUT trace rows fell back to
  `agent_name='single_agent'` in multi-agent runs; they now label as `'user'`.
### Phase 7 — Agentic BI dashboard (NL2SQL) — **DONE** (built 2026-07-05)

**Built:**
- `bi_dashboard/nl2sql_agent.py` — `NL2SQLAgent`: English question → one
  Groq LLM call producing structured `{sql, explanation, chart_hint}` → code
  guardrails → execution. The trust model is explicit: **the LLM is an
  untrusted SQL author**. The prompt asks for good behavior (quality); the
  post-generation validator enforces it (safety): SELECT/WITH-only, no
  semicolon smuggling, forbidden-keyword scan, table allowlist (only
  FACT_DECISIONS + FACT_FEATURE_SNAPSHOTS — RAW.* absent from the list IS
  the PII boundary BI_ROLE was designed with), and a LIMIT appended when
  missing. Violations raise a distinct `QueryRejected` so the UI can present
  "the platform refused" differently from "the query broke".
- `bi_dashboard/chart_renderer.py` — hint-proposes/shape-disposes rendering:
  the LLM (which saw the intent) hints bar/line/pie/table/metric; code (which
  saw the actual result shape) can veto down to table/metric, never erroring
  on odd shapes. Builds DataFrames manually rather than via
  `fetch_pandas_all()` to avoid the connector↔pyarrow version coupling
  (pyarrow is already pinned by Phase 1's Parquet writer).
- `bi_dashboard/streamlit_app.py` — thin UI over the two classes; the one
  UI-policy decision made there: **generated SQL is always shown, expanded** —
  an analyst who can't see the SQL can't catch a subtly-wrong query.
- `bi_dashboard/config.py` — outermost config layer; `requirements_bi.txt`
  fixed (filename in the install comment, pandas left to streamlit's resolver).

**Verified (2026-07-05):**
- `requirements_bi.txt` installs cleanly (streamlit 1.32.0, plotly 5.20.0,
  pandas 2.3.3 via streamlit).
- Real question → real answer against the live decision log: *"How many
  decisions per decision type, with their average confidence?"* produced
  correct SQL and the true numbers (5 BLOCK @ 0.94 avg, 3 ESCALATE @ 0.60) —
  matching the 8 decisions Phases 5–6 persisted. A CTE question over the eval
  columns also generated and ran correctly (NOTIFY_ONLY tier: 75% correct,
  which is the eval batch's 3/4).
- **Guardrails fail closed, all four layers:** `DROP TABLE` → rejected
  (SELECT-only); `SELECT ... FROM RAW.FACT_TRANSACTIONS` → rejected
  (allowlist — the PII boundary held); `SELECT 1; SELECT 2` → rejected
  (multi-statement); `UPDATE ...` → rejected. Missing LIMIT auto-appended.
- Streamlit app boots headless, serves HTTP 200, `/_stcore/health` → ok.
  Launch with: `cd bi_dashboard && streamlit run streamlit_app.py`.

---

## The platform is complete: all 7 phases built and verified

End-to-end flow now running against real infrastructure:
Kafka → Spark features (Redis online / S3+Snowpipe offline) → flagged
transactions → multi-agent orchestration (Groq LLM specialists + Weaviate RAG
policy grounding) → governance tier → durable decision + full reasoning trace
in Snowflake → ground-truth + LLM-judge evaluation → natural-language BI over
the audit log.

Remaining known open items (all documented above in place):
1. `rbac.sql` needs a manual run as ACCOUNTADMIN (AGENT_ROLE / BI_ROLE).
2. `snowflake/schema.sql` is stale vs. the live FACT_FEATURE_SNAPSHOTS
   (missing the two ground-truth columns added Day 4).
3. Hard-rule geo over-flagging (~44% flag rate) — now *measured* by the eval
   loop (the one wrong BLOCK); tuning `HARD_RULE_GEO_THRESHOLD` /
   `GEO_MAX_NORMAL_KM` is the first data-driven improvement the eval
   pipeline can validate.
4. Phase 3's `requirements_agents.txt` resolver conflict (langchain-groq
   needs a 0.3.x bound to coexist with langchain-core <0.4) — the venv has a
   working set installed; the file fix is pending from the earlier session.
5. Empty placeholders unchanged by design: `tests/`, CI workflow,
   `seed_data.sql`, `feature_store/` stubs. No README.md yet — this file
   currently carries the narrative.

Also empty by design (placeholders, not breakage): `tests/*.py`,
`.github/workflows/ci.yml`, `snowflake/seed_data.sql`. **There is no README.md
at all** — worth writing eventually, since the docstrings currently carry all
the architectural narrative.

---

## 2. Docstring claims vs. actual code

| # | Claim | Reality | Verdict |
|---|-------|---------|---------|
| 1 | `feature_engine.py:1369` — ground truth "carried through to the Snowflake row until this fix" | True for the primary S3/Snowpipe path (the Parquet file includes both fields). But the fallback `SnowflakeFeatureWriter.write_batch` INSERT column list still **omits** `is_synthetic_fraud`/`fraud_pattern` — if the fallback were ever used, ground truth would silently drop. | Minor inconsistency (fallback-only) |
| 2 | `snowflake/schema.sql` — FACT_FEATURE_SNAPSHOTS definition has **no** `is_synthetic_fraud`/`fraud_pattern` columns | The live table **does** have both (verified via DESC TABLE). The table was evidently ALTERed by hand on Day 4 and schema.sql was never updated. Anyone rebuilding from schema.sql gets a table the pipeline can't fully load. | Real drift — schema.sql is stale |
| 3 | `tools.py` header — "the Weaviate/embedding classes from Phase 2 ... get imported and reused directly" | The import mechanism is broken (config collision, §1 Phase 3). The *intent* is sound; the execution fails. | Broken |
| 4 | `agent.py:187` — "FACT_AGENT_TRACES ... is the next piece to build, not yet done" | Confirmed: 0 rows, no writer code anywhere. | Consistent — honest, intentional |
| 5 | `config.py` (agent) — "AGENT_ROLE (designed Day 1, not yet applied)" | Confirmed *and refined*: `SHOW ROLES` says **PIPELINE_ROLE exists** but AGENT_ROLE and BI_ROLE do not — rbac.sql was **partially** applied at some point, not "never" and not "fully". | Docstring roughly right; see §3 |
| 6 | `requirements.txt` comments say `pip install -r requirements-agents.txt` / `requirements-bi.txt` (hyphens) | Actual files use underscores: `requirements_agents.txt`, `requirements_bi.txt` (only the RAG file uses a hyphen). Copy-pasting the documented command fails. | Cosmetic inconsistency |
| 7 | `run_demo.py` — "fraud_pattern ground truth column (Day 4 fix)" queried from FACT_FEATURE_SNAPSHOTS | Columns exist and are populated on all 443,805 flagged rows. Query will return data. | Consistent |

---

## 3. Known open items — verified

- **RBAC:** *Partially* applied, not merely "never applied".
  `PIPELINE_ROLE` exists in the account; **`AGENT_ROLE` and `BI_ROLE` do not**.
  Also note rbac.sql grants cover existing tables only (`ON ALL TABLES`, no
  `FUTURE TABLES` clause) and no warehouse USAGE grants — fine for now, worth
  remembering when it's actually applied.
- **FACT_AGENT_TRACES:** Confirmed — table exists, **0 rows**, and no code
  anywhere writes to it. `AgentState.messages`' docstring already maps message
  positions → trace rows, so the writer is designed but unbuilt. Intentional
  (Phase 5/6 territory). `FACT_DECISIONS` is likewise 0 rows — nothing persists
  agent decisions yet either.
- **requirements_agents.txt:** Did **not** install cleanly as found
  (`ResolutionImpossible`). Root cause was not langchain-groq at all:
  `langgraph==1.0.10` → `langgraph-prebuilt>=1.0.8,<1.1.0` → hard requirement
  `langchain-core>=1.0.0`, which made the repo's `langchain-core>=0.3,<0.4` pin
  unsatisfiable. On top of that, langgraph 1.0.10's own prebuilt upper bound is
  broken upstream — prebuilt 1.0.9–1.0.13 import `ExecutionInfo` which doesn't
  exist in langgraph 1.0.10, failing at *import* time even when pip resolves.
  Fixed in Step 2: `langchain-core>=1.0,<2.0` + `langgraph-prebuilt==1.0.8`
  (found by bisection); `langchain-groq`/`langsmith` stay unpinned. Verified:
  installs cleanly, `pip check` clean, demo runs.

---

## 4. Broken vs. intentionally unfinished

**Actually broken — all three fixed in Step 2 (demo now runs end to end):**
1. `config` module collision between `agents/single_agent/config.py` and
   `retrieval/config.py` — hard ImportError in `tools.py`. Fixed with a scoped
   `sys.modules` swap during the retrieval imports (documented in tools.py).
2. Missing `load_dotenv()` in the agent path — no credentials at runtime.
   Fixed in `agents/single_agent/config.py`, matching feature_engine.py's pattern.
3. Unsatisfiable dependency pins in `requirements_agents.txt` (see §3) — fixed,
   installed, `pip check` clean.

**Still open, deliberately untouched (stale-doc, low priority):**
4. schema.sql missing the two ground-truth columns the live table has; fallback
   Snowflake INSERT missing the same two columns.

**Intentionally unfinished (leave alone):**
- FACT_AGENT_TRACES / FACT_DECISIONS writers; governance/eval columns in
  FACT_DECISIONS; MemorySaver as ephemeral-only checkpointing (all documented).
- velocity_5min/1hr/24hr windows (NULL by design until 15-min is validated).
- All Phase 4–7 zero-byte files, empty tests, empty CI workflow, empty
  seed_data.sql, `feature_store/` stubs.
- `merchant_category`/`city`/`country` = "UNKNOWN" in run_demo.py (documented
  as an acceptable gap — those fields aren't in FACT_FEATURE_SNAPSHOTS).
- Hard-rule flag-rate inflation (§1, Phase 1 observation) — a Phase 6 eval
  question, not a Phase 3 blocker.
