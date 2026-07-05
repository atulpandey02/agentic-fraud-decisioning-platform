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

### Phase 4 — Multi-agent orchestrator — **NOT STARTED** (all 7 files are 0 bytes)
### Phase 5 — Governance / HITL — **NOT STARTED** (`governance/*.py` 0 bytes; schema columns exist in FACT_DECISIONS, unused)
### Phase 6 — Observability + evaluation — **NOT STARTED** (`observability/*.py` 0 bytes; eval columns exist in FACT_DECISIONS, unused; LANGSMITH_API_KEY present in .env but unused)
### Phase 7 — Agentic BI dashboard — **NOT STARTED** (`bi_dashboard/*.py` 0 bytes; `requirements_bi.txt` exists)

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
