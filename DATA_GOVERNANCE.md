# Data Governance — Redaction, Retention, and Access Control

*Priority 1, item 7. This document defines the policy; where the codebase
already enforces a control it is cited, and where it does not yet, the gap is
marked **GAP** with the priority that closes it. The point of writing it now,
before those gaps close, is that you cannot enforce a boundary you have not
named — this is the named boundary.*

## 1. Data classes handled by the platform

| Class | Examples in this system | Where it lives | Sensitivity |
|---|---|---|---|
| **Direct PII** | `full_name`, home city/country, `home_latitude/longitude` | `RAW.FACT_TRANSACTIONS`, `DIM.DIM_USERS` | High |
| **Quasi-identifiers** | `user_id`, `device_id`, transaction city/geo distance | RAW, FEATURES, DECISIONS | Medium |
| **Behavioral / financial** | amounts, z-scores, velocity, risk scores | FEATURES, DECISIONS | Medium |
| **Agent reasoning** | prompts, tool outputs, `reasoning_text`, trace steps | `DECISIONS.FACT_AGENT_TRACES`, LangSmith | Medium (may quote the above) |
| **Human reviewer data** | `human_reviewer_id`, `human_notes` | `DECISIONS.FACT_DECISIONS` | Medium (staff PII + rationale) |
| **Secrets** | Snowflake/Groq/AWS creds | `.env` only | Critical |

## 2. Access control (role → data class)

The Snowflake RBAC (`snowflake/rbac.sql`) is the primary technical control.
Verified live in Priority 1:

| Role | RAW (PII) | DIM | FEATURES | DECISIONS | Used by |
|---|---|---|---|---|---|
| `PIPELINE_ROLE` | INSERT | R/W | INSERT | — | feature engine |
| `AGENT_ROLE` | **none** | SELECT | SELECT | INSERT/UPDATE/SELECT | agents, governance, observability |
| `BI_ROLE` | **none** | **none** | SELECT | SELECT | BI dashboard |

**Enforcement facts established in Priority 1:**
- BI dashboard connects via `bi_dashboard/db.open_bi_connection()` as
  `BI_ROLE`, **refuses any admin role**, and **disables secondary roles** —
  without the latter, an operator's own ACCOUNTADMIN rode along as a secondary
  role and RAW was readable despite the grant table. Now verified denied.
- The NL2SQL surface additionally cannot even *name* RAW/DIM: the AST validator
  (`sql_guard.py`) allowlists FACT_DECISIONS + FACT_FEATURE_SNAPSHOTS only.
  Two independent layers (grants + validator), by design.
- **GAP (Priority 3):** the agent/pipeline paths still default
  `SNOWFLAKE_ROLE=ACCOUNTADMIN` in shared config. `AGENT_ROLE` is now proven
  connectable, so those paths should move to role-specific credentials.

## 3. Redaction

**Principle:** the agent stack never needs direct PII to make a fraud
decision — it reasons over *derived* signals (z-scores, distances, velocity,
`is_new_device`). Direct identifiers should not enter prompts, traces, or the
BI surface at all.

| Surface | Current state | Policy |
|---|---|---|
| Agent prompts | Carry `user_id`, amounts, geo distance, city (currently "UNKNOWN" in demos) — **no names** | Keep names/lat-long out of prompts permanently. Pass `user_id` (a pseudonym), never `full_name`. |
| `reasoning_text` / traces | Free text; could echo any value placed in the prompt | Because prompts exclude direct PII, traces inherit that. **GAP (Priority 5):** add a pre-persist scrub in `audit_logger.py` that drops anything matching name/coordinate patterns, as defense in depth. |
| LangSmith | Full prompts/responses leave the platform to a vendor | Enable only in dev, never with real PII in prompts; keep the owned `FACT_AGENT_TRACES` as the system of record. Documented in `langsmith_config.py`. |
| BI results | Limited to allowlisted tables; those hold `user_id`/geo, not names | Acceptable at Medium. If names are ever needed in BI, add column-level masking policies, not a wider allowlist. |
| `human_notes` | Free text a reviewer types | Reviewer guidance: no third-party PII in notes; treat as retained staff record. |

## 4. Retention

| Store | Current | Policy / target |
|---|---|---|
| Redis online features | 24h TTL (`REDIS_TTL_SEC`) | Correct — online state, not history. Keep. |
| Kafka | 7-day retention | Transport, not system of record. Keep. |
| `RAW.FACT_TRANSACTIONS` (PII) | Indefinite | **GAP:** define a retention window (e.g. 12–24 months) + purge job; PII should not be kept forever. |
| `FEATURES` / `DECISIONS` / `FACT_AGENT_TRACES` | Indefinite (append-only audit) | Audit/compliance value justifies long retention, but set an explicit horizon and document the legal basis. Traces may quote behavioral data — include them in the same horizon as decisions. |
| LangSmith | Vendor default | Configure project retention; do not rely on "delete later." |
| `.env` secrets | Local file | **GAP (Priority 3):** move to a secrets manager; rotate the keys that have been in `.env`. |

## 5. Concrete next actions (owned by later priorities)

1. **Priority 3** — role-specific credentials; remove the shared ACCOUNTADMIN
   default from agent/pipeline paths; secrets manager + key rotation.
2. **Priority 5** — pre-persist PII scrub in the trace writer (defense in
   depth) and structured logging that never logs raw prompts at INFO.
3. **Owner/data-policy** — set and document retention windows for RAW and the
   DECISIONS/traces audit tables, with a purge mechanism.

## 6. What is already true (do not regress)

- BI surface is confined to `BI_ROLE`, secondary roles off, RAW denied
  (verified).
- NL2SQL cannot reference RAW/DIM/INFORMATION_SCHEMA or use dynamic
  identifiers / table functions (AST-enforced, 38 tests).
- Agents decide on derived signals, not names.
- Secrets are not committed (`.env` is git-ignored; grep the history before any
  public release).

## 7. Known debt / tracked risks (explicit, not implicit)

These are logged so they are decisions on the record, not surprises.

1. **Application validators are NOT yet enforced on live pipeline data
   (BLOCKS Priority 5 completion).** `db/validators.py` is unit-tested and is
   wired into the *decision* write boundary (`persist_decision`), but it is
   **not wired into `feature_engine.py`'s Spark processing loop**. That means
   live production feature rows — amounts, risk scores, z-scores — are still
   written without these checks today. **Priority 5 is not complete until**
   the validators are enforced inside the pipeline AND that enforcement is
   proven with real bad-data cases (NaN amount, out-of-range confidence,
   invalid enum combination) actually being *rejected in the live pipeline*,
   not merely in unit tests. This must be done with per-transaction failure
   isolation so one bad row cannot fail an entire micro-batch.

2. **Schema shape is hand-maintained in multiple places, reconciled by a test
   rather than generated from one source.** `snowflake/schema.sql` (the
   fresh-database baseline DDL), the `snowflake/migrations/VNNN__*.sql` DDLs,
   and `db/schema_contract.py` (the contract) each independently express column
   shape. `tests/test_schema_contract.py` catches drift *after the fact*; no
   single artifact generates the others. Architectural debt: a future
   improvement generates the DDL and the contract from one source (or the
   contract from the live `INFORMATION_SCHEMA`). If Priority 3's packaging work
   creates a clean opportunity to collapse these, take it; otherwise leave it.

3. **Three schema-contract tests compare by name, not fully-qualified
   identity** (see the Priority 2 review): `test_ddl_columns_match_contract`
   (substring), `test_pending_reviews_columns_exist` (single-table-only column
   attribution), `test_review_update_is_conditional_on_unreviewed` (substring).
   None is unsound *today*, but each would carry the same coincidental-name
   false-pass risk that the RAW collision exposed if the underlying statement
   changed shape (e.g. gained a JOIN). Cheap to harden; not yet done.
