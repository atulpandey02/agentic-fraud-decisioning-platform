# Credential & Data Migration — new Snowflake trial account

**Date:** 2026-07-27 · **Old account:** `CZYDFRS-AF06435` (expired) → **New account:** `MK46267` · **User:** `ATULPANDEY02`

This was a real credential-and-data reset after the previous trial account expired — a fresh, empty
Snowflake account. This document records exactly what was recreated, what was **not** recoverable, and the
current state of the new account versus the old one. Nothing here relates to the deferred DuckDB/Iceberg
work — that remains a separate, later effort and was not touched.

**Status: Steps 1–6 complete. Not committed, not pushed — awaiting review.**

---

## Summary of outcome

| Step | Result |
|---|---|
| 1 — Credentials | ✅ `.env` read by `settings.py`; ACCOUNTADMIN connection verified (`MK46267` / `ATULPANDEY02` / Snowflake 10.25.102) |
| 2 — DB + schema | ✅ `schema.sql` baseline + migrations V001–V004 applied in order |
| 3 — RBAC | ✅ roles + grants rebuilt; 3 roles granted to `ATULPANDEY02`; all access proofs pass with secondary roles confined |
| 4 — Data | ✅ **bounded regeneration** (Kafka backfill was gone — see below): 10K users, 25K transactions, 25K feature snapshots |
| 5 — Verify | ✅ single-agent, multi-agent, BI/NL2SQL under BI_ROLE, and eval smoke test all pass end-to-end |
| 6 — Report | ✅ this document |

---

## Step 1 — Credentials

`.env` was updated by the account owner (I do not handle plaintext secrets). `fraud_platform/settings.py`
read all fields correctly. Basic ACCOUNTADMIN connectivity confirmed via `SELECT CURRENT_ACCOUNT/USER/ROLE/VERSION`:
`MK46267` / `ATULPANDEY02` / `ACCOUNTADMIN` / `10.25.102`. `FRAUD_DETECTION` did not exist yet (expected).

## Step 2 — Database & schema rebuilt from empty

**Correction to the instruction:** the `VNNN` migrations are *incremental* (e.g. V001 adds columns; V002/V004
are grants). They cannot build a fresh account alone — the repo's documented model is that
`snowflake/schema.sql` is the **run-once baseline**, and `db/migrate.py` applies incremental changes on top
(stated in both files' docstrings and README line 68). So the correct, repo-documented sequence was applied:

1. `schema.sql` (as ACCOUNTADMIN): created `FRAUD_DETECTION`, schemas `DIM/RAW/FEATURES/DECISIONS`, and all
   6 base tables.
2. `db/migrate.py`: applied V001→V004. Final `SCHEMA_MIGRATIONS`:

   ```
   V001  V001__feature_snapshots_ground_truth.sql
   V002  V002__future_table_grants.sql
   V003  V003__feature_snapshots_staging.sql   (also created FEATURES.STG_FEATURE_SNAPSHOTS)
   V004  V004__pipeline_features_select.sql
   ```

## Step 3 — RBAC rebuilt & re-proven

**Ordering correction:** V002/V004 grant privileges *to* the RBAC roles, so `rbac.sql` (Step 3) had to run
**before** those migrations could succeed. On the first pass V002 failed with `Role 'BI_ROLE' does not exist`;
resolved by running `rbac.sql` first, then re-running `migrate.py` (idempotent; V001 was already recorded).
This is order-only — no content changed.

- `snowflake/rbac.sql` applied (account-agnostic; 3 roles + schema/table/warehouse grants + FUTURE-table grants via V002).
- User-to-role grants for `ATULPANDEY02` (from the `rbac_local_example.sql` recipe): `BI_ROLE`, `AGENT_ROLE`, `PIPELINE_ROLE`.

**Access proofs on `MK46267` (all with `USE SECONDARY ROLES NONE` — secondary roles confined to `{"roles":"","value":""}`):**

| Role | Result |
|---|---|
| PIPELINE_ROLE | startup probe `COUNT(*)` on FEATURES → **OK** |
| AGENT_ROLE | read DIM ✅ · write DECISIONS ✅ (insert, rolled back) · write FEATURES **denied** · read RAW **denied** |
| BI_ROLE | read DECISIONS ✅ · read FEATURES ✅ · read RAW **denied** |

### ⚠ Gap discovered and fixed (not in the repo recipe yet)

`snowflake/rbac.sql` grants `COMPUTE_WH` USAGE to **BI_ROLE and AGENT_ROLE only — not PIPELINE_ROLE**. The
metadata-served `COUNT(*)` probe hides this, but real DML (the generator's DIM writes, feature_engine's
FEATURES writes) needs the warehouse, and failed until fixed. I granted it live:

```sql
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PIPELINE_ROLE;
```

**Recommended repo fix:** add that line to `snowflake/rbac.sql` alongside the BI/AGENT warehouse grants, so a
future fresh-account rebuild doesn't hit the same wall. (Applied to `MK46267` already; the repo file still lacks it.)

## Step 4 — Data repopulation (bounded regeneration)

**The Kafka backfill was gone.** Despite the `kafka_data` Docker volume persisting at 549 MB, the topic's
**earliest offset == latest offset** on every partition — i.e. **zero messages retained**. The original
backfill (Jul 2) had aged out under the topic's ~7-day retention (~25 days elapsed). The 549 MB is Kafka
segment/metadata + `__consumer_offsets`, not transaction data. A reprocess would have consumed nothing.

Per instruction, this was surfaced before regenerating; **bounded regeneration** was chosen. What was done:

- **DIM:** ran `user_profile_generator` fresh → **10,000 users / 20,083 devices** written under PIPELINE_ROLE,
  and `user_map.json` refreshed. (Note: regenerating fresh — rather than reloading the old `user_map.json` —
  because that cache lacks several DIM columns *and* the old user identities carry no value now: nothing
  references them, and transactions were regenerated too. The old cache was backed up first.)
- **Code change (approved):** added an opt-in `FEATURE_WRITE_MODE` env toggle to `feature_engine.py`. Default
  `"snowpipe"` (unchanged production path); `"direct"` writes each micro-batch straight to
  `FACT_FEATURE_SNAPSHOTS` via the existing `SnowflakeFeatureWriter` (executemany), needing **no Snowpipe
  pipe/stage/integration** — which don't exist on the new account and aren't reconstructable from the repo
  (see "Not recoverable"). Two small edits: a config flag + a branch in `process_batch`.
- **Transactions:** produced a **25,000-transaction** bounded backfill to Kafka. *(First attempt produced
  100% fraud — `BACKFILL_NUM_BURSTS=3000` is tuned for the 10M backfill and swamped a 25K budget; reset the
  topic and regenerated with `num_bursts=150` → realistic **19.6% fraud / 80.4% legit**.)*
- **Feature reprocess:** ran `feature_engine` with `FEATURE_WRITE_MODE=direct` (local Spark) → computed and
  wrote **25,000 feature snapshots** (2,177 flagged) directly to the new account, plus **9,052 user keys** to
  the Redis online store.

## Step 5 — Verification (all pass, end-to-end on the new account)

- **Single-agent demo:** pulled a real flagged txn, ran the ReAct loop (Weaviate policy search re-ingested —
  23 chunks — + tools + Groq) → BLOCK / 0.95. Correctly flagged the fraud (pattern call AMOUNT_ANOMALY vs
  ground-truth NEW_DEVICE — the documented overlap case; the txn had both a high z-score and a new device).
- **Multi-agent demo:** orchestrator routed feature → policy → decision, **skipped risk_agent**, guardrail
  (policy-before-decision) held → BLOCK / 1.0.
- **BI / NL2SQL under BI_ROLE:** `open_bi_connection` confirmed `BI_ROLE` with secondary roles empty; the
  agent generated guarded SQL via Groq, executed as BI_ROLE, returned the correct flagged count (2,177). RAW
  reads rejected by the guard.
- **eval_runner smoke (2 txns, stratified):** full stack ran — orchestrate → govern → persist → trace →
  LLM judge; 2 decisions + 16 trace rows persisted to `DECISIONS.*`.

---

## Not recoverable (permanent data loss)

- **`DECISIONS` history from the old account** — every past agent decision, HITL review, and Phase 5/6/7
  audit trace is **gone and unrecoverable**. The new account's `DECISIONS` tables start empty (now holding
  only the 2 decisions + 16 traces from the Step 5 eval smoke test).
- **The original 1M-row Kafka backfill** — aged out by retention, not recoverable. The current data is
  freshly regenerated synthetic data (new users, new transactions), not the original.
- **Snowpipe ingestion objects** (storage integration, external stage, pipe, S3/SQS notification) — were
  account-specific, are not in the repo, and were **not** recreated. The direct-write path is used instead.

## Current state — new account vs old

| | Old account (pre-expiry) | New account `MK46267` (now) |
|---|---|---|
| Schema/tables | present | ✅ recreated identically |
| RBAC roles + proofs | present | ✅ recreated + re-proven (+ PIPELINE warehouse grant fixed) |
| DIM_USERS / DIM_DEVICES | 10,000 / ~20,083 | 10,000 / 20,083 (new identities) |
| RAW.FACT_TRANSACTIONS | ~1M | **0** (backfill went to Kafka only; RAW audit-copy not repopulated — agents/BI are denied RAW anyway) |
| FEATURES.FACT_FEATURE_SNAPSHOTS | ~1M (~44%→26% flagged) | 25,000 (2,177 flagged, 4,893 truth-fraud) |
| DECISIONS.FACT_DECISIONS / TRACES | accumulated history | 2 / 16 (from Step 5 smoke only) |
| Snowpipe ingestion | wired | not present (direct-write used) |

## Code / repo changes in this commit

1. `fraud_platform/stream_processing/feature_engine.py` — added `FEATURE_WRITE_MODE` config flag (default
   `"snowpipe"`, unchanged behavior) + a `direct` branch in `process_batch`.
2. `snowflake/rbac.sql` — added `GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PIPELINE_ROLE;` (the gap found
   live on MK46267 — now fixed in the repo, not just on the account).
3. `README.md` + `fraud_platform/db/migrate.py` docstring — corrected the fresh-account bootstrap order to
   **schema.sql → rbac.sql → migrate.py** (the previously-documented schema.sql → migrate.py order fails,
   because migrations V002/V004 grant to roles that `rbac.sql` creates — this session hit exactly that error).
4. `fraud_platform/data_generator/transaction_stream_generator.py` — added a `--bursts` CLI flag and made the
   default scale proportionally to `--num`, so a small bounded backfill can't silently come out ~100% fraud
   (yields exactly `BACKFILL_NUM_BURSTS` at the full default `--num`).

All Python files compile (`py_compile`).

## Environment left running

Docker stack (kafka, redis, weaviate, spark-master/worker) is **up**. Stop with `docker compose down` when
done. The old `user_map.json` was backed up to the session scratchpad before regeneration.

Committed and pushed on branch `ops/snowflake-account-migration` (based off `harden/audit-fixes`, the
current code tip — `main` predates the `fraud_platform/` repackaging and lacks this code). Not merged.
