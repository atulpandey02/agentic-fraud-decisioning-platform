# Idempotent Replay Strategy

*Priority 2, item 4. Concrete artifacts: `snowflake/migrations/V003__feature_snapshots_staging.sql`
(staging table), `db/replay.py` (the MERGE), `tests/test_replay.py` (structure
tests), demonstrated idempotent on throwaway tables against the live account.*

## The problem

The feature-snapshot ingestion path is:

```
Kafka (earliest) → Spark → S3 Parquet → Snowpipe COPY → FACT_FEATURE_SNAPSHOTS
```

Every stage but the last is naturally re-runnable; the last is not. `COPY`
only **appends**. And the feature engine reads Kafka from `earliest`, so any
restart, backfill, or reprocess re-emits every transaction — each time with a
**freshly generated `snapshot_id`** (`str(uuid.uuid4())` per computation).
`COPY` therefore appends a second, third, Nth row for the same
`transaction_id`. Replays silently inflate the table, and every `COUNT`, flag
rate, and eval that groups by transaction is corrupted. (The forensic
duplicate-check the reviewer asked for on `FACT_DECISIONS` exists precisely
because this class of bug is easy to miss.)

## Why `transaction_id` is the identity, not `snapshot_id`

`snapshot_id` is the table's primary key, but it is **regenerated on every
run** — it identifies a *computation*, not a *transaction*. Keying dedup on it
would make every replay look brand new. `transaction_id` is the stable
business identity: "the current feature snapshot for this transaction." That
is the MERGE key.

## The design: stage → MERGE → truncate

1. **Load into staging.** Snowpipe/`COPY` targets
   `FEATURES.STG_FEATURE_SNAPSHOTS` (same shape as the fact table). Staging is
   append-only and disposable — it holds only the current load.
2. **MERGE into the fact table** (`db/replay.py::build_merge_sql`), keyed on
   `transaction_id`:
   - `WHEN MATCHED` → `UPDATE` the existing row (a replay refreshes, never
     duplicates).
   - `WHEN NOT MATCHED` → `INSERT`.
   - The MERGE **source is de-duplicated within the batch** by
     `QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY
     computed_at DESC) = 1` — keeping the latest computation per transaction.
     This is both correct ("latest wins") and *required*: Snowflake errors if a
     MERGE source matches one target row more than once.
3. **Truncate staging** after a successful MERGE (the loader owns this; a
   failed MERGE leaves staged rows for retry).

The two dedup layers correspond to the two ways replays create duplicates —
the same transaction appearing twice *within* one load (QUALIFY), and a
transaction already *in* the fact table (WHEN MATCHED).

## Why this is safe to run repeatedly

Running the exact same load twice produces the exact same table state:
matched rows are updated to identical values; no new rows appear. Demonstrated
on throwaway temp tables against the live account:

| step | rows | distinct txn | note |
|---|---|---|---|
| load 1 (txnA×2, txnB) | 2 | 2 | in-batch duplicate collapsed; latest amount won |
| replay (txnA, txnB again, new snapshot_ids) | 2 | 2 | **no duplicates** |
| *(plain COPY/append would be)* | *5* | *2* | *the bug this prevents* |

## Column drift protection

`build_merge_sql()` derives its column list from `db/schema_contract.py`, so
the MERGE cannot fall out of sync with the table — adding a column to the
contract flows into both the `UPDATE SET` and `INSERT` clauses automatically.
`tests/test_replay.py` asserts every contract column is present in the INSERT.

## What is intentionally NOT changed here

- The **live Spark pipeline still writes via S3→Snowpipe→FACT directly.**
  Re-pointing Snowpipe at the staging table and scheduling the MERGE (a task,
  or a post-load hook) is an operational cutover, not a code change, and it
  touches the running pipeline — out of scope for this schema-correctness
  priority and better done with the reliability work in Priority 5. The
  artifacts here (staging table, tested MERGE) are what that cutover will use.
- **Historical duplicates are not retro-deduped.** If any exist from past
  replays, a one-time `MERGE`/dedup pass keyed on `transaction_id` +
  `computed_at` closes them; deliberately not run now against the 1M-row table.
