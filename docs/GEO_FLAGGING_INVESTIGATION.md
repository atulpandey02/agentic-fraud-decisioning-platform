# GEO_JUMP Over-Flagging Investigation

*2026-07-05. All numbers measured against the live
FEATURES.FACT_FEATURE_SNAPSHOTS table (999,878 rows, 17.3% ground-truth
fraud, 44.4% flagged). Reconstruction of the scoring logic in SQL matched the
stored `is_flagged_for_review` on 999,877 of 999,878 rows (1 float-boundary
mismatch), so the attribution below is trustworthy.*

## Symptom

Flag rate 44.4% against 17.3% injected fraud — overall flag precision 33.3%.
First noticed as a hunch in the Phase 1 review; confirmed as a measured error
by the Phase 6 eval batch (a legitimate transaction wrongly BLOCKed as
GEO_JUMP by an agent given a corrupted geo feature).

## Attribution: which trigger produces the flags

| trigger (exclusive)  | rows      | frauds  | precision |
|----------------------|-----------|---------|-----------|
| geo_hard only        | 331,620   | 57,477  | **17.3%** |
| amt_hard only        | 56,796    | 55,009  | 96.9%     |
| multiple triggers    | 53,133    | 35,251  | 66.3%     |
| vel_hard only        | 2,232     | 48      | 2.2%      |
| weighted only        | 25        | 7       | 28.0%     |

The geo hard rule (`geo_distance_km >= 475`) alone produces **75% of all
flags at 17.3% precision** — essentially re-labeling the base rate. The
amount hard rule, for contrast, is nearly perfect (96.9%). The geo rule is
the over-flagging, full stop. (`vel_hard only` at 2.2% precision over a small
n is a separate, smaller problem — see "Out of scope" below.)

## Root cause — three layers, two of them data corruption

**1. The threshold is semantically wrong.** 475 km is *domestic travel*
distance (Boston→NYC ~300 km, Boston→Chicago ~1,400 km), not impossible
travel. The production concept this rule descends from is **implied speed >
900 km/h** — distance divided by time — but the rule as shipped ignores
`time_since_last_txn_min` entirely, even though the pipeline computes it.

**2. Out-of-order processing corrupts the geo state.** The 10M-row backfill
randomizes event timestamps across 30 days, but Kafka delivers in produced
order and the Redis `user:{id}:location` state is updated in *processing*
order. "Distance from last seen location" is therefore frequently a distance
from a location the user visits *in the future*. Measured: **80% of
legitimate rows with geo_distance ≥ 475 km have a NEGATIVE time delta** —
the smoking gun (234,075 of 291,916 rows).

**3. Fraud poisons the location state.** A GEO_JUMP fraud in London becomes
the user's new "last location"; their next legitimate purchase back home in
Boston then computes as a second 5,265 km jump. Measured: **99% of users with
far-legit rows (9,927 of 10,000) also carry a GEO_JUMP fraud.** One injected
fraud manufactures a chain of false flags.

Why this also inflated *other* patterns' apparent recall: corrupted geo
distances pushed unrelated rows over the hard rule, so VELOCITY_SPIKE's
"recall" (36.1%) was partly geo-flagging by accident. Fixing geo makes other
weaknesses honest — expect *reported* recall for other patterns to drop
toward their true values.

## Fix options measured (simulated over all 999,878 historical rows)

| rule | flag rate | precision | recall | GEO_JUMP recall |
|---|---|---|---|---|
| current (dist ≥ 475) | 44.4% | 33.3% | 85.5% | 99.2% |
| speed > 900 only | 11.7% | 78.8% | 53.1% | **8.8%** ← unusable: 80% of rows (and 38,699/48,304 GEO_JUMP frauds) lack a usable time delta in this dataset |
| speed > 900 OR dist ≥ 2000 | 32.7% | 43.6% | 82.4% | 98.9% |
| **speed > 900 OR dist ≥ 3000** | **26.6%** | **52.6%** | **80.9%** | **98.9%** |
| speed > 900 OR dist ≥ 4000 | 23.0% | 60.0% | 79.9% | 98.7% |

## Approved fix (implemented in this commit)

Three parts — the rule change alone treats the symptom; the state-hygiene
changes remove the corruption that made the old rule look worse than its
design:

1. **Composite hard rule.** `geo_hard` fires when *either* implied speed >
   900 km/h (computed only when the time delta is positive, with a 100 km
   distance floor so meter-scale GPS noise over second-scale deltas can't
   fabricate speeds) *or* distance ≥ 3,000 km (true intercontinental-jump
   scale: the generator's minimum injected jump is 5,000 km; normal domestic
   travel tops out ~500 km — 3,000 splits them with margin on both sides).
   The graded `geo_signal` (distance/500, weight 0.25) in the weighted score
   is unchanged — as a *contributing* signal, moderate distance is still
   legitimately suspicious-ish.
2. **Ordering guard.** A non-positive time delta means the "previous"
   location is actually in the event-time future; the geo features are
   meaningless and now stay NULL (prev_city/prev_ts kept for provenance).
3. **Poisoning guard.** A transaction that itself trips the geo hard rule no
   longer overwrites the Redis location state — the last *trusted* location
   remains the comparison baseline, so one fraud can't manufacture a chain of
   false flags. Trade-off, documented in code: a user who genuinely relocates
   at jump speed updates their baseline only after a non-flagged transaction.

**Simulated impact on historical data (rule change alone): flag rate 44.4% →
26.6%, precision 33.3% → 52.6%, GEO_JUMP recall essentially unchanged (99.2%
→ 98.9%).** The two hygiene guards cannot be simulated in SQL (they change
the state *sequence*, not a per-row formula); on hygiene-clean data the
distance tail that produces the remaining false positives largely disappears,
so 26.6%/52.6% is the *floor* of the improvement, not the estimate.

Historical rows are not rewritten — FACT_FEATURE_SNAPSHOTS is an append-only
audit of what was computed at the time. New data benefits going forward.

## Out of scope, logged for later

- **VELOCITY_SPIKE true recall is weak** (36.1% measured, and much of that
  was accidental geo-flagging). Suspected: velocity window state vs backfill
  ordering, and the burst window (90s) vs the 15-min sliding window's 1-min
  slide. Deserves its own investigation before touching thresholds.
- `vel_hard only` precision (2.2%) is part of the same question.
- Rewriting historical `is_flagged_for_review` — deliberately not done.
