# VELOCITY_SPIKE

## Definition

A velocity spike occurs when a single user makes an unusually high
number of transactions within a short rolling time window. This is
one of the oldest and most reliable fraud signals, because a
compromised card is frequently used in rapid succession by a
fraudster testing its limits before it gets blocked, or draining
value from it as fast as possible.

The platform computes this as a 15-minute sliding window count per
user, recomputed every minute as new transactions arrive. A sliding
window is used instead of a fixed (tumbling) window because fraud
does not respect clock boundaries — two transactions two minutes
apart must be evaluated together even if they happen to fall in
different fixed 15-minute buckets.

## Detection Threshold

The platform flags a user once their 15-minute transaction count
exceeds 5. This threshold is based on a JP Morgan-style production
velocity rule researched during platform design: more than 5
transactions in 15 minutes is treated as suspicious in real banking
fraud systems, since a normal cardholder rarely transacts that
frequently in such a short window.

In the risk scoring formula, velocity carries a weight of 0.30 —
the highest individual weight of any signal — reflecting that
velocity is considered the single most reliable fraud indicator
when it fires cleanly.

As of the most recent model version, a hard-rule override also
exists: if the 15-minute count reaches the full threshold of 5 or
more, the transaction is flagged automatically regardless of any
other signal, rather than requiring the weighted score to also
cross the overall flag threshold.

## Why This Threshold

Two important caveats apply to this pattern specifically, and any
agent reasoning about a velocity-flagged transaction should account
for both.

First, velocity is a same-weighted additive signal combined with
amount, geo-distance, and device signals. A pure velocity spike,
with nothing else unusual about the transaction, previously could
not reliably cross the overall flag threshold on its own — this is
exactly why the hard-rule override was added.

Second, and more structurally important: a transaction is scored
using only the window as it exists at the moment that specific
transaction arrives. Early transactions within a still-forming
burst are evaluated before the rest of the burst has happened yet,
so they show artificially low velocity counts even though the
burst will eventually exceed the threshold. Measured catch rate for
this pattern is meaningfully lower than the platform's other three
fraud patterns for exactly this reason — it is a timing limitation
in the architecture, not a miscalibrated threshold. An agent should
treat a borderline velocity signal on an early transaction with
more suspicion than the raw score alone suggests, since later
transactions in the same burst are structurally more likely to be
caught than earlier ones.

## Response Procedure

- Velocity count at or above 5 in 15 minutes: auto-flag for review,
  regardless of any other signal.
- Velocity count between 3 and 4 in 15 minutes, combined with any
  other elevated signal (new device, amount anomaly, geo distance):
  escalate for human review rather than auto-allow.
- Velocity count below 3 with no other elevated signal: allow.
- If a velocity flag fires on what appears to be an early
  transaction in a fast-arriving sequence for the same user,
  consider placing a short hold pending the next few transactions
  rather than immediately allowing or blocking, since the full
  picture may not yet be visible.