# GEO_JUMP

## Definition

A geo-jump occurs when a user's transaction location changes by a
distance that would be physically impossible to travel in the time
elapsed since their previous transaction. This is commonly called
impossible travel detection in the fraud industry, and it is one of
the strongest single-signal indicators available, because there is
essentially no legitimate explanation for a person transacting in
two distant cities minutes apart.

Distance between the current and previous transaction location is
computed using the Haversine formula, which calculates the shortest
great-circle distance between two points on a sphere. A simple flat
Euclidean distance on latitude and longitude would be inaccurate at
this scale, since it does not account for the Earth's curvature.

## Detection Threshold

The platform computes implied travel speed as distance divided by
elapsed time. A speed greater than 900 kilometers per hour is
treated as impossible travel. This threshold is grounded in a
concrete physical fact: commercial jets cruise at approximately 925
kilometers per hour, so any implied speed above roughly that level
cannot be explained by any legitimate mode of travel.

For risk scoring purposes, distances are normalized against a
baseline of 500 kilometers, representing normal domestic travel.
Geo-distance carries a weight of 0.25 in the overall risk score.

A hard-rule override exists for this pattern: if the normalized
geo-distance signal reaches 0.95 or higher — meaning the distance
involved is close to or exceeds the 500 kilometer normalization
baseline several times over — the transaction is flagged
automatically, independent of any other signal.

## Why This Threshold

A pure geo-jump, by design, elevates only the geo-distance signal
and leaves amount, device, and velocity looking completely normal.
Under a same-weighted additive scoring formula, geo-distance alone
can contribute at most its own weight of 0.25 toward the overall
score — nowhere near enough to cross a combined flag threshold on
its own, no matter how extreme the actual distance involved is.

This was a measured, confirmed gap: before the hard-rule override
was introduced, this pattern's catch rate was in the single digits,
because a transaction on the other side of the world with a
perfectly normal amount and a known device simply never
accumulated enough combined score to be flagged. The hard-rule
override exists specifically to correct this — a signal this
extreme does not need corroboration from an unrelated dimension to
be treated as suspicious. After the fix, this became the platform's
best-performing pattern, with the large majority of true geo-jump
fraud caught.

## Response Procedure

- Implied travel speed above 900 km/h: auto-flag for review,
  regardless of amount, device, or velocity signals.
- Distance greater than 500 km with a plausible elapsed time (for
  example, consistent with a commercial flight): treat as
  legitimate travel, not fraud, unless another signal is also
  elevated.
- Distance within normal domestic range: allow.
- When flagging, always surface both the distance and the implied
  speed together — distance alone can be explained by travel;
  implied speed exceeding physical transportation limits is the
  actual fraud signal.