# AMOUNT_ANOMALY

## Definition

An amount anomaly occurs when a transaction's value deviates
significantly from a user's normal spending pattern. This platform
tracks each user's historical average transaction amount and
standard deviation, and measures new transactions against that
personal baseline rather than a single fixed dollar threshold —
what counts as anomalous for one user may be completely ordinary
for another.

Three distinct sub-patterns fall under this category, and they look
different in practice:

- Card testing: fraudsters verify a stolen card works by making one
  or more very small, suspiciously round purchases (for example,
  exactly $1.00, $5.00, or $10.00) before attempting a larger
  purchase. Real purchases are rarely round numbers — a genuine
  coffee purchase is more likely to be $4.73 than $5.00.
- Threshold evasion: fraudsters deliberately keep amounts just
  under common review thresholds (for example, $99.99 instead of
  $100.00, or $499.99 instead of $500.00), anticipating that round
  thresholds are what gets extra scrutiny.
- Large deviation: a transaction far above the user's typical
  spend, unrelated to round numbers — simply an amount well outside
  their normal range.

## Detection Threshold

The platform computes a z-score for each transaction: the
difference between the transaction amount and the user's average,
divided by their standard deviation. A z-score with an absolute
value greater than 3 — three standard deviations from the user's
normal spending — is the baseline threshold for this pattern.

Amount anomaly carries a weight of 0.25 in the overall risk score.

A hard-rule override exists here as well: if the normalized amount
signal reaches 0.95 or higher — meaning the z-score is close to or
exceeds the threshold of 3 by a wide margin — the transaction is
flagged automatically, independent of any other signal.

## Why This Threshold

Like geo-jump, amount anomaly is frequently a pure single-signal
pattern — a card-testing transaction or a threshold-evasion
transaction typically looks completely normal on velocity, device,
and location, with only the amount itself being unusual. The same
structural limitation that affected geo-jump applied here before
the hard-rule override was introduced: an extreme amount deviation
alone could not cross a same-weighted combined threshold, so
catch rates for this pattern were very low under the additive
formula by itself.

After the hard-rule override was added, this became one of the
platform's strongest-performing patterns, with the large majority
of true amount-anomaly fraud caught. The z-score-based large
deviation sub-pattern in particular saturates this signal reliably,
since fraud amounts in that sub-pattern are deliberately generated
several times above the user's normal spend.

## Response Procedure

- Z-score magnitude at or above 3: auto-flag for review, regardless
  of other signals.
- Round-number amount under $20, on a user whose typical spend is
  meaningfully higher: treat as a likely card-testing attempt even
  if the raw z-score is borderline — round small amounts are a
  strong independent signal in their own right.
- Amount just under a common threshold ($99.99, $499.99, $999.99):
  treat with elevated suspicion, particularly if the user's normal
  spend pattern does not typically approach that threshold.
- Amount within 1–2 standard deviations of the user's normal
  spend: allow.