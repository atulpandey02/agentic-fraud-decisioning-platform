# NEW_DEVICE

## Definition

A new-device signal fires when a transaction originates from a
device that is not on the user's list of trusted, previously-used
devices. A device switching on its own is common and often
completely legitimate — a new phone, a new browser, a borrowed
computer — so this signal is treated differently from the
platform's other three fraud patterns, and that difference is
deliberate.

## Detection Threshold

Device trust is a simple boolean check: is this device_id present
in the user's known trusted device list. There is no numeric
threshold to tune here, unlike velocity, geo-distance, or amount.

In the risk scoring formula, the new-device signal carries a weight
of 0.20 — the lowest weight of the four signals — and it is a
supporting signal only, not a standalone one.

## Why This Threshold

This is the one fraud pattern that deliberately does NOT have a
hard-rule override, and the reasoning matters for any agent
reasoning about a new-device flag.

Legitimate secondary devices are frequently marked as not fully
trusted in the platform's own device history — a user's second or
third device is only sometimes flagged as trusted, meaning a
completely ordinary transaction from a real, previously-used device
can still show as "new" under a strict trust check. Auto-flagging
on this signal alone would generate a meaningful volume of false
positives against entirely normal user behavior, which is why it
is kept as a weighted contributor rather than a standalone trigger.

In practice, new-device fraud is rarely caught by the device signal
in isolation. It is typically caught because genuine new-device
fraud also involves an unusually large transaction amount — a
stolen card used on an unfamiliar device is frequently used to
make one large purchase quickly, before the device or card is
flagged. When that happens, the amount-anomaly hard-rule override
fires on its own terms, and the new-device flag effectively rides
along as corroborating context rather than being the reason the
transaction was caught.

An agent should treat a new-device signal with no other elevated
signal as weak evidence at most — likely a legitimate device
change — and reserve real suspicion for a new device paired with an
unusual amount, unusual velocity, or unusual location.

## Response Procedure

- New device alone, with a normal transaction amount and no other
  elevated signal: allow, but log the device as newly seen for
  future reference.
- New device combined with an unusually large transaction amount:
  treat as a real fraud risk — the amount-anomaly indicator is
  doing the actual work here, not the device signal itself.
- New device combined with a geo-jump or velocity spike: treat as
  high risk; the combination is unlikely to be coincidental.
- Never auto-block solely on a new-device signal — always require
  at least one corroborating signal before escalating past a
  routine, low-friction verification step.