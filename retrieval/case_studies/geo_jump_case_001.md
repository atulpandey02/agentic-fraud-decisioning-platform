# GEO_JUMP

## Case Study

A user whose transaction history was entirely concentrated in
Boston made a transaction in London approximately forty minutes
after their previous transaction in Boston. The Haversine distance
between the two points was roughly 5,265 kilometers, producing an
implied travel speed far in excess of 900 km/h — physically
impossible even by direct commercial flight, which would require
several hours minimum plus airport time on both ends. The
transaction was auto-flagged immediately under the hard-rule
override, with no other signal elevated: the amount was within the
user's normal range, the device was recognized, and there was no
velocity concern. This is the clean, textbook case the geo-jump
pattern is designed to catch — a single extreme signal with nothing
else corroborating it, which is exactly the scenario the hard-rule
override exists to handle without needing a second signal to also
fire.