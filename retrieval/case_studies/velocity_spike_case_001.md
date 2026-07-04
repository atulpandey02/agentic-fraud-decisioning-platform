# VELOCITY_SPIKE

## Case Study

A user with a stable history of two to three transactions per day,
averaging around $45 each, generated nine transactions within a
seventy-second window, all at electronics and gift card merchants.
The third transaction in the sequence crossed the five-per-fifteen-
minute threshold and was auto-flagged under the hard-rule override.
The first two transactions in the sequence were not flagged, since
the sliding window had not yet accumulated enough count to cross
the threshold at the time each of those two arrived — an example of
the early-transaction blind spot this pattern is known to have.
Investigation confirmed the account had been compromised via a
phishing link two days prior. Recommended handling: when a velocity
flag fires, retroactively review the one or two transactions
immediately preceding it from the same user, since they are likely
part of the same fraudulent burst even though they did not
individually cross the threshold.