# AMOUNT_ANOMALY

## Case Study

A user with a typical transaction amount around $60 had two
transactions of exactly $1.00 and $5.00 in quick succession at
merchants with generic names, followed six minutes later by an
attempted $340 purchase from the same device and location. The two
round, tiny amounts were classic card-testing behavior — a
fraudster verifying the card still worked before attempting a real
purchase. Both small transactions produced a z-score whose absolute
value exceeded 3, since they were far below the user's normal
spend, and were auto-flagged under the amount hard-rule override
before the larger $340 attempt was ever processed. This is the
intended sequence: catching the small testing transactions early
is what prevents the larger fraudulent purchase from succeeding at
all, rather than only detecting fraud after the meaningful loss has
already occurred.