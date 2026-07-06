# NEW_DEVICE

## Case Study

A user with an average transaction amount of $32 made a $148
purchase from a device never seen before on their account, at an
electronics merchant. The device signal alone contributed only its
weighted 0.20 to the overall score and would not have triggered a
flag by itself. The transaction was ultimately flagged because the
amount was more than four times the user's normal spend, which
crossed the amount-anomaly hard-rule threshold independently. This
illustrates the intended behavior of this pattern: the new-device
signal did not do the catching, the amount signal did, with the new
device serving only as corroborating context in the final
explanation. A contrasting case from the same week: a different
user made a $28 purchase — close to their own $30 average — from an
unrecognized device, and the transaction was correctly allowed,
since a device change with a normal amount and no other elevated
signal is common, legitimate behavior, not fraud.