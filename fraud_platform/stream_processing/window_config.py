# =============================================================
# WINDOW CONFIGURATION
# =============================================================
# Single source of truth for all sliding-window definitions used
# across the streaming layer. Both feature_engine.py (the main
# pipeline) and velocity_engine.py (the standalone velocity query)
# import from here — this file exists specifically so the two
# don't silently drift apart on window duration or thresholds.
#
# Why velocity_engine.py is a SEPARATE FILE from this one:
#   This module holds CONFIGURATION (durations, thresholds) —
#   plain constants, no Spark session, no Kafka connection, no
#   computation logic. velocity_engine.py is the ENGINE that
#   consumes these constants to actually build and run a Spark
#   Structured Streaming query. Keeping config and engine logic
#   separate means changing a threshold never requires touching
#   engine code, and vice versa.
# =============================================================

# -------------------------------------------------------------
# EVENT-TIME WATERMARK
# How long Spark waits for late-arriving events (by transaction_ts)
# before finalizing a window and emitting its result. Applies to
# every window definition below, in both engines.
# -------------------------------------------------------------
WATERMARK_DELAY = "10 minutes"

# -------------------------------------------------------------
# SLIDING WINDOW DEFINITIONS
# (window_duration, slide_duration, output_column_name)
#
# Sliding, not tumbling — a fraud velocity check asks "how many
# transactions in the last 15 minutes FROM RIGHT NOW", which
# requires overlapping windows. Tumbling windows would split
# events 2 minutes apart into different non-overlapping buckets,
# breaking the signal entirely (established Day 1).
# -------------------------------------------------------------
VELOCITY_WINDOWS = [
    ("5 minutes",  "1 minute",   "velocity_5min"),
    ("15 minutes", "1 minute",   "velocity_15min"),
    ("1 hour",     "5 minutes",  "velocity_1hr"),
    ("24 hours",   "30 minutes", "velocity_24hr"),
]

# -------------------------------------------------------------
# VELOCITY FLAG THRESHOLD
# Real production reference point (JP Morgan-style velocity rule,
# researched and applied Day 2): more than 5 transactions in a
# 15-minute window is treated as suspicious. Our synthetic burst
# generator fires 8-10 transactions per burst specifically to
# land clearly above this threshold, not borderline.
# -------------------------------------------------------------
VELOCITY_FLAG_THRESHOLD = 5

# -------------------------------------------------------------
# PRIMARY VELOCITY WINDOW (used by the standalone velocity engine)
# The 15-minute window is the one with a real, sourced production
# threshold behind it, so it's the first one implemented end-to-end
# in velocity_engine.py. The other three windows in VELOCITY_WINDOWS
# above follow once the 15-min window pattern is verified working.
# -------------------------------------------------------------
PRIMARY_WINDOW_DURATION = "15 minutes"
PRIMARY_SLIDE_DURATION  = "1 minute"
PRIMARY_WINDOW_COLUMN   = "velocity_15min"

# -------------------------------------------------------------
# TRIGGER INTERVAL
# How often each streaming query checks Kafka for new messages
# and processes a micro-batch. Shared default across engines;
# override per-engine only if there's a specific reason to.
# -------------------------------------------------------------
DEFAULT_TRIGGER_INTERVAL = "30 seconds"