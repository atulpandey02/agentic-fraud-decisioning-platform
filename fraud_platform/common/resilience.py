# =============================================================
# RESILIENCE — retries, circuit breaker, error classification
# =============================================================
# Priority 5 item 1. External services (Redis, Snowflake, Weaviate,
# Groq) fail in two very different ways, and the response must
# differ: a TRANSIENT failure (a dropped connection, a timeout, a
# rate limit) should be retried with backoff; a PERMANENT one (bad
# credentials, a syntax error, a constraint violation) must NOT be
# retried — retrying just burns time and quota to fail identically.
# classify_error() draws that line, and retry() acts on it.
#
# The circuit breaker adds the third response: when a dependency is
# hard down, stop hammering it. After N consecutive failures the
# breaker OPENS and fails fast for a cooldown, then HALF-OPENS to
# probe with a single call before closing again. This protects both
# us (no pile-up of doomed calls) and the struggling dependency.
#
# Everything here is pure and injectable (sleep and clock are
# parameters), so the retry timing and breaker state machine are
# unit-tested deterministically with no real sleeping or services.
#
# Note: per-TRANSACTION failure isolation — the other half of item 1
# — lives in the feature pipeline itself (scoring.build_feature_row +
# feature_engine's quarantine loop), where one bad row is dropped
# without failing the batch. That is a different concern (bad data,
# not a flaky service) and is proven by tests/test_enrichment.py.
# =============================================================

from __future__ import annotations

import time
import logging
import functools
from typing import Callable

logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# ERROR CLASSIFICATION
# -------------------------------------------------------------
TRANSIENT = "TRANSIENT"
PERMANENT = "PERMANENT"

# Substrings that, in an exception's type name or message, signal a
# retryable/transient condition. Kept as names/substrings (not imported
# exception classes) so this module has no hard dependency on redis /
# snowflake / groq being installed.
_TRANSIENT_MARKERS = (
    "timeout", "timed out", "connection", "connectionerror", "connectionreset",
    "temporarily", "unavailable", "rate limit", "ratelimit", "429",
    "503", "502", "504", "broken pipe", "reset by peer", "econnreset",
)
_PERMANENT_MARKERS = (
    "auth", "unauthorized", "forbidden", "permission", "invalid",
    "syntax", "does not exist", "not found", "constraint", "programmingerror",
)


def classify_error(exc: BaseException) -> str:
    """TRANSIENT (retryable) vs PERMANENT (don't retry). Checks the
    permanent markers FIRST — an 'invalid credentials' error that also
    happens to mention 'connection' must not be treated as retryable."""
    text = f"{type(exc).__name__} {exc}".lower()
    if any(m in text for m in _PERMANENT_MARKERS):
        return PERMANENT
    if any(m in text for m in _TRANSIENT_MARKERS):
        return TRANSIENT
    # Default: treat unknown errors as PERMANENT so we fail fast rather
    # than retry something that will never succeed.
    return PERMANENT


# -------------------------------------------------------------
# RETRY
# -------------------------------------------------------------
def retry(
    max_attempts: int = 3,
    base_delay: float = 0.1,
    backoff: float = 2.0,
    max_delay: float = 5.0,
    only_transient: bool = True,
    sleep: Callable[[float], None] = time.sleep,
):
    """
    Decorator: bounded retries with exponential backoff. Retries only
    errors classify_error() calls TRANSIENT (unless only_transient=False).
    `sleep` is injectable so tests run instantly and assert the delays.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 — classified below
                    last_exc = exc
                    kind = classify_error(exc)
                    if only_transient and kind != TRANSIENT:
                        logger.warning("Not retrying %s (%s): %s", fn.__name__, kind, exc)
                        raise
                    if attempt == max_attempts:
                        logger.error("%s failed after %d attempts: %s", fn.__name__, max_attempts, exc)
                        raise
                    logger.warning(
                        "%s attempt %d/%d failed (%s); retrying in %.2fs",
                        fn.__name__, attempt, max_attempts, kind, delay,
                    )
                    sleep(delay)
                    delay = min(delay * backoff, max_delay)
            raise last_exc  # pragma: no cover
        return wrapper
    return decorator


# -------------------------------------------------------------
# CIRCUIT BREAKER
# -------------------------------------------------------------
class CircuitOpenError(RuntimeError):
    """Raised when a call is rejected because the breaker is OPEN."""


class CircuitBreaker:
    """
    Trips OPEN after `failure_threshold` consecutive failures; rejects
    calls fast for `reset_timeout` seconds; then HALF-OPEN lets ONE
    probe through — success closes it, failure re-opens it. `clock` is
    injectable so the timing is unit-tested without real time.
    """

    CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0,
                 clock: Callable[[], float] = time.monotonic):
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._clock = clock
        self._failures = 0
        self._state = self.CLOSED
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        # Lazily transition OPEN -> HALF_OPEN once the cooldown elapses.
        if self._state == self.OPEN and self._clock() - self._opened_at >= self._reset_timeout:
            self._state = self.HALF_OPEN
        return self._state

    def call(self, fn: Callable, *args, **kwargs):
        state = self.state
        if state == self.OPEN:
            raise CircuitOpenError("Circuit is OPEN — failing fast.")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        self._on_success()
        return result

    def _on_success(self):
        self._failures = 0
        self._state = self.CLOSED

    def _on_failure(self):
        self._failures += 1
        if self._failures >= self._threshold or self._state == self.HALF_OPEN:
            self._state = self.OPEN
            self._opened_at = self._clock()
