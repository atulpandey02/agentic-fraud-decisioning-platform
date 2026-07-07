# =============================================================
# UNIT TESTS — resilience primitives (Priority 5 item 1)
# =============================================================
# sleep and clock are injected, so retries and breaker timing are
# tested instantly and deterministically — no real sleeping.
# =============================================================

import pytest

from fraud_platform.common.resilience import (
    classify_error, retry, CircuitBreaker, CircuitOpenError,
    TRANSIENT, PERMANENT,
)


class TestClassifyError:
    @pytest.mark.parametrize("exc", [
        TimeoutError("operation timed out"),
        ConnectionError("connection reset by peer"),
        RuntimeError("Rate limit reached (429)"),
        RuntimeError("Service temporarily unavailable"),
    ])
    def test_transient(self, exc):
        assert classify_error(exc) == TRANSIENT

    @pytest.mark.parametrize("exc", [
        PermissionError("unauthorized"),
        ValueError("invalid identifier"),
        RuntimeError("SQL syntax error near"),
        RuntimeError("relation does not exist"),
    ])
    def test_permanent(self, exc):
        assert classify_error(exc) == PERMANENT

    def test_permanent_wins_over_transient_markers(self):
        # an auth error that also says 'connection' must not be retried
        assert classify_error(RuntimeError("invalid credentials on connection")) == PERMANENT

    def test_unknown_defaults_permanent(self):
        assert classify_error(RuntimeError("something weird")) == PERMANENT


class TestRetry:
    def test_succeeds_first_try(self):
        calls = []

        @retry(sleep=lambda d: None)
        def f():
            calls.append(1)
            return "ok"
        assert f() == "ok"
        assert len(calls) == 1

    def test_retries_transient_then_succeeds(self):
        calls = []

        @retry(max_attempts=3, sleep=lambda d: None)
        def f():
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("connection dropped")
            return "ok"
        assert f() == "ok"
        assert len(calls) == 3

    def test_gives_up_after_max_attempts(self):
        calls = []

        @retry(max_attempts=3, sleep=lambda d: None)
        def f():
            calls.append(1)
            raise TimeoutError("timed out")
        with pytest.raises(TimeoutError):
            f()
        assert len(calls) == 3

    def test_permanent_not_retried(self):
        calls = []

        @retry(max_attempts=5, sleep=lambda d: None)
        def f():
            calls.append(1)
            raise PermissionError("unauthorized")
        with pytest.raises(PermissionError):
            f()
        assert len(calls) == 1  # no retries

    def test_backoff_delays_are_exponential(self):
        delays = []

        @retry(max_attempts=4, base_delay=0.1, backoff=2.0, sleep=delays.append)
        def f():
            raise ConnectionError("timeout")
        with pytest.raises(ConnectionError):
            f()
        assert delays == [0.1, 0.2, 0.4]  # 3 sleeps before the 4th (final) attempt


class TestCircuitBreaker:
    def test_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3, reset_timeout=10, clock=lambda: 0.0)

        def boom():
            raise ConnectionError("down")
        for _ in range(3):
            with pytest.raises(ConnectionError):
                cb.call(boom)
        assert cb.state == cb.OPEN
        # now fails fast without calling through
        with pytest.raises(CircuitOpenError):
            cb.call(boom)

    def test_half_open_after_cooldown_then_closes_on_success(self):
        now = {"t": 0.0}
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=5, clock=lambda: now["t"])

        def boom():
            raise ConnectionError("down")
        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(boom)
        assert cb.state == cb.OPEN

        now["t"] = 6.0  # cooldown elapsed
        assert cb.state == cb.HALF_OPEN
        assert cb.call(lambda: "recovered") == "recovered"  # probe succeeds
        assert cb.state == cb.CLOSED

    def test_half_open_failure_reopens(self):
        now = {"t": 0.0}
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=5, clock=lambda: now["t"])

        def boom():
            raise ConnectionError("down")
        with pytest.raises(ConnectionError):
            cb.call(boom)
        assert cb.state == cb.OPEN
        now["t"] = 6.0
        assert cb.state == cb.HALF_OPEN
        with pytest.raises(ConnectionError):
            cb.call(boom)  # probe fails
        assert cb.state == cb.OPEN

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=3, clock=lambda: 0.0)

        def boom():
            raise ConnectionError("down")
        with pytest.raises(ConnectionError):
            cb.call(boom)
        cb.call(lambda: "ok")  # success resets
        # two more failures should NOT open (count was reset)
        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(boom)
        assert cb.state == cb.CLOSED
