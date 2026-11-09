"""Tests for polite FIDE access: rate limiting, slowdown, block detection."""

from __future__ import annotations

import pytest

from fide_http import (
    AdaptiveRateLimiter,
    FideBlocked,
    PoliteFetcher,
    is_block_error,
    is_strain_error,
)

CONNECT_TIMEOUT = (
    "timeout: HTTPSConnectionPool(host='ratings.fide.com', port=443): Max retries "
    "exceeded (Caused by ConnectTimeoutError(..., 'Connection to ratings.fide.com "
    "timed out. (connect timeout=15)'))"
)
REFUSED = (
    "connection error: HTTPSConnectionPool(host='ratings.fide.com', port=443): "
    "Max retries exceeded (Caused by NewConnectionError('Failed to establish a new "
    "connection: [Errno 111] Connection refused'))"
)
READ_TIMEOUT = (
    "timeout: HTTPSConnectionPool(host='ratings.fide.com', port=443): "
    "Read timed out. (read timeout=45)"
)


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


def _limiter(clock: FakeClock, rate: float = 0.5, **kw) -> AdaptiveRateLimiter:
    return AdaptiveRateLimiter(rate, clock=clock, sleep=clock.sleep, **kw)


def _fetcher(clock: FakeClock, limiter: AdaptiveRateLimiter) -> PoliteFetcher:
    return PoliteFetcher(limiter, sleep=clock.sleep, clock=clock)


def _script(*errors):
    """A fetch that returns the given errors in order (None = success)."""
    calls = []

    def fetch(max_attempts: int):
        assert max_attempts == 1  # inner retries must not bypass the limiter
        err = errors[len(calls)]
        calls.append(err)
        return ({"ok": True} if err is None else None), err, 1, None

    return fetch, calls


@pytest.mark.parametrize("err", [CONNECT_TIMEOUT, REFUSED])
def test_block_errors(err):
    assert is_block_error(err)
    assert not is_strain_error(err)


@pytest.mark.parametrize(
    "err",
    [
        "HTTP 502",
        "HTTP 429",
        READ_TIMEOUT,
        "network error: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))",
    ],
)
def test_strain_errors(err):
    assert is_strain_error(err)
    assert not is_block_error(err)


@pytest.mark.parametrize(
    "err", [None, "no data found", "report_updated_or_replaced", "HTTP 404"]
)
def test_neither(err):
    assert not is_block_error(err)
    assert not is_strain_error(err)


def test_limiter_spaces_requests():
    clock = FakeClock()
    lim = _limiter(clock, rate=0.5)
    lim.wait()
    lim.wait()
    clock.t += 0.5  # time spent on the request
    lim.wait()
    assert clock.sleeps == [2.0, 1.5]


def test_unlimited_never_sleeps():
    clock = FakeClock()
    lim = _limiter(clock, rate=0)
    for _ in range(5):
        lim.wait()
    lim.slow_down("x")
    assert clock.sleeps == []
    assert lim.rate == 0


def test_slowdown_halves_and_recovers_to_target():
    clock = FakeClock()
    lim = _limiter(clock, rate=0.4, recover_after=10)
    lim.slow_down("HTTP 502")
    assert lim.rate == pytest.approx(0.2)
    for _ in range(10 * 5):
        lim.record_ok(1.0)
    assert lim.rate == pytest.approx(0.4)  # capped at target


def test_slowdown_floor():
    clock = FakeClock()
    lim = _limiter(clock, rate=0.4, min_rate=0.05)
    for _ in range(10):
        lim.slow_down("x")
    assert lim.rate == pytest.approx(0.05)


def test_latency_climb_triggers_slowdown():
    clock = FakeClock()
    lim = _limiter(clock, rate=0.4, latency_baseline_n=5, latency_floor_s=2.0)
    for _ in range(5):
        lim.record_ok(1.0)
    for _ in range(60):
        lim.record_ok(8.0)
    assert lim.slowdowns >= 1
    assert lim.rate < 0.4


def test_block_waits_once_then_raises():
    clock = FakeClock()
    fetch, calls = _script(CONNECT_TIMEOUT, REFUSED)
    with pytest.raises(FideBlocked):
        _fetcher(clock, _limiter(clock)).fetch("details 1", fetch)
    assert len(calls) == 2
    assert 30.0 in clock.sleeps


def test_block_then_success_continues():
    clock = FakeClock()
    fetch, calls = _script(CONNECT_TIMEOUT, None)
    data, err, _, _ = _fetcher(clock, _limiter(clock)).fetch("details 1", fetch)
    assert data == {"ok": True} and err is None


def test_strain_slows_down_and_retries():
    clock = FakeClock()
    lim = _limiter(clock, rate=0.4)
    fetch, calls = _script("HTTP 502", READ_TIMEOUT, None)
    data, err, _, _ = _fetcher(clock, lim).fetch("report 1", fetch)
    assert err is None and len(calls) == 3
    assert lim.rate == pytest.approx(0.1)
    # every attempt went through the limiter (spaced, never a fast inner retry)
    assert lim.requests == 3


def test_strain_gives_up_and_returns_error():
    clock = FakeClock()
    fetch, calls = _script(*["HTTP 503"] * 4)
    data, err, _, _ = _fetcher(clock, _limiter(clock)).fetch("report 1", fetch)
    assert data is None and err == "HTTP 503"
    assert len(calls) == 4


def test_page_structure_error_returned_without_retry():
    clock = FakeClock()
    fetch, calls = _script("no data found")
    data, err, _, _ = _fetcher(clock, _limiter(clock)).fetch("details 1", fetch)
    assert err == "no data found" and len(calls) == 1
