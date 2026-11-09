"""
Polite access to ratings.fide.com: one rate limiter that every request and every
retry goes through, automatic slowdown on signs of strain, and a hard stop
(FideBlocked) when FIDE starts blocking our IP.

FIDE's throttling (CloudWatch, Mar–May 2026) is a per-IP firewall block: packets
silently dropped (connect timeouts) or connections refused, never HTTP 429.
Once an IP is blocked it essentially never recovers within a run, so grinding on
only wastes Lambda time and may extend the block. The rule here:

  block signal  -> wait 30s, try once more, then raise FideBlocked
                   (Step Functions waits 30min -> 1h -> 2h ... before retrying)
  strain signal -> halve the request rate, retry later through the limiter
                   (HTTP 5xx/429, read timeouts, dropped connections, latency
                   climbing well above its baseline)
  clean streak  -> creep the rate back up toward the configured target

Fetch functions (fetch_tournament_details / fetch_tournament_report) are called
with max_attempts=1 so their own fast inner retries cannot burst past the
limiter.
"""

from __future__ import annotations

import logging
import re
import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger(__name__)

FetchResult = Tuple[Optional[Any], Optional[str], int, Optional[bytes]]

# Connection never established: the firewall is dropping or refusing us.
_BLOCK_RE = re.compile(
    r"connect ?timeout|connecttimeout|errno 111|connection refused"
    r"|failed to establish a new connection",
    re.I,
)
# Server reachable but struggling or shedding load.
_STRAIN_RE = re.compile(
    r"http (429|5\d\d)|read timed out|read ?timeout|remotedisconnected"
    r"|remote end closed|connection reset|connection aborted|\beof\b|broken pipe",
    re.I,
)


class FideBlocked(RuntimeError):
    """FIDE is refusing or dropping our connections. Stop and wait; do not grind."""


def is_block_error(error: Optional[str]) -> bool:
    return bool(error) and bool(_BLOCK_RE.search(error))


def is_strain_error(error: Optional[str]) -> bool:
    return bool(error) and not is_block_error(error) and bool(_STRAIN_RE.search(error))


class AdaptiveRateLimiter:
    """
    Minimum spacing between requests (no bursting), with multiplicative
    slowdown on strain and gradual recovery after a clean streak.

    target_rate <= 0 means unlimited (local/dev use only); adaptation is off.
    """

    def __init__(
        self,
        target_rate: float,
        *,
        min_rate: float = 0.05,
        recover_after: int = 100,
        recover_factor: float = 1.25,
        latency_baseline_n: int = 20,
        latency_factor: float = 3.0,
        latency_floor_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.target_rate = target_rate
        self.rate = target_rate
        self.min_rate = min(min_rate, target_rate) if target_rate > 0 else 0
        self.recover_after = recover_after
        self.recover_factor = recover_factor
        self.latency_baseline_n = latency_baseline_n
        self.latency_factor = latency_factor
        self.latency_floor_s = latency_floor_s
        self._clock = clock
        self._sleep = sleep
        self._last: Optional[float] = None
        self._clean_streak = 0
        self._baseline_samples: list[float] = []
        self._baseline: Optional[float] = None
        self._ewma: Optional[float] = None
        self._since_latency_slowdown = 0
        self.slowdowns = 0
        self.requests = 0

    @property
    def unlimited(self) -> bool:
        return self.target_rate <= 0

    def wait(self) -> None:
        if not self.unlimited and self._last is not None:
            gap = 1.0 / self.rate - (self._clock() - self._last)
            if gap > 0:
                self._sleep(gap)
        self._last = self._clock()
        self.requests += 1

    def slow_down(self, reason: str) -> None:
        if self.unlimited:
            return
        old = self.rate
        self.rate = max(self.min_rate, self.rate / 2)
        self._clean_streak = 0
        self.slowdowns += 1
        logger.warning(
            "Slowing down: %s; rate %.3f -> %.3f req/s", reason, old, self.rate
        )

    def record_ok(self, latency_s: float) -> None:
        if self.unlimited:
            return
        self._track_latency(latency_s)
        self._clean_streak += 1
        if self.rate < self.target_rate and self._clean_streak >= self.recover_after:
            old = self.rate
            self.rate = min(self.target_rate, self.rate * self.recover_factor)
            self._clean_streak = 0
            logger.info(
                "Clean streak; rate %.3f -> %.3f req/s (target %.3f)",
                old,
                self.rate,
                self.target_rate,
            )

    def _track_latency(self, latency_s: float) -> None:
        if self._baseline is None:
            self._baseline_samples.append(latency_s)
            if len(self._baseline_samples) >= self.latency_baseline_n:
                self._baseline = statistics.median(self._baseline_samples)
                self._ewma = self._baseline
            return
        self._ewma = 0.9 * self._ewma + 0.1 * latency_s
        self._since_latency_slowdown += 1
        threshold = max(self.latency_floor_s, self.latency_factor * self._baseline)
        if self._ewma > threshold and self._since_latency_slowdown >= 20:
            self._since_latency_slowdown = 0
            self.slow_down(
                f"latency {self._ewma:.1f}s vs baseline {self._baseline:.1f}s"
            )


@dataclass
class PoliteFetcher:
    """
    Run fetches through the limiter, classify failures, and stop on blocks.

    fetch(max_attempts=1) must return (data, error, attempts, raw).
    """

    limiter: AdaptiveRateLimiter
    block_retry_wait_s: float = 30.0
    strain_retries: int = 3
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic

    def _once(self, fetch: Callable[..., FetchResult]) -> tuple[FetchResult, float]:
        self.limiter.wait()
        t0 = self.clock()
        result = fetch(max_attempts=1)
        return result, self.clock() - t0

    def fetch(self, label: str, fetch: Callable[..., FetchResult]) -> FetchResult:
        """
        Returns the fetch result. Page-structure errors (e.g. "no data found")
        come back as-is. Strain errors are retried (slower) up to
        strain_retries times, then returned. Raises FideBlocked if a block
        signal repeats after one wait.
        """
        block_retried = False
        strain_left = self.strain_retries
        while True:
            result, latency = self._once(fetch)
            _, error, _, _ = result
            if error is None or not (is_block_error(error) or is_strain_error(error)):
                self.limiter.record_ok(latency)
                return result
            if is_block_error(error):
                if block_retried:
                    raise FideBlocked(
                        f"FIDE blocking this IP ({label}: {error}); stopping so the "
                        "run can wait instead of grinding"
                    )
                block_retried = True
                logger.warning(
                    "Block signal on %s: %s; waiting %.0fs before one retry",
                    label,
                    error,
                    self.block_retry_wait_s,
                )
                self.sleep(self.block_retry_wait_s)
                continue
            # Strain
            self.limiter.slow_down(f"{label}: {error}")
            if strain_left <= 0:
                return result
            strain_left -= 1
