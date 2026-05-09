# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Rate limiter + throttle window constants — leaf module, no project deps."""

from __future__ import annotations

import threading
import time

# Throttle target is set per batch to ``len(eligible)`` files/min — that
# is, "process the work we have in roughly one minute".  When backlog is
# large the eligible query hits ``_ELIGIBLE_BATCH_SIZE`` (LIMIT) and the
# target far exceeds GPU capacity, so the rate limiter never sleeps
# (full-speed catchup) and processing naturally takes longer than a
# window; the loop skips the post-batch sleep in that case.
#
# When work IS available, the loop targets a fixed iteration cycle of
# one window — sleep at the end is whatever's left after processing.
# When NO work is available, sleep until the next recording becomes
# eligible (capped at ``MAX_SLEEP_SEC`` so pathological states still
# re-check periodically).
_THROTTLE_WINDOW_SEC = 60.0
MAX_SLEEP_SEC = 600.0

# Adaptive pacing knobs.  The encode loop tunes ``pace_scale`` per
# iteration to keep cycle wall-clock time at roughly _THROTTLE_WINDOW_SEC
# seconds.  scale=1.0 paces N jobs over the full window; scale<1.0 paces
# them over a tighter sub-window, leaving headroom for the last job's
# encode time + per-iteration overhead (eligibility query, futures
# collection, log emit, etc.).  Production observation: with no
# adaptation the cycle ran 1.4s long every iter; the controller
# converges this to ~zero drift within a handful of cycles.
_PACE_SCALE_MIN = 0.90
_PACE_SCALE_MAX = 1.00
_PACE_GAIN = 0.5
_PACE_PER_CYCLE_CLAMP = 0.05


def adapt_pace_scale(scale: float, elapsed: float) -> float:
    """Update ``pace_scale`` based on observed iteration wall-clock time.

    Proportional control on overshoot:

        delta = -(elapsed - window) / window * gain

    Negative ``delta`` shrinks scale (jobs get paced into a tighter
    sub-window, freeing up time for last-job-encode + overhead).
    Positive ``delta`` grows scale toward the ceiling when there's
    slack to absorb.  Clamped to ``±_PACE_PER_CYCLE_CLAMP`` so a
    transient anomaly (e.g. brief lock contention) doesn't overshoot
    the shrink — recovery takes a few iters but is stable.  Final
    value is clamped to ``[_PACE_SCALE_MIN, _PACE_SCALE_MAX]``.
    """
    overshoot = elapsed - _THROTTLE_WINDOW_SEC
    delta = -overshoot / _THROTTLE_WINDOW_SEC * _PACE_GAIN
    delta = max(-_PACE_PER_CYCLE_CLAMP, min(_PACE_PER_CYCLE_CLAMP, delta))
    return max(_PACE_SCALE_MIN, min(_PACE_SCALE_MAX, scale + delta))


class RateLimiter:
    """Thread-safe rate limiter shared across worker threads.

    ``target_per_min`` is set by the main loop just before submitting each
    batch (single-writer, multi-reader).  Workers call ``acquire`` and
    read whatever target is in effect at that moment.  No-op when target
    ≤ 0 (initial state, or no eligible work).
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.next_allowed = 0.0
        self.target_per_min = 0.0

    def set_target(self, target_per_min: float) -> None:
        """Update the active target (called by the main loop).

        Float writes are atomic in CPython so no lock is needed for this
        field; ``next_allowed`` is still protected by ``self.lock``.
        """
        self.target_per_min = max(0.0, target_per_min)

    def acquire(self, stopping: threading.Event) -> None:
        target = self.target_per_min
        if target <= 0:
            return
        interval_sec = 60.0 / target
        with self.lock:
            now = time.time()
            wait = self.next_allowed - now
            self.next_allowed = max(now, self.next_allowed) + interval_sec
        if wait > 0:
            stopping.wait(timeout=wait)
