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
