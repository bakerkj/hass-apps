# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-camera snapshot metrics, shared between Workers and the MQTT publisher.

Workers (one thread per stream) call ``record_success`` / ``record_error``
as they produce snapshots.  The MQTT publisher (a separate thread) calls
``snapshot()`` periodically to read a point-in-time view.  All access is
guarded by a single lock per stats instance.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class SnapshotView:
    """Point-in-time view of one stream's metrics.

    ``last_bytes`` is ``None`` until the first successful snapshot.  ``error``
    follows HA's ``binary_sensor`` ``problem`` convention — True means
    *problem*, False means *ok*.
    """

    snapshots_per_minute: float
    bytes_per_second: float
    last_bytes: int | None
    error: bool


class SnapshotStats:
    """Rolling per-camera snapshot counters.

    Successful snapshots are tracked as ``(timestamp, bytes)`` samples in a
    sliding window of ``rate_window_seconds``; rate sensors are computed by
    summing the window and dividing by its length.  The error flag flips on
    when either (a) the most recent attempt failed or (b) more than
    ``error_timeout_seconds`` have elapsed since the last success.
    """

    def __init__(
        self, rate_window_seconds: float, error_timeout_seconds: float
    ) -> None:
        self._window = float(rate_window_seconds)
        self._error_timeout = float(error_timeout_seconds)
        self._lock = threading.Lock()
        self._successes: list[tuple[float, int]] = []
        self._has_attempted: bool = False
        self._last_attempt_success: bool = True
        self._last_success_ts: float | None = None
        self._last_error_ts: float | None = None

    def record_success(self, bytes_: int, now: float | None = None) -> None:
        now = time.time() if now is None else float(now)
        with self._lock:
            self._successes.append((now, int(bytes_)))
            self._last_success_ts = now
            self._last_attempt_success = True
            self._has_attempted = True
            self._evict_old(now)

    def record_error(self, now: float | None = None) -> None:
        now = time.time() if now is None else float(now)
        with self._lock:
            self._last_error_ts = now
            self._last_attempt_success = False
            self._has_attempted = True

    def _evict_old(self, now: float) -> None:
        cutoff = now - self._window
        while self._successes and self._successes[0][0] < cutoff:
            self._successes.pop(0)

    def snapshot(self, now: float | None = None) -> SnapshotView:
        now = time.time() if now is None else float(now)
        with self._lock:
            self._evict_old(now)
            count = len(self._successes)
            total_bytes = sum(b for _, b in self._successes)
            last_bytes = self._successes[-1][1] if self._successes else None
            if not self._has_attempted:
                # No attempts yet — don't flag a problem before the Worker has
                # had a chance to try.
                error = False
            else:
                # Either the last attempt failed, or we haven't seen a success
                # within the error-timeout window.  A never-succeeded stream
                # reads as infinitely-stale, which naturally errors.
                if self._last_success_ts is None:
                    since_success = float("inf")
                else:
                    since_success = now - self._last_success_ts
                error = (not self._last_attempt_success) or (
                    since_success > self._error_timeout
                )
            return SnapshotView(
                snapshots_per_minute=count * 60.0 / self._window,
                bytes_per_second=total_bytes / self._window,
                last_bytes=last_bytes,
                error=error,
            )
