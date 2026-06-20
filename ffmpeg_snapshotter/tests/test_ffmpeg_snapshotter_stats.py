# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for SnapshotStats — the per-camera rolling-window metrics store."""

from __future__ import annotations

import ffmpeg_snapshotter as fs


def _stats(rate_window: float = 60.0, error_timeout: float = 30.0) -> fs.SnapshotStats:
    return fs.SnapshotStats(
        rate_window_seconds=rate_window, error_timeout_seconds=error_timeout
    )


# ---------------------------------------------------------------------------
# Fresh state
# ---------------------------------------------------------------------------


def test_fresh_stats_reports_zeros_and_no_error():
    v = _stats().snapshot(now=1000.0)
    assert v.snapshots_per_minute == 0.0
    assert v.bytes_per_second == 0.0
    assert v.last_bytes is None
    # No attempts yet → not in error state (don't alarm before we've tried).
    assert v.error is False


# ---------------------------------------------------------------------------
# Rate math
# ---------------------------------------------------------------------------


def test_rate_counts_only_samples_in_window():
    s = _stats(rate_window=60.0)
    # One success inside the window, one outside.
    s.record_success(1000, now=1000.0)
    s.record_success(2000, now=1050.0)
    v = s.snapshot(now=1060.0)
    # 2 successes in a 60s window → 2 * 60/60 = 2.0/min
    assert v.snapshots_per_minute == 2.0
    # 3000 bytes over 60s → 50 B/s
    assert v.bytes_per_second == 50.0
    # last_bytes = most recent success's bytes
    assert v.last_bytes == 2000


def test_rate_evicts_samples_past_window():
    s = _stats(rate_window=60.0)
    s.record_success(1000, now=1000.0)  # will fall out of window
    s.record_success(2000, now=1050.0)  # still inside at now=1065
    v = s.snapshot(now=1065.0)  # cutoff = 1005 → first success evicted
    assert v.snapshots_per_minute == 1.0
    assert v.bytes_per_second == 2000 / 60.0
    assert v.last_bytes == 2000


def test_last_bytes_is_none_after_all_samples_expire():
    s = _stats(rate_window=60.0)
    s.record_success(1000, now=1000.0)
    v = s.snapshot(now=1100.0)  # way past window
    assert v.snapshots_per_minute == 0.0
    assert v.bytes_per_second == 0.0
    assert v.last_bytes is None


# ---------------------------------------------------------------------------
# Error flag semantics
# ---------------------------------------------------------------------------


def test_error_flag_on_after_single_failure():
    s = _stats(error_timeout=30.0)
    s.record_error(now=1000.0)
    assert s.snapshot(now=1001.0).error is True


def test_error_flag_clears_after_success():
    s = _stats(error_timeout=30.0)
    s.record_error(now=1000.0)
    s.record_success(500, now=1005.0)
    assert s.snapshot(now=1006.0).error is False


def test_error_flag_on_when_last_success_is_stale():
    s = _stats(error_timeout=30.0)
    s.record_success(500, now=1000.0)
    # 40s later: last attempt was a success, but too long ago.
    v = s.snapshot(now=1040.0)
    assert v.error is True


def test_error_flag_on_when_only_errors_have_happened():
    s = _stats(error_timeout=30.0)
    s.record_error(now=1000.0)
    s.record_error(now=1005.0)
    assert s.snapshot(now=1006.0).error is True


def test_error_flag_off_when_success_is_within_timeout():
    s = _stats(error_timeout=30.0)
    s.record_success(500, now=1000.0)
    assert s.snapshot(now=1020.0).error is False


# ---------------------------------------------------------------------------
# Thread safety (smoke check — not a correctness proof)
# ---------------------------------------------------------------------------


def test_concurrent_record_and_snapshot_do_not_crash():
    import threading

    s = _stats(rate_window=60.0, error_timeout=30.0)
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            s.record_success(i, now=1000.0 + i * 0.001)
            i += 1

    def reader():
        while not stop.is_set():
            s.snapshot()

    writers = [threading.Thread(target=writer, daemon=True) for _ in range(2)]
    readers = [threading.Thread(target=reader, daemon=True) for _ in range(2)]
    for t in writers + readers:
        t.start()
    # Run briefly; no assertion other than "no crash".
    import time as _t

    _t.sleep(0.2)
    stop.set()
    for t in writers + readers:
        t.join(timeout=1.0)
