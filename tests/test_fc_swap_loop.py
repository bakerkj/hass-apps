# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for ``swap_eligibility.get_eligible_swaps`` and ``swap_loop.run_swap_loop``."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import frigate_compressor as fc

from fc_helpers import (
    _insert_recording,
    _make_frigate_db,
    _make_options,
    _open_compress_db,
)


def _make_swap_ctx(tmp_path, frigate_db, compress_conn=None, **cfg_overrides):
    """CompressorContext suitable for swap-loop eligibility tests."""
    if compress_conn is None:
        compress_conn = _open_compress_db(tmp_path)
    cfg = fc.load_config(
        str(_make_options(tmp_path, frigate_db=str(frigate_db), **cfg_overrides))
    )
    fc._attach_frigate(compress_conn, cfg, "frigate")
    return fc.CompressorContext(cfg=cfg, compress_db=compress_conn)


def _insert_swap_row(
    frigate_conn, compress_conn, rid, camera, path, start_time, t2_status
):
    """Insert a recording + files row in a given t2_status."""
    _insert_recording(frigate_conn, rid, camera, path, start_time)
    compress_conn.execute(
        "INSERT OR REPLACE INTO files"
        " (recording_id, camera, path, recording_type, file_size,"
        "  start_time, scanned_at, t1_status, t2_status)"
        " VALUES (?, ?, ?, 'continuous', 1000, ?, '2026-01-01T00:00:00',"
        "         ?, ?)",
        (rid, camera, path, start_time, fc.STATUS_OK, t2_status),
    )
    compress_conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# get_eligible_swaps
# ═══════════════════════════════════════════════════════════════════════════════


def test_get_eligible_swaps_returns_only_direct_rows_past_min_days(tmp_path):
    """Only rows with ``t2_status='direct'`` past ``tier2.min_days`` surface
    here.  Chained-tier-2 candidates and direct rows still inside the
    tier-2 grace window are excluded."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    now = time.time()
    # Eligible: direct + past min_days.
    _insert_swap_row(
        frigate_conn,
        compress_conn,
        "ready",
        "cam1",
        "/m/ready.mp4",
        now - 100 * 86400,
        fc.STATUS_DIRECT,
    )
    # Excluded: direct but still inside grace (t2 min_days defaults to 30).
    _insert_swap_row(
        frigate_conn,
        compress_conn,
        "young",
        "cam1",
        "/m/young.mp4",
        now - 5 * 86400,
        fc.STATUS_DIRECT,
    )
    # Excluded: chained candidate (NULL t2_status) — handled by the encode
    # loop, not here.
    _insert_swap_row(
        frigate_conn,
        compress_conn,
        "chained",
        "cam1",
        "/m/chained.mp4",
        now - 100 * 86400,
        None,
    )

    ctx = _make_swap_ctx(tmp_path, frigate_db, compress_conn)
    rows = fc.get_eligible_swaps(ctx)
    assert [r["recording_id"] for r in rows] == ["ready"]
    assert rows[0]["camera"] == "cam1"
    assert rows[0]["path"] == "/m/ready.mp4"
    assert rows[0]["recording_type"] == "continuous"

    compress_conn.close()
    frigate_conn.close()


def test_get_eligible_swaps_orders_oldest_first(tmp_path):
    """Sibling that's been parked longest gets freed first."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    now = time.time()
    for i, age_days in enumerate([200, 100, 150]):
        _insert_swap_row(
            frigate_conn,
            compress_conn,
            f"r{i}",
            "cam1",
            f"/m/r{i}.mp4",
            now - age_days * 86400,
            fc.STATUS_DIRECT,
        )

    ctx = _make_swap_ctx(tmp_path, frigate_db, compress_conn)
    rows = fc.get_eligible_swaps(ctx)
    # Inserted ages 200, 100, 150 → oldest-first order: r0 (200), r2 (150), r1 (100)
    assert [r["recording_id"] for r in rows] == ["r0", "r2", "r1"]

    compress_conn.close()
    frigate_conn.close()


def test_get_eligible_swaps_caps_at_max_per_window(tmp_path, monkeypatch):
    """The per-call SELECT honours ``_MAX_SWAPS_PER_WINDOW``."""
    monkeypatch.setattr(fc.swap_eligibility, "_MAX_SWAPS_PER_WINDOW", 3)

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    now = time.time()
    for i in range(5):
        _insert_swap_row(
            frigate_conn,
            compress_conn,
            f"r{i}",
            "cam1",
            f"/m/r{i}.mp4",
            now - (200 - i) * 86400,
            fc.STATUS_DIRECT,
        )

    ctx = _make_swap_ctx(tmp_path, frigate_db, compress_conn)
    rows = fc.get_eligible_swaps(ctx)
    assert len(rows) == 3

    compress_conn.close()
    frigate_conn.close()


def test_get_eligible_swaps_skips_disabled_cameras(tmp_path):
    """Cameras with ``enabled=False`` or ``tier2.enabled=False`` are skipped."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    _insert_swap_row(
        frigate_conn,
        compress_conn,
        "off",
        "cam1",
        "/m/off.mp4",
        time.time() - 100 * 86400,
        fc.STATUS_DIRECT,
    )

    ctx = _make_swap_ctx(tmp_path, frigate_db, compress_conn)
    # Disable tier2 entirely on this camera.
    ctx.cfg.cameras["cam1"].tier2.enabled = False

    assert fc.get_eligible_swaps(ctx) == []

    compress_conn.close()
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# run_swap_loop
# ═══════════════════════════════════════════════════════════════════════════════


def test_run_swap_loop_calls_swap_t2_for_each_eligible(monkeypatch):
    """One iteration: swap_t2 is invoked once per eligible row, in order.
    The loop exits after the batch drains by stopping on the 2nd
    eligibility poll."""
    swapped: list[tuple[str, str]] = []

    def fake_swap_t2(rid, path, camera, rtype, encoder, ctx):
        swapped.append((rid, path))
        return True

    monkeypatch.setattr(fc.swap_loop, "swap_t2", fake_swap_t2)

    eligible = [
        {
            "recording_id": "a",
            "camera": "c",
            "path": "/p/a",
            "recording_type": "continuous",
        },
        {
            "recording_id": "b",
            "camera": "c",
            "path": "/p/b",
            "recording_type": "continuous",
        },
    ]
    stopping = threading.Event()
    poll_calls = {"n": 0}

    def fake_eligible(_ctx):
        poll_calls["n"] += 1
        # 1st poll → return work; 2nd poll → empty (exit via the no-work
        # sleep path, which we short-circuit below).
        return eligible if poll_calls["n"] == 1 else []

    monkeypatch.setattr(fc.swap_loop, "get_eligible_swaps", fake_eligible)
    # The rate limiter doesn't matter for this test — we just need it not
    # to actually wait.  No-op the acquire.
    monkeypatch.setattr(fc.swap_loop.RateLimiter, "acquire", lambda self, ev: None)

    # On the no-work poll, ``run_swap_loop`` calls ``stopping.wait`` to
    # sleep the window; flip the event there so the next ``while`` check
    # exits.
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        stopping.set()
        return real_wait(timeout=0)

    stopping.wait = fake_wait  # type: ignore[method-assign]

    fake_ctx = MagicMock()
    fake_ctx.cfg.all_dry_run = False
    fc.run_swap_loop(fake_ctx, stopping, encoder="cpu")

    assert swapped == [("a", "/p/a"), ("b", "/p/b")]
    assert poll_calls["n"] == 2


def test_run_swap_loop_sleeps_when_no_eligible(monkeypatch):
    """No eligible rows → loop calls ``stopping.wait`` and exits when set."""
    monkeypatch.setattr(fc.swap_loop, "get_eligible_swaps", lambda _ctx: [])

    waits: list[float] = []
    stopping = threading.Event()

    def fake_wait(timeout=None):
        waits.append(timeout)
        stopping.set()
        return True

    stopping.wait = fake_wait  # type: ignore[method-assign]

    fake_ctx = MagicMock()
    fake_ctx.cfg.all_dry_run = False
    fc.run_swap_loop(fake_ctx, stopping, encoder="cpu")

    assert waits == [fc.swap_loop._SWAP_WINDOW_SEC]


def test_run_swap_loop_logs_and_continues_on_query_failure(monkeypatch):
    """If ``get_eligible_swaps`` raises, the loop logs and sleeps a window
    instead of dying."""
    calls = {"n": 0}

    def boom(_ctx):
        calls["n"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(fc.swap_loop, "get_eligible_swaps", boom)

    stopping = threading.Event()

    def fake_wait(timeout=None):
        # First call: stop the loop so we don't busy-loop on the exception.
        stopping.set()
        return True

    stopping.wait = fake_wait  # type: ignore[method-assign]

    fake_ctx = MagicMock()
    fake_ctx.cfg.all_dry_run = False
    fc.run_swap_loop(fake_ctx, stopping, encoder="cpu")

    assert calls["n"] == 1
