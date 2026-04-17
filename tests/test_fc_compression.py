# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for get_eligible_recordings, time_until_next_eligible, compress_one, and housekeeping."""

from __future__ import annotations

import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import frigate_compressor as fc

from fc_helpers import (
    _insert_recording,
    _make_config,
    _make_frigate_db,
    _make_options,
    _open_compress_db,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Context builders
# ═══════════════════════════════════════════════════════════════════════════════


def _make_eligible_ctx(tmp_path, frigate_db, compress_conn=None, **cfg_overrides):
    """Build a minimal CompressorContext for get_eligible_recordings tests."""
    if compress_conn is None:
        compress_conn = _open_compress_db(tmp_path)
    cfg = fc.load_config(
        str(_make_options(tmp_path, frigate_db=str(frigate_db), **cfg_overrides))
    )
    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row
    return fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
        db_lock=threading.Lock(),
        frigate_ro=frigate_ro,
        frigate_ro_lock=threading.Lock(),
        frigate_rw=frigate_rw,
        frigate_lock=threading.Lock(),
    )


def _make_compress_one_ctx(tmp_path, src: Path, frigate_db: Path):
    """Build a CompressorContext for compress_one tests."""
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))
    compress_conn = _open_compress_db(tmp_path)
    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row
    return fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
        db_lock=threading.Lock(),
        frigate_ro=frigate_ro,
        frigate_ro_lock=threading.Lock(),
        frigate_rw=frigate_rw,
        frigate_lock=threading.Lock(),
    )


def _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn=None):
    """Build a CompressorContext suitable for run_housekeeping tests."""
    (tmp_path / "recordings").mkdir(exist_ok=True)
    if compress_conn is None:
        compress_conn = _open_compress_db(tmp_path)
    cfg = _make_config(
        tmp_path,
        frigate_db=str(frigate_db),
        recordings_dir=str(tmp_path / "recordings"),
    )
    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row
    return fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
        db_lock=threading.Lock(),
        frigate_ro=frigate_ro,
        frigate_ro_lock=threading.Lock(),
        frigate_rw=frigate_rw,
        frigate_lock=threading.Lock(),
    )


def _close_ctx(ctx):
    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()


# ═══════════════════════════════════════════════════════════════════════════════
# get_eligible_recordings
# ═══════════════════════════════════════════════════════════════════════════════


def test_get_eligible_recordings_returns_old_enough(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)
    _insert_recording(
        frigate_conn,
        "rec1",
        "cam1",
        "/media/cam1/a.mp4",
        time.time() - 8 * 86400,
        motion=5,
        objects=0,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
    results = fc.get_eligible_recordings(ctx)
    assert len(results) == 1
    assert results[0]["recording_id"] == "rec1"
    assert results[0]["tier"] == 1
    assert results[0]["recording_type"] == "motion"

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_skips_too_new(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "rec2", "cam1", "/media/cam1/b.mp4", time.time() - 3 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    assert fc.get_eligible_recordings(ctx) == []

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_skips_already_done(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "rec3", "cam1", "/media/cam1/c.mp4", time.time() - 10 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    fc._record(
        ctx.compress_db,
        ctx.db_lock,
        recording_id="rec3",
        camera="cam1",
        path="/media/cam1/c.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    assert fc.get_eligible_recordings(ctx) == []

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_retries_errored(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "rec4", "cam1", "/media/cam1/d.mp4", time.time() - 10 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    fc._record(
        ctx.compress_db,
        ctx.db_lock,
        recording_id="rec4",
        camera="cam1",
        path="/media/cam1/d.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=None,
        duration_sec=None,
        status=fc.STATUS_ERROR,
        error_msg="timeout",
    )
    results = fc.get_eligible_recordings(ctx)
    assert len(results) == 1
    assert results[0]["recording_id"] == "rec4"

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_tier2_assignment(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "rec5", "cam1", "/media/cam1/e.mp4", time.time() - 35 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    # Without tier 1 done, recording is eligible for tier 1 (not tier 2)
    results = fc.get_eligible_recordings(ctx)
    assert len(results) == 1
    assert results[0]["tier"] == 1

    # Mark tier 1 as done
    fc._record(
        ctx.compress_db,
        ctx.db_lock,
        recording_id="rec5",
        camera="cam1",
        path="/media/cam1/e.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )

    # Now it should be eligible for tier 2
    results = fc.get_eligible_recordings(ctx)
    assert len(results) == 1
    assert results[0]["tier"] == 2

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_object_type(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "rec6",
        "cam1",
        "/media/cam1/f.mp4",
        time.time() - 10 * 86400,
        motion=10,
        objects=3,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    results = fc.get_eligible_recordings(ctx)
    assert results[0]["recording_type"] == "object"

    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# time_until_next_eligible
# ═══════════════════════════════════════════════════════════════════════════════


def test_time_until_next_eligible_no_future_recordings(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db=frigate_db)

    assert fc.time_until_next_eligible(ctx) == fc.MAX_SLEEP_SEC

    _close_ctx(ctx)
    frigate_conn.close()


def test_time_until_next_eligible_with_pending_recording(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    # 5 days old; tier1 cutoff is 7 days → would naively wait ~2 days,
    # but the cap clamps it to MAX_SLEEP_SEC.
    _insert_recording(
        frigate_conn, "pending", "cam1", "/media/cam1/p.mp4", time.time() - 5 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db=frigate_db)
    wait = fc.time_until_next_eligible(ctx)
    assert fc.MIN_SLEEP_SEC <= wait <= fc.MAX_SLEEP_SEC

    _close_ctx(ctx)
    frigate_conn.close()


def test_time_until_next_eligible_minimum_60s(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    # 30 seconds until eligible
    _insert_recording(
        frigate_conn, "soon", "cam1", "/media/cam1/s.mp4", time.time() - 7 * 86400 + 30
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db=frigate_db)
    assert fc.time_until_next_eligible(ctx) >= fc.MIN_SLEEP_SEC

    _close_ctx(ctx)
    frigate_conn.close()


def test_time_until_next_eligible_caps_at_max_sleep(tmp_path):
    """A recording that just started recording (tier1 deadline ~min_days
    away) must not produce a multi-day sleep — it should clamp to
    MAX_SLEEP_SEC so the loop wakes up and re-checks state."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    # Just-started recording: tier1 eligibility is min_days × 86400 seconds
    # away, far longer than MAX_SLEEP_SEC (600s).
    _insert_recording(frigate_conn, "fresh", "cam1", "/media/cam1/f.mp4", time.time())

    ctx = _make_eligible_ctx(tmp_path, frigate_db=frigate_db)
    assert fc.time_until_next_eligible(ctx) == fc.MAX_SLEEP_SEC

    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# compress_one
# ═══════════════════════════════════════════════════════════════════════════════


def _setup_compress_one(tmp_path):
    """Create a source file and Frigate DB entry; return (ctx, src_path)."""
    src = tmp_path / "recordings" / "cam1" / "clip.mp4"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"x" * 10000)

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "r1",
        "cam1",
        str(src),
        time.time() - 10 * 86400,
        motion=5,
        objects=0,
    )
    frigate_conn.close()

    ctx = _make_compress_one_ctx(tmp_path, src, frigate_db)
    return ctx, src


def _compress_one(ctx, src, *, path=None, recording_id="r1", recording_type="motion"):
    return fc.compress_one(
        recording_id=recording_id,
        path=path if path is not None else str(src),
        camera="cam1",
        tier=1,
        recording_type=recording_type,
        encoder="cpu",
        ctx=ctx,
    )


def _db_row(ctx, recording_id="r1"):
    return ctx.compress_db.execute(
        "SELECT * FROM files WHERE recording_id=?", (recording_id,)
    ).fetchone()


def test_compress_one_missing_file(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)
    result = _compress_one(ctx, src, path=str(tmp_path / "nonexistent.mp4"))
    assert result is False
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_ERROR
    assert "missing" in row["t1_error_msg"]
    _close_ctx(ctx)


def test_compress_one_ffmpeg_success(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is True
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_OK
    assert row["file_size"] == 10000
    assert row["t1_file_size"] == 5000
    _close_ctx(ctx)


def test_compress_one_ffmpeg_failure(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        m = MagicMock()
        m.returncode = 1
        m.stderr = "ffmpeg error"
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is False
    assert _db_row(ctx)["t1_status"] == fc.STATUS_ERROR
    _close_ctx(ctx)


def test_compress_one_output_too_small(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"z" * 5)  # less than 10% of 10000
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is False
    assert "small" in _db_row(ctx)["t1_error_msg"]
    _close_ctx(ctx)


def test_compress_one_original_deleted_during_encode(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        src.unlink()  # simulate Frigate deleting original while encoding
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is False
    assert "deleted" in _db_row(ctx)["t1_error_msg"]
    _close_ctx(ctx)


def test_compress_one_recording_removed_from_frigate_db(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)
    frigate_rw2 = sqlite3.connect(str(tmp_path / "frigate.db"))

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        frigate_rw2.execute("DELETE FROM recordings WHERE id='r1'")
        frigate_rw2.commit()
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is False
    assert src.exists()  # original not replaced
    assert "Frigate DB" in _db_row(ctx)["t1_error_msg"]
    frigate_rw2.close()
    _close_ctx(ctx)


def test_compress_one_ffmpeg_exception(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    with patch("subprocess.run", side_effect=OSError("ffmpeg not found")):
        result = _compress_one(ctx, src)

    assert result is False
    assert list(src.parent.glob(fc._TEMP_GLOB)) == []
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_ERROR
    assert "ffmpeg not found" in row["t1_error_msg"]
    _close_ctx(ctx)


def test_compress_one_timeout_records_duration(tmp_path):
    ctx, src = _setup_compress_one(tmp_path)

    with patch(
        "subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=[], timeout=300)
    ):
        result = _compress_one(ctx, src)

    assert result is False
    assert _db_row(ctx)["t1_encode_sec"] is not None
    _close_ctx(ctx)


def test_compress_one_segment_size_update_fails(tmp_path):
    # If the Frigate DB segment_size update fails, compress_one should still
    # return True but record status='segment_update_failed' for housekeeping to retry.
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    # sqlite3.Connection.execute is a C-level slot; wrap in a MagicMock to intercept only the segment_size UPDATE.
    # _close_ctx() will close ctx.frigate_rw, which is now the mock — so we
    # have to close the real connection ourselves to avoid leaking it.
    real_rw = ctx.frigate_rw
    mock_rw = MagicMock(spec=sqlite3.Connection)
    mock_rw.execute.side_effect = lambda sql, *a, **kw: (
        (_ for _ in ()).throw(sqlite3.OperationalError("database is locked"))
        if "segment_size" in sql
        else real_rw.execute(sql, *a, **kw)
    )
    mock_rw.commit.side_effect = real_rw.commit
    ctx.frigate_rw = mock_rw

    try:
        with patch("subprocess.run", side_effect=fake_run):
            result = _compress_one(ctx, src)

        assert result is True
        row = _db_row(ctx)
        assert row["t1_status"] == fc.STATUS_SEGMENT_UPDATE_FAILED
        assert "locked" in row["t1_error_msg"]
    finally:
        real_rw.close()
        _close_ctx(ctx)


def test_compress_one_segment_update_failed_not_recompressed(tmp_path):
    # A recording with status='segment_update_failed' must not be returned by
    # get_eligible_recordings — the file is already compressed.
    ctx, src = _setup_compress_one(tmp_path)
    ctx.compress_db.execute(
        "INSERT INTO files"
        " (recording_id, camera, path, recording_type, file_size,"
        "  t1_encoder, t1_file_size, t1_encode_sec, t1_compressed_at, t1_status)"
        " VALUES ('r1','cam1',?,'motion',10000,"
        "  'cpu',5000,1.0,datetime('now'),?)",
        (str(src), fc.STATUS_SEGMENT_UPDATE_FAILED),
    )
    ctx.compress_db.commit()

    eligible = fc.get_eligible_recordings(ctx)
    assert not any(r["recording_id"] == "r1" for r in eligible)
    _close_ctx(ctx)


def test_compress_one_uses_unique_temp_name(tmp_path):
    # Temp file must embed the recording_id, not use a fixed suffix.
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run) as mock_run:
        _compress_one(ctx, src)

    temp_path = Path(mock_run.call_args[0][0][-1])
    assert "r1" in temp_path.name
    assert temp_path.name.startswith(fc._TEMP_PREFIX)
    _close_ctx(ctx)


# ═══════════════════════════════════════════════════════════════════════════════
# housekeeping
# ═══════════════════════════════════════════════════════════════════════════════


def test_housekeeping_prunes_orphaned_entries(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)
    lock = threading.Lock()

    # Row in compress DB with no matching Frigate recording — should be pruned.
    fc._record(
        compress_conn,
        lock,
        recording_id="orphan1",
        camera="cam1",
        path="/media/cam1/gone.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    # Row with a matching Frigate recording — should be kept.
    _insert_recording(
        frigate_conn, "alive1", "cam1", "/media/cam1/alive.mp4", time.time() - 86400
    )
    fc._record(
        compress_conn,
        lock,
        recording_id="alive1",
        camera="cam1",
        path="/media/cam1/alive.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=2000,
        size_after=1000,
        duration_sec=2.0,
        status=fc.STATUS_OK,
    )

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)

    remaining = {
        r[0] for r in compress_conn.execute("SELECT recording_id FROM files").fetchall()
    }
    assert "orphan1" not in remaining
    assert "alive1" in remaining

    _close_ctx(ctx)
    frigate_conn.close()


def test_housekeeping_retries_segment_update_failed(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)

    rec_path = tmp_path / "recordings" / "cam1" / "clip.mp4"
    rec_path.parent.mkdir(parents=True)
    rec_path.write_bytes(b"x" * 4096)
    _insert_recording(frigate_conn, "seg1", "cam1", str(rec_path), time.time() - 86400)
    frigate_conn.commit()

    compress_conn = _open_compress_db(tmp_path)
    fc._record(
        compress_conn,
        threading.Lock(),
        recording_id="seg1",
        camera="cam1",
        path=str(rec_path),
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=8192,
        size_after=4096,
        duration_sec=1.0,
        status=fc.STATUS_SEGMENT_UPDATE_FAILED,
        error_msg="database is locked",
    )

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)

    row = compress_conn.execute(
        "SELECT * FROM files WHERE recording_id='seg1'"
    ).fetchone()
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t1_error_msg"] is None

    seg_row = ctx.frigate_rw.execute(
        "SELECT segment_size FROM recordings WHERE id='seg1'"
    ).fetchone()
    assert seg_row["segment_size"] == pytest.approx(4096 / (1024 * 1024), rel=1e-4)

    _close_ctx(ctx)
    frigate_conn.close()


def test_housekeeping_segment_retry_file_missing(tmp_path):
    # If a segment_update_failed file no longer exists on disk, housekeeping skips it.
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "seg2", "cam1", "/nonexistent/clip.mp4", time.time() - 86400
    )
    frigate_conn.commit()

    compress_conn = _open_compress_db(tmp_path)
    fc._record(
        compress_conn,
        threading.Lock(),
        recording_id="seg2",
        camera="cam1",
        path="/nonexistent/clip.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=8192,
        size_after=4096,
        duration_sec=1.0,
        status=fc.STATUS_SEGMENT_UPDATE_FAILED,
        error_msg="locked",
    )

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)

    row = compress_conn.execute(
        "SELECT * FROM files WHERE recording_id='seg2'"
    ).fetchone()
    assert row["t1_status"] == fc.STATUS_SEGMENT_UPDATE_FAILED  # unchanged — file gone

    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# run_main_loop scheduling
# ═══════════════════════════════════════════════════════════════════════════════
#
# These tests pin down the rule that the daemon must NOT sleep between passes
# when the previous pass had work — only when the queue is fully drained.
# Regression here would let the daemon fall arbitrarily far behind a steady
# recording rate just because one pass took longer than the inter-recording
# gap.


def _eligible_row(rid: str = "r1", camera: str = "cam1") -> dict:
    return {
        "recording_id": rid,
        "camera": camera,
        "path": f"/tmp/{rid}.mp4",
        "tier": 1,
        "recording_type": "object",
    }


def test_run_main_loop_does_not_sleep_when_work_was_done(monkeypatch, tmp_path):
    """If a pass processed >0 recordings, the loop must re-query immediately
    instead of calling time_until_next_eligible / sleeping."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    # Three passes: work, work, empty.  After the empty pass we should sleep
    # once via time_until_next_eligible, and the test ends.
    eligible_calls = {"n": 0}
    next_calls = {"n": 0}
    wait_timeouts: list[float | None] = []

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        if eligible_calls["n"] == 1:
            return [_eligible_row("r1"), _eligible_row("r2")]
        if eligible_calls["n"] == 2:
            return [_eligible_row("r3")]
        return []

    def fake_next_eligible(_ctx):
        next_calls["n"] += 1
        return 999.0

    def fake_compress(*_args, **_kwargs):
        return True

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        # End the loop the first time it tries to sleep so the test terminates.
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "time_until_next_eligible", fake_next_eligible)
    monkeypatch.setattr(fc, "compress_one", fake_compress)
    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # 3 eligible queries: work, work, empty.
    assert eligible_calls["n"] == 3
    # Sleep path entered exactly once — only after the empty pass.
    assert next_calls["n"] == 1
    assert wait_timeouts == [999.0]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_sleeps_immediately_when_no_work(monkeypatch, tmp_path):
    """If the very first pass returns nothing, the loop should go straight
    to time_until_next_eligible without ever entering the work branch."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    eligible_calls = {"n": 0}
    next_calls = {"n": 0}
    compress_calls = {"n": 0}
    wait_timeouts: list[float | None] = []

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        return []

    def fake_next_eligible(_ctx):
        next_calls["n"] += 1
        return 60.0

    def fake_compress(*_args, **_kwargs):
        compress_calls["n"] += 1
        return True

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "time_until_next_eligible", fake_next_eligible)
    monkeypatch.setattr(fc, "compress_one", fake_compress)
    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    assert eligible_calls["n"] == 1
    assert next_calls["n"] == 1
    assert compress_calls["n"] == 0  # never entered the work branch
    assert wait_timeouts == [60.0]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_sleep_path_handles_query_exception(monkeypatch, tmp_path):
    """time_until_next_eligible blowing up must not crash the loop —
    it should fall back to a 1h sleep."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    wait_timeouts: list[float | None] = []

    monkeypatch.setattr(fc, "get_eligible_recordings", lambda _ctx: [])

    def boom(_ctx):
        raise RuntimeError("frigate db went away")

    monkeypatch.setattr(fc, "time_until_next_eligible", boom)

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # Fell back to the MAX_SLEEP_SEC default instead of crashing.
    assert wait_timeouts == [fc.MAX_SLEEP_SEC]

    _close_ctx(ctx)
    frigate_conn.close()
