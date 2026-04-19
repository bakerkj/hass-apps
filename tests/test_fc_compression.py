# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for get_eligible_recordings, compress_one, and housekeeping."""

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
        frigate_ro=frigate_ro,
        frigate_rw=frigate_rw,
        compress_db=compress_conn,
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
        frigate_ro=frigate_ro,
        frigate_rw=frigate_rw,
        compress_db=compress_conn,
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
        frigate_ro=frigate_ro,
        frigate_rw=frigate_rw,
        compress_db=compress_conn,
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


def test_get_eligible_recordings_orders_tier1_before_tier2(tmp_path):
    """Tier 1 work always precedes tier 2 work in the batch, even when the
    tier-2 candidate is older — drains the bigger storage win first."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)

    # Older recording: t1 already done → eligible for tier 2.
    _insert_recording(
        frigate_conn, "old-t2", "cam1", "/m/old.mp4", time.time() - 60 * 86400
    )
    # Newer recording: t1 not done → eligible for tier 1.
    _insert_recording(
        frigate_conn, "new-t1", "cam1", "/m/new.mp4", time.time() - 10 * 86400
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    fc._record(
        ctx.compress_db,
        recording_id="old-t2",
        camera="cam1",
        path="/m/old.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )

    results = fc.get_eligible_recordings(ctx)
    # Tier 1 first despite being newer; tier 2 second despite being older.
    assert [r["recording_id"] for r in results] == ["new-t1", "old-t2"]
    assert [r["tier"] for r in results] == [1, 2]

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

    # Insert probe data so compress_one doesn't skip the file.
    fc._store_probe(
        ctx.compress_db,
        "r1",
        "cam1",
        str(src),
        {
            "codec": "h264",
            "width": 1920,
            "height": 1080,
            "fps": 20.0,
            "bitrate": 5000000,
            "duration_sec": 10.0,
            "file_size": 10000,
        },
    )

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
        if cmd[0] == "ffmpeg":
            # Write a tiny file — less than 3% of 10000 bytes.
            Path(cmd[-1]).write_bytes(b"z" * 5)
        elif cmd[0] == "ffprobe":
            # Return failure for ffprobe on the tiny file.
            return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="")
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is False
    row = _db_row(ctx)
    assert row["t1_error_msg"] is not None
    assert "small" in row["t1_error_msg"] or "ffprobe" in row["t1_error_msg"]
    _close_ctx(ctx)


def test_compress_one_output_small_but_valid(tmp_path):
    """Output <3% of original but ffprobe confirms valid video with matching duration."""
    ctx, src = _setup_compress_one(tmp_path)

    def fake_run(cmd, **kwargs):
        if cmd[0] == "ffmpeg":
            # Write a small file — less than 3% of 10000 bytes.
            Path(cmd[-1]).write_bytes(b"y" * 200)
        elif cmd[0] == "ffprobe":
            # Return valid probe results with matching duration.
            m = MagicMock()
            m.returncode = 0
            m.stderr = ""
            m.stdout = (
                "codec_name=h264\n"
                "width=1920\n"
                "height=1080\n"
                "r_frame_rate=20/1\n"
                "bit_rate=100000\n"
                "duration=10.0\n"
                "size=200\n"
            )
            return m
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_one(ctx, src)

    assert result is True
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t1_file_size"] == 200
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

    # Make the Frigate DB read-only so the segment_size UPDATE fails.
    frigate_db = Path(str(ctx.cfg.frigate_db))
    frigate_db.chmod(0o444)

    try:
        with patch("subprocess.run", side_effect=fake_run):
            result = _compress_one(ctx, src)

        assert result is True
        row = _db_row(ctx)
        assert row["t1_status"] == fc.STATUS_SEGMENT_UPDATE_FAILED
    finally:
        frigate_db.chmod(0o644)
        _close_ctx(ctx)


def test_compress_one_segment_update_failed_not_recompressed(tmp_path):
    # A recording with status='segment_update_failed' must not be returned by
    # get_eligible_recordings — the file is already compressed.
    ctx, src = _setup_compress_one(tmp_path)
    ctx.compress_db.execute(
        "UPDATE files SET"
        " recording_type='motion', file_size=10000,"
        " t1_encoder='cpu', t1_file_size=5000, t1_encode_sec=1.0,"
        " t1_compressed_at=datetime('now'), t1_status=?"
        " WHERE recording_id='r1'",
        (fc.STATUS_SEGMENT_UPDATE_FAILED,),
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

    # Row in compress DB with no matching Frigate recording — should be pruned.
    fc._record(
        compress_conn,
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


def test_run_main_loop_sleeps_one_window_after_partial_batch(monkeypatch, tmp_path):
    """A partial batch (fewer than _ELIGIBLE_BATCH_SIZE files) means we've
    drained the queue — the loop should sleep one full window before the
    next pass instead of immediately re-querying."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    eligible_calls = {"n": 0}
    wait_timeouts: list[float | None] = []

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        return [_eligible_row("r1"), _eligible_row("r2")]

    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "compress_one", lambda *a, **k: True)

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        # End the loop the first time it tries to sleep so the test terminates.
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    assert eligible_calls["n"] == 1
    assert wait_timeouts == [fc._THROTTLE_WINDOW_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_continues_immediately_on_full_batch(monkeypatch, tmp_path):
    """A full batch (== _ELIGIBLE_BATCH_SIZE) signals more work is waiting —
    the loop must skip the post-batch sleep and re-query immediately so
    catchup runs flat-out."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    eligible_calls = {"n": 0}
    wait_timeouts: list[float | None] = []

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        # Pass 1: full batch.  Pass 2: empty (forces sleep, ends test).
        if eligible_calls["n"] == 1:
            return [_eligible_row(f"r{i}") for i in range(fc._ELIGIBLE_BATCH_SIZE)]
        return []

    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "compress_one", lambda *a, **k: True)

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # Two eligibility queries: the full batch (continued immediately) then the
    # empty pass that finally hits the sleep.
    assert eligible_calls["n"] == 2
    assert wait_timeouts == [fc._THROTTLE_WINDOW_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_sleeps_one_window_when_no_work(monkeypatch, tmp_path):
    """An empty queue means the loop sleeps one window before re-checking."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    eligible_calls = {"n": 0}
    compress_calls = {"n": 0}
    wait_timeouts: list[float | None] = []

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        return []

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
    monkeypatch.setattr(fc, "compress_one", fake_compress)
    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    assert eligible_calls["n"] == 1
    assert compress_calls["n"] == 0
    assert wait_timeouts == [fc._THROTTLE_WINDOW_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# throttle (count-based: workload + EWMA)
# ═══════════════════════════════════════════════════════════════════════════════


def test_throttle_target_zero_when_no_workload(monkeypatch, tmp_path):
    """workload=0 → _effective_throttle_target returns 0 (limiter disabled)."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc, "measure_workload_per_min", lambda _ctx: 0.0)
    assert fc._effective_throttle_target(ctx) == 0.0

    _close_ctx(ctx)
    frigate_conn.close()


def test_throttle_target_equals_workload(monkeypatch, tmp_path):
    """target = workload (1:1; no overhead multiplier)."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc, "measure_workload_per_min", lambda _ctx: 40.0)
    assert fc._effective_throttle_target(ctx) == pytest.approx(40.0)

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_refreshes_throttle_target_on_first_iteration(
    monkeypatch, tmp_path
):
    """The main loop forces a throttle refresh on the first iteration so the
    limiter leaves its initial 0 (= disabled) state."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc, "measure_workload_per_min", lambda _ctx: 50.0)
    monkeypatch.setattr(fc, "get_eligible_recordings", lambda _ctx: [])

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # target = workload = 50
    assert ctx.rate_limiter.target_per_min == pytest.approx(50.0)

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_throttle_refresh_is_gated_by_cadence(monkeypatch, tmp_path):
    """Throttle target is only recomputed every _THROTTLE_WINDOW_SEC.  Catchup
    iterations (full batch → continue immediately) happen back-to-back without
    sleeping; the throttle should not re-fire on each one."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    target_calls = {"n": 0}

    def fake_target(_ctx):
        target_calls["n"] += 1
        return 50.0

    monkeypatch.setattr(fc, "_effective_throttle_target", fake_target)

    eligible_calls = {"n": 0}

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        # Pass 1: full batch (forces continue).  Pass 2: empty (forces sleep).
        if eligible_calls["n"] == 1:
            return [_eligible_row(f"r{i}") for i in range(fc._ELIGIBLE_BATCH_SIZE)]
        return []

    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "compress_one", lambda *a, **k: True)
    # Disable per-file throttling so the catchup batch finishes instantly,
    # keeping mocked time inside the 60s gate.
    ctx.rate_limiter.set_target(0)
    monkeypatch.setattr(fc.RateLimiter, "set_target", lambda self, target_per_min: None)

    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        if timeout is not None:
            t["now"] += timeout
        if timeout is not None and timeout >= fc._THROTTLE_WINDOW_SEC:
            stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # Throttle target was computed exactly once across both iterations —
    # pass 2 is in the same wall-clock tick as pass 1 (catchup continues
    # immediately, no time advanced), well inside the 60s gate.
    assert target_calls["n"] == 1
    # And both eligibility queries did fire (catchup re-queried).
    assert eligible_calls["n"] == 2

    _close_ctx(ctx)
    frigate_conn.close()


def test_pace_then_compress_acquires_before_compressing(monkeypatch):
    """Worker wrapper paces FIRST, then runs compress.  Pacing-after-compress
    is what produced the bursty "first 2 workers fire instantly" pattern;
    asserting the order here pins the fix."""
    events: list[str] = []

    def fake_compress(rid, path, camera, tier, rtype, encoder, ctx):
        events.append("compress")
        return True

    monkeypatch.setattr(fc, "compress_one", fake_compress)

    fake_ctx = MagicMock()

    def fake_acquire(_stopping):
        events.append("acquire")

    fake_ctx.rate_limiter.acquire = fake_acquire
    stopping = threading.Event()

    result = fc._pace_then_compress(
        stopping, "rid-1", "/tmp/x.mp4", "cam", 1, "continuous", "cpu", fake_ctx
    )

    assert result is True
    assert events == ["acquire", "compress"]


def test_pace_then_compress_propagates_compress_exception(monkeypatch):
    """A compress failure propagates to the caller after the slot was
    already consumed.  No try/finally needed since acquire runs first."""
    events: list[str] = []

    def boom(*_a, **_k):
        events.append("compress")
        raise RuntimeError("encoder crashed")

    monkeypatch.setattr(fc, "compress_one", boom)

    fake_ctx = MagicMock()
    fake_ctx.rate_limiter.acquire = lambda _stopping: events.append("acquire")
    stopping = threading.Event()

    with pytest.raises(RuntimeError):
        fc._pace_then_compress(
            stopping, "r", "/p", "cam", 1, "continuous", "cpu", fake_ctx
        )

    assert events == ["acquire", "compress"]


def test_run_main_loop_paces_via_real_rate_limiter(monkeypatch, tmp_path):
    """End-to-end: don't mock the limiter; pre-set its target and verify
    workers sleep at the expected per-file interval."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    eligible_calls = {"n": 0}

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        return (
            [_eligible_row("r1"), _eligible_row("r2"), _eligible_row("r3")]
            if eligible_calls["n"] == 1
            else []
        )

    # Pin the inline target update to 30/min so we can verify per-file pacing
    # at a known rate (interval=2s).
    monkeypatch.setattr(fc, "_effective_throttle_target", lambda _ctx: 30.0)
    monkeypatch.setattr(fc, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc, "compress_one", lambda *a, **k: True)

    sleeps: list[float] = []
    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        sleeps.append(timeout)
        if timeout is not None:
            t["now"] += timeout
        # The 60s post-batch sleep ends the test.
        if timeout is not None and timeout >= fc._THROTTLE_WINDOW_SEC:
            stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    fc.run_main_loop(ctx, "cpu", stopping, housekeeping_interval_sec=999_999)

    # 3 files at target=30/min → interval=2.0s.  First file: no wait.
    # Next two: 2.0s each.  Then trailing _THROTTLE_WINDOW_SEC post-batch sleep.
    throttle_sleeps = [s for s in sleeps if s != fc._THROTTLE_WINDOW_SEC]
    assert len(throttle_sleeps) == 2
    for s in throttle_sleeps:
        assert s == pytest.approx(2.0, abs=0.05)
    assert sleeps[-1] == fc._THROTTLE_WINDOW_SEC

    _close_ctx(ctx)
    frigate_conn.close()


def test_measure_workload_per_min_counts_backlog_plus_incoming(tmp_path):
    """Workload includes backlog (already past line) AND incoming next minute.
    Done recordings excluded."""
    frigate_db = tmp_path / "frigate.db"
    conn = _make_frigate_db(frigate_db)
    now = time.time()
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))
    cam_name = next(iter(cfg.cameras))
    t1_min_days = cfg.cameras[cam_name].tier1.min_days

    # 4 backlog + 3 about to cross within 60s = 7.
    for i in range(4):
        _insert_recording(
            conn,
            f"backlog-{i}",
            cam_name,
            f"/tmp/back-{i}.mp4",
            start_time=now - (t1_min_days + 1) * 86400 - i * 60,
        )
    for i in range(3):
        _insert_recording(
            conn,
            f"incoming-{i}",
            cam_name,
            f"/tmp/inc-{i}.mp4",
            start_time=now - t1_min_days * 86400 + (i + 1) * 10,
        )
    # One done recording — must NOT count.
    _insert_recording(
        conn,
        "done-1",
        cam_name,
        "/tmp/done.mp4",
        start_time=now - (t1_min_days + 2) * 86400,
    )
    conn.close()

    ctx = _make_eligible_ctx(tmp_path, frigate_db)
    fc._record(
        ctx.compress_db,
        recording_id="done-1",
        camera=cam_name,
        path="/tmp/done.mp4",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )

    # 7 / 1 min = 7/min.
    workload = fc.measure_workload_per_min(ctx)
    assert workload == pytest.approx(7.0, abs=0.01)
    _close_ctx(ctx)


def test_rate_limiter_paces_concurrent_callers(monkeypatch):
    """Limiter spaces calls to interval = 60/target across all callers."""
    rl = fc.RateLimiter()
    rl.set_target(60)
    stopping = threading.Event()
    sleeps: list[float] = []

    monkeypatch.setattr(stopping, "wait", lambda timeout=None: sleeps.append(timeout))

    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    # 3 acquires at 60/min → interval 1.0s; first is free, next two sleep.
    rl.acquire(stopping)  # next=1001, wait=0 → no sleep
    rl.acquire(stopping)  # next=1002, wait=1 → sleep 1
    rl.acquire(stopping)  # next=1003, wait=2 → sleep 2

    assert sleeps == [pytest.approx(1.0), pytest.approx(2.0)]


def test_rate_limiter_disabled_is_noop(monkeypatch):
    """target_per_min=0 returns immediately, no sleep, no state change."""
    rl = fc.RateLimiter()
    # default target=0
    before = rl.next_allowed
    stopping = threading.Event()
    monkeypatch.setattr(stopping, "wait", lambda timeout=None: pytest.fail("slept"))
    rl.acquire(stopping)
    assert rl.next_allowed == before


def test_rate_limiter_set_target_reads_atomically(monkeypatch):
    """set_target updates the active rate; subsequent acquires use the new value."""
    rl = fc.RateLimiter()
    stopping = threading.Event()
    sleeps: list[float] = []
    monkeypatch.setattr(stopping, "wait", lambda timeout=None: sleeps.append(timeout))
    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    rl.set_target(60)  # interval 1s
    rl.acquire(stopping)  # warmup, no sleep
    rl.acquire(stopping)  # interval 1s → sleep 1
    rl.set_target(30)  # interval now 2s
    rl.acquire(stopping)  # next was 1002, now=1000, wait=2 → sleep 2 (still 1s
    # interval state from before, but new interval applies to next_allowed advance)

    assert sleeps == [pytest.approx(1.0), pytest.approx(2.0)]
