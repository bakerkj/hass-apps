# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for get_eligible_recordings, compress_one, and housekeeping."""

from __future__ import annotations

import sqlite3
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
    fc._attach_frigate(compress_conn, cfg, "frigate")
    return fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
    )


def _insert_probed(
    frigate_conn,
    compress_conn,
    rid,
    camera,
    path,
    start_time,
    *,
    motion=None,
    objects=None,
    file_size=1000,
):
    """Insert a recording into Frigate + a matching probed files row.

    The eligibility query drives from ``files`` now, so a recording
    is only visible to it after the probe loop has inserted a files
    row.  Mirrors production where every recording gets probed within
    a minute or so.
    """
    _insert_recording(frigate_conn, rid, camera, path, start_time, motion, objects)
    rtype = "object" if objects else "motion" if motion else "continuous"
    compress_conn.execute(
        "INSERT OR REPLACE INTO files"
        " (recording_id, camera, path, recording_type, file_size, start_time, scanned_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (rid, camera, path, rtype, file_size, start_time, "2026-01-01T00:00:00"),
    )
    compress_conn.commit()


def _make_compress_one_ctx(tmp_path, src: Path, frigate_db: Path):
    """Build a CompressorContext for compress_one tests."""
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))
    compress_conn = _open_compress_db(tmp_path)
    fc._attach_frigate(compress_conn, cfg, "frigate")
    return fc.CompressorContext(
        cfg=cfg,
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
    fc._attach_frigate(compress_conn, cfg, "frigate")
    return fc.CompressorContext(
        cfg=cfg,
        compress_db=compress_conn,
    )


def _close_ctx(ctx):
    ctx.compress_db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# get_eligible_recordings
# ═══════════════════════════════════════════════════════════════════════════════


def test_get_eligible_recordings_returns_old_enough(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)
    _insert_probed(
        frigate_conn,
        compress_conn,
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
    compress_conn = _open_compress_db(tmp_path)
    # Probe first (sets start_time), then transition to error.
    _insert_probed(
        frigate_conn,
        compress_conn,
        "rec4",
        "cam1",
        "/media/cam1/d.mp4",
        time.time() - 10 * 86400,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
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
    compress_conn = _open_compress_db(tmp_path)
    _insert_probed(
        frigate_conn,
        compress_conn,
        "rec5",
        "cam1",
        "/media/cam1/e.mp4",
        time.time() - 35 * 86400,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)

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
    compress_conn = _open_compress_db(tmp_path)
    _insert_probed(
        frigate_conn,
        compress_conn,
        "rec6",
        "cam1",
        "/media/cam1/f.mp4",
        time.time() - 10 * 86400,
        motion=10,
        objects=3,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
    results = fc.get_eligible_recordings(ctx)
    assert results[0]["recording_type"] == "object"

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_interleaves_tiers_when_caught_up(tmp_path):
    """When the eligible count fits in one batch (steady state), tier-1
    and tier-2 chained alternate by per-tier rank — within each rank
    tier-1 sorts first.  Pure ``start_time`` order doesn't interleave
    because tier-2 chained rows are always older than tier-1 rows."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    # Older recording: t1 already done → eligible for tier 2 (chained).
    _insert_probed(
        frigate_conn,
        compress_conn,
        "old-t2",
        "cam1",
        "/m/old.mp4",
        time.time() - 60 * 86400,
    )
    # Newer recording: t1 not done → eligible for tier 1.
    _insert_probed(
        frigate_conn,
        compress_conn,
        "new-t1",
        "cam1",
        "/m/new.mp4",
        time.time() - 10 * 86400,
    )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
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
    # Caught up: rank 1 of tier-1 before rank 1 of tier-2.
    assert [r["recording_id"] for r in results] == ["new-t1", "old-t2"]
    assert [r["tier"] for r in results] == [1, 2]

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_interleaves_multiple_pairs_when_caught_up(tmp_path):
    """With three tier-1 and three tier-2 chained candidates, steady-state
    output alternates: ``t1[0], t2[0], t1[1], t2[1], t1[2], t2[2]``."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    now = time.time()
    # Three tier-2 chained candidates (older — past tier2.min_days, t1 ok).
    for i, age_days in enumerate([90, 80, 70]):
        rid = f"t2-{i}"
        _insert_probed(
            frigate_conn,
            compress_conn,
            rid,
            "cam1",
            f"/m/{rid}.mp4",
            now - age_days * 86400,
        )
    # Three tier-1 candidates (newer — past tier1.min_days, t1 NULL).
    for i, age_days in enumerate([15, 12, 10]):
        rid = f"t1-{i}"
        _insert_probed(
            frigate_conn,
            compress_conn,
            rid,
            "cam1",
            f"/m/{rid}.mp4",
            now - age_days * 86400,
        )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
    # Mark the three tier-2 candidates' tier-1 as already done.
    for i in range(3):
        fc._record(
            ctx.compress_db,
            recording_id=f"t2-{i}",
            camera="cam1",
            path=f"/m/t2-{i}.mp4",
            tier=1,
            recording_type="continuous",
            encoder="cpu",
            size_before=1000,
            size_after=500,
            duration_sec=1.0,
            status=fc.STATUS_OK,
        )

    results = fc.get_eligible_recordings(ctx)
    # rk=1: t1-0 (oldest tier-1), t2-0 (oldest tier-2)
    # rk=2: t1-1, t2-1
    # rk=3: t1-2, t2-2
    assert [r["recording_id"] for r in results] == [
        "t1-0",
        "t2-0",
        "t1-1",
        "t2-1",
        "t1-2",
        "t2-2",
    ]
    assert [r["tier"] for r in results] == [1, 2, 1, 2, 1, 2]

    _close_ctx(ctx)
    frigate_conn.close()


def test_get_eligible_recordings_orders_tier1_first_when_catching_up(
    tmp_path, monkeypatch
):
    """When the eligible count exceeds one batch (catch-up), tier-1 sorts
    ahead of tier-2 regardless of ``start_time`` so the heavier encodes
    drain first."""
    # Lower the batch size so the test only needs a handful of rows to
    # cross the catch-up threshold.  The threshold tracks the LIMIT.
    monkeypatch.setattr(fc.eligibility, "_ELIGIBLE_BATCH_SIZE", 2)

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    now = time.time()
    # Two tier-2 candidates (older) — t1 already done.
    for i, age_days in enumerate([60, 50]):
        rid = f"old-t2-{i}"
        _insert_probed(
            frigate_conn,
            compress_conn,
            rid,
            "cam1",
            f"/m/{rid}.mp4",
            now - age_days * 86400,
        )
    # Two tier-1 candidates (newer) — t1 not yet done.
    for i, age_days in enumerate([10, 9]):
        rid = f"new-t1-{i}"
        _insert_probed(
            frigate_conn,
            compress_conn,
            rid,
            "cam1",
            f"/m/{rid}.mp4",
            now - age_days * 86400,
        )

    ctx = _make_eligible_ctx(tmp_path, frigate_db, compress_conn)
    for i in range(2):
        fc._record(
            ctx.compress_db,
            recording_id=f"old-t2-{i}",
            camera="cam1",
            path=f"/m/old-t2-{i}.mp4",
            tier=1,
            recording_type="continuous",
            encoder="cpu",
            size_before=1000,
            size_after=500,
            duration_sec=1.0,
            status=fc.STATUS_OK,
        )

    results = fc.get_eligible_recordings(ctx)
    # 4 total eligible > batch size of 2 → catch-up → tier-1 first.
    # LIMIT 2, so we get the two tier-1 rows in start_time order.
    assert [r["tier"] for r in results] == [1, 1]
    assert [r["recording_id"] for r in results] == ["new-t1-0", "new-t1-1"]

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
    ctx.compress_db.commit()

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


def test_compress_one_detached_ffmpeg_calls_run_detached(tmp_path):
    """When cfg.detached_ffmpeg is True, the worker dispatches via
    run_detached instead of subprocess.run.  Verifies wiring only —
    run_detached's own behaviour is covered in test_fc_detached_subprocess.
    """
    ctx, src = _setup_compress_one(tmp_path)
    ctx.cfg.detached_ffmpeg = True

    def fake_detached(cmd, *, timeout, stderr_max_len):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with (
        patch(
            "frigate_compressor.compressor.run_detached", side_effect=fake_detached
        ) as run_detached_mock,
        patch("subprocess.run") as subprocess_run_mock,
    ):
        result = _compress_one(ctx, src)

    assert result is True
    assert run_detached_mock.called
    assert not subprocess_run_mock.called
    assert _db_row(ctx)["t1_status"] == fc.STATUS_OK
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

    # Make the segment_size UPDATE fail at the SQLite layer.  Frigate is
    # ATTACHed rw to compress_db as ``frigate``; re-attach as read-only
    # (after detaching) so the safety-check SELECT still succeeds but the
    # later ``UPDATE frigate.recordings`` raises ``OperationalError``.
    ctx.compress_db.execute("DETACH DATABASE frigate")
    fdb = str(ctx.cfg.frigate_db).replace('"', "")
    ctx.compress_db.execute(f'ATTACH DATABASE "file:{fdb}?mode=ro" AS frigate')

    try:
        with patch("subprocess.run", side_effect=fake_run):
            result = _compress_one(ctx, src)

        assert result is True
        row = _db_row(ctx)
        assert row["t1_status"] == fc.STATUS_SEGMENT_UPDATE_FAILED
    finally:
        ctx.compress_db.close()


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

    seg_row = ctx.compress_db.execute(
        "SELECT segment_size FROM frigate.recordings WHERE id='seg1'"
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


def test_housekeeping_deletes_orphan_t2_siblings(tmp_path):
    """When Frigate retires a segment, our sibling .t2.mp4 must be cleaned up.

    Frigate's retention deletes the primary .mp4 but doesn't know about our
    sibling. After the recording is gone from frigate.recordings, housekeeping
    should delete the sibling before pruning the compress_db row.
    """
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    # Set up a recording that's already been retired by Frigate (no row in
    # frigate.recordings) but our compress_db row is in t2_status='direct'
    # state with a sibling file still on disk.
    primary = tmp_path / "recordings" / "cam1" / "27.18.mp4"
    primary.parent.mkdir(parents=True)
    primary.write_bytes(b"primary tier-1 file (about to be cleaned up too)")
    sibling = fc.sibling_path(primary)
    sibling.write_bytes(b"sibling tier-2 file (the one we need to delete)")
    assert sibling.exists()

    # compress_db row in 'direct' state, but no matching frigate.recordings row.
    compress_conn.execute(
        "INSERT INTO files"
        " (recording_id, camera, path, recording_type, file_size, start_time, "
        "  scanned_at, t1_status, t2_status)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "orphan_direct",
            "cam1",
            str(primary),
            "continuous",
            1000,
            time.time() - 100 * 86400,
            "2026-01-01T00:00:00",
            fc.STATUS_OK,
            fc.STATUS_DIRECT,
        ),
    )
    compress_conn.commit()

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)

    # Sibling deleted by orphan-cleanup pass
    assert not sibling.exists()
    # Compress_db row pruned by the standard prune step
    remaining = compress_conn.execute(
        "SELECT recording_id FROM files WHERE recording_id='orphan_direct'"
    ).fetchone()
    assert remaining is None

    _close_ctx(ctx)
    frigate_conn.close()


def test_housekeeping_keeps_sibling_when_recording_alive(tmp_path):
    """A sibling file for a still-active recording must NOT be deleted."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    primary = tmp_path / "recordings" / "cam1" / "27.18.mp4"
    primary.parent.mkdir(parents=True)
    primary.write_bytes(b"primary")
    sibling = fc.sibling_path(primary)
    sibling.write_bytes(b"sibling - should survive")

    # Recording IS in Frigate's table (still alive)
    _insert_recording(
        frigate_conn, "alive_direct", "cam1", str(primary), time.time() - 100 * 86400
    )
    frigate_conn.commit()
    compress_conn.execute(
        "INSERT INTO files"
        " (recording_id, camera, path, recording_type, file_size, start_time, "
        "  scanned_at, t1_status, t2_status)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "alive_direct",
            "cam1",
            str(primary),
            "continuous",
            1000,
            time.time() - 100 * 86400,
            "2026-01-01T00:00:00",
            fc.STATUS_OK,
            fc.STATUS_DIRECT,
        ),
    )
    compress_conn.commit()

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)

    assert sibling.exists()  # Sibling preserved
    remaining = compress_conn.execute(
        "SELECT recording_id FROM files WHERE recording_id='alive_direct'"
    ).fetchone()
    assert remaining is not None  # Row preserved

    _close_ctx(ctx)
    frigate_conn.close()


def test_housekeeping_handles_missing_sibling_gracefully(tmp_path):
    """If row is t2_status='direct' but sibling is already gone, no error."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    compress_conn = _open_compress_db(tmp_path)

    primary = tmp_path / "recordings" / "cam1" / "27.18.mp4"
    primary.parent.mkdir(parents=True)
    primary.write_bytes(b"primary")
    # No sibling file created — the cleanup path must handle this no-op.

    compress_conn.execute(
        "INSERT INTO files"
        " (recording_id, camera, path, recording_type, file_size, start_time, "
        "  scanned_at, t1_status, t2_status)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "orphan_no_sib",
            "cam1",
            str(primary),
            "continuous",
            1000,
            time.time() - 100 * 86400,
            "2026-01-01T00:00:00",
            fc.STATUS_OK,
            fc.STATUS_DIRECT,
        ),
    )
    compress_conn.commit()

    ctx = _make_housekeeping_ctx(tmp_path, frigate_db, compress_conn)
    fc.run_housekeeping(ctx)  # must not raise

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


# ═══════════════════════════════════════════════════════════════════════════════
# adapt_pace_scale (drift-control proportional controller)
# ═══════════════════════════════════════════════════════════════════════════════


def test_adapt_pace_scale_shrinks_on_overshoot():
    """Iteration ran longer than the window → scale shrinks proportionally."""
    # Overshoot of 1.4s on 60s window: delta = -1.4/60 * 0.5 ≈ -0.01167.
    new_scale = fc.adapt_pace_scale(1.0, 61.4)
    assert new_scale == pytest.approx(1.0 - 1.4 / 60.0 * fc._PACE_GAIN)
    assert new_scale < 1.0


def test_adapt_pace_scale_grows_on_undershoot():
    """Iteration ran short of the window → scale grows toward the ceiling."""
    new_scale = fc.adapt_pace_scale(0.95, 58.0)
    # Undershoot of 2s: delta = +2/60 * 0.5 ≈ +0.0167.
    expected = min(fc._PACE_SCALE_MAX, 0.95 + 2.0 / 60.0 * fc._PACE_GAIN)
    assert new_scale == pytest.approx(expected)
    assert new_scale > 0.95


def test_adapt_pace_scale_clamps_per_cycle_change():
    """A huge overshoot still moves the scale by at most _PACE_PER_CYCLE_CLAMP."""
    # Overshoot of 30s would imply delta = -0.25, clamped to -0.05.
    new_scale = fc.adapt_pace_scale(1.0, 90.0)
    assert new_scale == pytest.approx(1.0 - fc._PACE_PER_CYCLE_CLAMP)


def test_adapt_pace_scale_floor():
    """Repeated overshoots can't drive the scale below _PACE_SCALE_MIN."""
    scale = 0.91
    # One iter with huge overshoot — clamp would take it to 0.86, but floor
    # holds at 0.90.
    new_scale = fc.adapt_pace_scale(scale, 120.0)
    assert new_scale == fc._PACE_SCALE_MIN


def test_adapt_pace_scale_ceiling():
    """Repeated undershoots can't drive the scale above _PACE_SCALE_MAX."""
    scale = 1.00
    new_scale = fc.adapt_pace_scale(scale, 10.0)
    assert new_scale == fc._PACE_SCALE_MAX


def test_adapt_pace_scale_converges_under_steady_overshoot():
    """A controller fed a constant +1.4s overshoot every cycle settles within
    a handful of cycles to a scale that would (in production) eliminate the
    overshoot.  This is a behaviour smoke test, not a numerical fit."""
    scale = 1.0
    history = [scale]
    for _ in range(20):
        # Simulate "elapsed = 60 + leftover overshoot proportional to scale".
        # Higher scale → more overshoot; lower scale → less.  Approximation:
        # at scale 0.97 the per-iter overshoot is ~0.
        overshoot = (scale - 0.97) * 60.0  # 0 at scale=0.97, +1.8 at scale=1.0
        elapsed = fc._THROTTLE_WINDOW_SEC + overshoot
        scale = fc.adapt_pace_scale(scale, elapsed)
        history.append(scale)

    # Final scale should be near the equilibrium (~0.97) and stable.
    assert 0.96 < history[-1] < 0.98
    # Last few values vary by less than 1% — controller has settled.
    assert max(history[-3:]) - min(history[-3:]) < 0.005


def test_run_main_loop_sleeps_remainder_after_partial_batch(monkeypatch, tmp_path):
    """A partial batch that processes fast (mocked instant compress) means
    elapsed << window — the loop should sleep approximately the full window
    to keep the iteration cycle ≈ _THROTTLE_WINDOW_SEC."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    eligible_calls = {"n": 0}

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        return [_eligible_row("r1"), _eligible_row("r2")]

    monkeypatch.setattr(fc.app, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc.app, "compress_one", lambda *a, **k: True)
    # Skip per-file throttle pacing so we observe only the iteration sleep.
    monkeypatch.setattr(fc.RateLimiter, "acquire", lambda self, stopping: None)

    stopping = threading.Event()
    real_wait = stopping.wait
    wait_timeouts: list[float | None] = []

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    assert eligible_calls["n"] == 1
    # elapsed ≈ 0 (instant mocked compress) → sleep ≈ full window.
    assert wait_timeouts == [pytest.approx(fc._THROTTLE_WINDOW_SEC, abs=1.0)]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_skips_sleep_when_processing_overruns_window(
    monkeypatch, tmp_path
):
    """If processing took longer than _THROTTLE_WINDOW_SEC (catchup mode:
    batch outran capacity), the loop must not sleep — it should re-query
    immediately so backlog drains flat-out."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    # Mocked time so we can simulate "processing took 90 seconds".
    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    eligible_calls = {"n": 0}

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        if eligible_calls["n"] == 1:
            return [_eligible_row("r1")]
        return []

    def fake_compress(*_args, **_kwargs):
        # Simulate slow compression — push wall-clock past the window.
        t["now"] += fc._THROTTLE_WINDOW_SEC + 30
        return True

    monkeypatch.setattr(fc.app, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc.app, "compress_one", fake_compress)
    monkeypatch.setattr(fc.RateLimiter, "acquire", lambda self, stopping: None)
    # Make the no-work path return a sentinel so the test ends predictably.
    # The loop adds _THROTTLE_WINDOW_SEC to this so the first batch on wake
    # has one full window of accumulated work.
    monkeypatch.setattr(fc.app, "time_until_next_eligible", lambda _ctx: 333.0)

    stopping = threading.Event()
    real_wait = stopping.wait
    wait_timeouts: list[float | None] = []

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        if timeout is not None:
            t["now"] += timeout
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    # Pass 1's processing took 90s (> 60s window), so no sleep was scheduled
    # — we went straight to pass 2.  Pass 2 was empty → no-work sleep
    # (333 + window) was the only call to wait().
    assert eligible_calls["n"] == 2
    assert wait_timeouts == [333.0 + fc._THROTTLE_WINDOW_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_sleeps_until_next_eligible_when_no_work(monkeypatch, tmp_path):
    """An empty queue means we sleep until the next recording becomes
    eligible PLUS one full window — so the first batch on wake has a
    window's worth of accumulated work, not a lone first-eligible file.
    Capped at MAX_SLEEP_SEC."""
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

    monkeypatch.setattr(fc.app, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc.app, "compress_one", fake_compress)
    # Pin to a known value so we can assert it precisely.
    monkeypatch.setattr(fc.app, "time_until_next_eligible", lambda _ctx: 137.0)

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    assert eligible_calls["n"] == 1
    assert compress_calls["n"] == 0
    assert wait_timeouts == [137.0 + fc._THROTTLE_WINDOW_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_no_work_sleep_capped_at_max(monkeypatch, tmp_path):
    """If next eligible is far in the future, the sleep is clamped."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc.app, "get_eligible_recordings", lambda _ctx: [])
    monkeypatch.setattr(fc.app, "time_until_next_eligible", lambda _ctx: 99_999.0)

    stopping = threading.Event()
    real_wait = stopping.wait
    wait_timeouts: list[float | None] = []

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    assert wait_timeouts == [fc.MAX_SLEEP_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_no_work_sleep_handles_query_exception(monkeypatch, tmp_path):
    """If the no-work query throws, fall back to MAX_SLEEP_SEC instead of
    crashing the loop."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc.app, "get_eligible_recordings", lambda _ctx: [])

    def boom(_ctx):
        raise RuntimeError("frigate db went away")

    monkeypatch.setattr(fc.app, "time_until_next_eligible", boom)

    stopping = threading.Event()
    real_wait = stopping.wait
    wait_timeouts: list[float | None] = []

    def fake_wait(timeout=None):
        wait_timeouts.append(timeout)
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    assert wait_timeouts == [fc.MAX_SLEEP_SEC]

    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# throttle (target = len(eligible))
# ═══════════════════════════════════════════════════════════════════════════════


def test_run_main_loop_sets_target_to_batch_size(monkeypatch, tmp_path):
    """The throttle target is set to ``len(eligible)`` files/min — exactly
    the work in the current batch over one minute.  No lookahead, no
    separate workload measurement."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(
        fc.app,
        "get_eligible_recordings",
        lambda _ctx: [_eligible_row(f"r{i}") for i in range(7)],
    )
    monkeypatch.setattr(fc.app, "compress_one", lambda *a, **k: True)

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    # target = N / pace_scale; pace_scale starts at 0.98 → target ≈ 7.143
    # for a 7-row batch on iter 1 (the only iter the test runs).
    assert ctx.rate_limiter.target_per_min == pytest.approx(7 / 0.98)

    _close_ctx(ctx)
    frigate_conn.close()


def test_run_main_loop_target_unchanged_when_no_work(monkeypatch, tmp_path):
    """An empty batch leaves the limiter target alone (no DB query needed)."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    monkeypatch.setattr(fc.app, "get_eligible_recordings", lambda _ctx: [])

    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    # Pre-set a sentinel target AFTER the limiter is otherwise idle; the
    # loop must not overwrite it on the empty pass.
    ctx.rate_limiter.set_target(123.0)
    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    assert ctx.rate_limiter.target_per_min == pytest.approx(123.0)

    _close_ctx(ctx)
    frigate_conn.close()


def test_pace_then_compress_acquires_before_compressing(monkeypatch):
    """Worker wrapper paces FIRST, then runs compress.  Pacing-after-compress
    is what produced the bursty "first 2 workers fire instantly" pattern;
    asserting the order here pins the fix."""
    events: list[str] = []

    def fake_compress(rid, path, camera, tier, rtype, encoder, ctx, probe_data=None):
        events.append("compress")
        return True

    monkeypatch.setattr(fc.app, "compress_one", fake_compress)

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

    monkeypatch.setattr(fc.app, "compress_one", boom)

    fake_ctx = MagicMock()
    fake_ctx.rate_limiter.acquire = lambda _stopping: events.append("acquire")
    stopping = threading.Event()

    with pytest.raises(RuntimeError):
        fc._pace_then_compress(
            stopping, "r", "/p", "cam", 1, "continuous", "cpu", fake_ctx
        )

    assert events == ["acquire", "compress"]


def test_run_main_loop_paces_via_real_rate_limiter(monkeypatch, tmp_path):
    """End-to-end: don't mock the limiter.  6 files at target=6/0.98/min
    (pace_scale starts at 0.98) → interval 9.8s → 5 pacing sleeps of
    9.8s each, then the elapsed-remainder fills out the iteration to
    one window (60 - 5*9.8 = 11s), then the no-work sleep ends the
    test."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    ctx = _make_eligible_ctx(tmp_path, frigate_db)

    t = {"now": 1000.0}
    monkeypatch.setattr(fc.time, "time", lambda: t["now"])

    eligible_calls = {"n": 0}

    def fake_eligible(_ctx):
        eligible_calls["n"] += 1
        if eligible_calls["n"] == 1:
            return [_eligible_row(f"r{i}") for i in range(6)]
        return []

    monkeypatch.setattr(fc.app, "get_eligible_recordings", fake_eligible)
    monkeypatch.setattr(fc.app, "compress_one", lambda *a, **k: True)
    # No-work sleep will be capped to MAX_SLEEP_SEC; mock returns a value
    # well above that so we can identify it as "the no-work sleep" in the
    # asserted sleep list.
    monkeypatch.setattr(fc.app, "time_until_next_eligible", lambda _ctx: 9999.0)

    sleeps: list[float] = []
    stopping = threading.Event()
    real_wait = stopping.wait

    def fake_wait(timeout=None):
        sleeps.append(timeout)
        if timeout is not None:
            t["now"] += timeout
        # The no-work sleep (capped at MAX_SLEEP_SEC) ends the test.
        if timeout is not None and timeout >= fc.MAX_SLEEP_SEC:
            stopping.set()
        return real_wait(timeout=0)

    monkeypatch.setattr(stopping, "wait", fake_wait)

    with ThreadPoolExecutor(max_workers=1) as _pool:
        fc.run_main_loop(ctx, "cpu", stopping, 999_999, _pool)

    # Iter 1: 6 files at interval=9.8s.  First file fires immediately;
    #   workers sleep 9.8s × 5 between starts (49s total wall-clock).
    #   Then elapsed=49s, post-batch sleep=11s fills out the window.
    # Iter 2: empty → MAX-capped no-work sleep ends the test.
    assert sleeps[:5] == [pytest.approx(9.8, abs=0.05)] * 5
    assert sleeps[5] == pytest.approx(11.0, abs=0.05)
    assert sleeps[-1] == fc.MAX_SLEEP_SEC
    assert len(sleeps) == 7

    _close_ctx(ctx)
    frigate_conn.close()


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


# ═══════════════════════════════════════════════════════════════════════════════
# compress_direct (dual-output: native → tier-1 + tier-2 sibling)
# ═══════════════════════════════════════════════════════════════════════════════


def _setup_compress_direct(tmp_path, *, recording_type="motion"):
    """Same setup as compress_one but with tier2.source="direct".

    Returns (ctx, src_path, sibling_path).
    """
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
        motion=5 if recording_type != "object" else 0,
        objects=3 if recording_type == "object" else 0,
    )
    frigate_conn.close()

    # Use a per-camera override so we add source="direct" without clobbering
    # test_defaults' tier2 block (the yaml_defaults override path does a
    # shallow update that would drop tier2.enabled/quality/etc.).
    cfg = _make_config(
        tmp_path,
        frigate_db=str(frigate_db),
        yaml_cameras={"cam1": {"tier2": {"source": "direct"}}},
    )
    compress_conn = _open_compress_db(tmp_path)
    fc._attach_frigate(compress_conn, cfg, "frigate")
    ctx = fc.CompressorContext(cfg=cfg, compress_db=compress_conn)

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
    ctx.compress_db.commit()

    return ctx, src, fc.sibling_path(src)


def _compress_direct(ctx, src, *, recording_id="r1", recording_type="motion"):
    return fc.compress_direct(
        recording_id=recording_id,
        path=str(src),
        camera="cam1",
        recording_type=recording_type,
        encoder="cpu",
        ctx=ctx,
    )


def _fake_dual_run(t1_size=5000, t2_size=2000, returncode=0, stderr=""):
    """Build a subprocess.run-replacement that writes BOTH .mp4 outputs.

    The dual-output ffmpeg cmd has multiple .mp4 paths: -i <input> followed
    by output args ending in .mp4 paths (one per output).  The mock writes
    each non-input .mp4 path, with the first matching t1_size and the rest
    matching t2_size.  Uses the .tmp.{rid}.t{1,2}.mp4 naming convention.
    """

    def fake_run(cmd, **kwargs):
        # Find output paths (.mp4 entries that are not the -i argument)
        outputs = []
        for i, arg in enumerate(cmd):
            if arg.endswith(".mp4") and (i == 0 or cmd[i - 1] != "-i"):
                outputs.append(arg)
        # First output is tier-1, rest are tier-2 (only one in our case)
        for j, out in enumerate(outputs):
            size = t1_size if ".t1.mp4" in out else t2_size
            Path(out).write_bytes(b"y" * size)
        m = MagicMock()
        m.returncode = returncode
        m.stderr = stderr
        return m

    return fake_run


def test_compress_direct_writes_both_outputs(tmp_path):
    ctx, src, sib = _setup_compress_direct(tmp_path)
    with patch("subprocess.run", side_effect=_fake_dual_run()):
        result = _compress_direct(ctx, src)
    assert result is True
    # Primary path now contains tier-1 output (5000 bytes)
    assert src.exists()
    assert src.stat().st_size == 5000
    # Sibling path contains tier-2 output (2000 bytes)
    assert sib.exists()
    assert sib.stat().st_size == 2000
    _close_ctx(ctx)


def test_compress_direct_db_marks_t1_ok_and_t2_direct(tmp_path):
    ctx, src, _ = _setup_compress_direct(tmp_path)
    with patch("subprocess.run", side_effect=_fake_dual_run()):
        _compress_direct(ctx, src)
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t2_status"] == fc.STATUS_DIRECT
    assert row["t1_file_size"] == 5000
    assert row["t2_file_size"] == 2000
    _close_ctx(ctx)


def test_compress_direct_updates_frigate_segment_size(tmp_path):
    ctx, src, _ = _setup_compress_direct(tmp_path)
    with patch("subprocess.run", side_effect=_fake_dual_run(t1_size=4096)):
        _compress_direct(ctx, src)
    seg_row = ctx.compress_db.execute(
        "SELECT segment_size FROM frigate.recordings WHERE id='r1'"
    ).fetchone()
    # segment_size is in MB, set to tier-1 size
    assert seg_row["segment_size"] == pytest.approx(4096 / (1024 * 1024), rel=1e-4)
    _close_ctx(ctx)


def test_compress_direct_falls_back_to_tier1_when_t2_disabled(tmp_path):
    """If tier2 is disabled for this rtype, compress_direct delegates to compress_one(tier=1)."""
    src = tmp_path / "recordings" / "cam1" / "clip.mp4"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"x" * 10000)
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn, "r1", "cam1", str(src), time.time() - 10 * 86400, motion=5
    )
    frigate_conn.close()
    # Disable tier2.motion specifically; still source=direct
    cfg = _make_config(
        tmp_path,
        frigate_db=str(frigate_db),
        yaml_defaults={
            "tier2": {
                "source": "direct",
                "motion": {"enabled": False},
            }
        },
    )
    compress_conn = _open_compress_db(tmp_path)
    fc._attach_frigate(compress_conn, cfg, "frigate")
    ctx = fc.CompressorContext(cfg=cfg, compress_db=compress_conn)
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
    ctx.compress_db.commit()

    def fake_run(cmd, **kwargs):
        Path(cmd[-1]).write_bytes(b"y" * 5000)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = fc.compress_direct("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is True
    # Single-output flow: no sibling created
    assert not fc.sibling_path(src).exists()
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_OK
    # tier-2 not touched
    assert row["t2_status"] is None
    _close_ctx(ctx)


def test_compress_direct_ffmpeg_failure_keeps_native(tmp_path):
    ctx, src, sib = _setup_compress_direct(tmp_path)
    with patch(
        "subprocess.run",
        side_effect=_fake_dual_run(returncode=1, stderr="ffmpeg error"),
    ):
        result = _compress_direct(ctx, src)
    assert result is False
    # Native untouched
    assert src.stat().st_size == 10000
    # No sibling
    assert not sib.exists()
    row = _db_row(ctx)
    assert row["t1_status"] == fc.STATUS_ERROR
    assert row["t2_status"] == fc.STATUS_ERROR
    _close_ctx(ctx)


def test_compress_direct_output_missing_keeps_native(tmp_path):
    """If ffmpeg returns 0 but one of the outputs wasn't created."""
    ctx, src, sib = _setup_compress_direct(tmp_path)

    def fake_run(cmd, **kwargs):
        # Write only the FIRST output (t1) — leave t2 missing
        for i, arg in enumerate(cmd):
            if arg.endswith(".t1.mp4") and (i == 0 or cmd[i - 1] != "-i"):
                Path(arg).write_bytes(b"y" * 5000)
                break  # don't write t2
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    with patch("subprocess.run", side_effect=fake_run):
        result = _compress_direct(ctx, src)
    assert result is False
    # Native preserved (atomic replace gated on both outputs existing)
    assert src.stat().st_size == 10000
    assert not sib.exists()
    _close_ctx(ctx)


def test_compress_direct_dry_run(tmp_path):
    ctx, src, sib = _setup_compress_direct(tmp_path)
    # Force dry_run on
    for cam in ctx.cfg.cameras.values():
        cam.dry_run = True

    def fail_run(cmd, **kwargs):
        pytest.fail("ffmpeg should not be invoked in dry_run")

    with patch("subprocess.run", side_effect=fail_run):
        result = _compress_direct(ctx, src)
    assert result is True
    assert src.stat().st_size == 10000  # native untouched
    assert not sib.exists()
    _close_ctx(ctx)


def test_get_eligible_recordings_excludes_direct_rows(tmp_path):
    """Sibling-swap rows (``t2_status='direct'``) are handled by the swap
    loop, not the encode loop, so they must NOT surface to
    ``get_eligible_recordings`` — only chained-tier-2 candidates do."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))
    compress_conn = _open_compress_db(tmp_path)
    fc._attach_frigate(compress_conn, cfg, "frigate")
    ctx = fc.CompressorContext(cfg=cfg, compress_db=compress_conn)

    # Direct-swap row: must be excluded.
    _insert_probed(
        frigate_conn,
        compress_conn,
        "rd",
        "cam",
        "/x/d.mp4",
        time.time() - 100 * 86400,
    )
    compress_conn.execute(
        "UPDATE files SET t1_status=?, t2_status=? WHERE recording_id=?",
        (fc.STATUS_OK, fc.STATUS_DIRECT, "rd"),
    )
    # Chained-tier-2 row: must be included.
    _insert_probed(
        frigate_conn,
        compress_conn,
        "rn",
        "cam",
        "/x/n.mp4",
        time.time() - 100 * 86400,
    )
    compress_conn.execute(
        "UPDATE files SET t1_status=? WHERE recording_id=?",
        (fc.STATUS_OK, "rn"),
    )
    compress_conn.commit()

    eligible = fc.get_eligible_recordings(ctx)
    by_id = {r["recording_id"]: r for r in eligible}
    assert "rd" not in by_id
    assert "rn" in by_id
    _close_ctx(ctx)
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _pace_then_compress dispatch — direct vs chained
# ═══════════════════════════════════════════════════════════════════════════════


def _fake_dispatch_ctx(*, source: str, t2_enabled: bool):
    """Minimal mock ctx with cfg.cameras['cam'].tier2.source + tier2.<rtype>.enabled."""
    cam = MagicMock()
    cam.tier2.source = source
    # The dispatch reads ``getattr(cam_cfg.tier2, rtype)``; here `rtype` is
    # always "continuous" in the dispatch tests below.
    cam.tier2.continuous.enabled = t2_enabled
    fake_ctx = MagicMock()
    fake_ctx.cfg.cameras = {"cam": cam}
    fake_ctx.rate_limiter.acquire = lambda _stopping: None
    return fake_ctx


def test_pace_then_compress_dispatches_to_direct_when_source_direct(monkeypatch):
    """tier=1, source=direct, t2 enabled → compress_direct, not compress_one."""
    calls: list[str] = []
    monkeypatch.setattr(
        fc.app,
        "compress_direct",
        lambda *a, **k: calls.append("direct") or True,
    )
    monkeypatch.setattr(
        fc.app,
        "compress_one",
        lambda *a, **k: calls.append("one") or True,
    )
    ctx = _fake_dispatch_ctx(source="direct", t2_enabled=True)
    fc._pace_then_compress(
        threading.Event(), "r", "/p", "cam", 1, "continuous", "cpu", ctx
    )
    assert calls == ["direct"]


def test_pace_then_compress_dispatches_to_one_when_source_chained(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        fc.app,
        "compress_direct",
        lambda *a, **k: calls.append("direct") or True,
    )
    monkeypatch.setattr(
        fc.app,
        "compress_one",
        lambda *a, **k: calls.append("one") or True,
    )
    ctx = _fake_dispatch_ctx(source="chained", t2_enabled=True)
    fc._pace_then_compress(
        threading.Event(), "r", "/p", "cam", 1, "continuous", "cpu", ctx
    )
    assert calls == ["one"]


def test_pace_then_compress_dispatches_to_one_for_tier2_regardless_of_source(
    monkeypatch,
):
    """tier=2 always uses compress_one (chained re-encode of tier-1 file)."""
    calls: list[str] = []
    monkeypatch.setattr(
        fc.app,
        "compress_direct",
        lambda *a, **k: calls.append("direct") or True,
    )
    monkeypatch.setattr(
        fc.app,
        "compress_one",
        lambda *a, **k: calls.append("one") or True,
    )
    ctx = _fake_dispatch_ctx(source="direct", t2_enabled=True)
    fc._pace_then_compress(
        threading.Event(), "r", "/p", "cam", 2, "continuous", "cpu", ctx
    )
    assert calls == ["one"]


def test_pace_then_compress_dispatches_to_one_when_t2_type_disabled(monkeypatch):
    """tier=1, source=direct, but t2 disabled for THIS rtype → compress_one."""
    calls: list[str] = []
    monkeypatch.setattr(
        fc.app,
        "compress_direct",
        lambda *a, **k: calls.append("direct") or True,
    )
    monkeypatch.setattr(
        fc.app,
        "compress_one",
        lambda *a, **k: calls.append("one") or True,
    )
    ctx = _fake_dispatch_ctx(source="direct", t2_enabled=False)
    fc._pace_then_compress(
        threading.Event(), "r", "/p", "cam", 1, "continuous", "cpu", ctx
    )
    assert calls == ["one"]


# ═══════════════════════════════════════════════════════════════════════════════
# swap_t2 (rename sibling .t2 onto primary path at tier-2 min_days)
# ═══════════════════════════════════════════════════════════════════════════════


def _setup_swap_ready(
    tmp_path, *, with_sibling=True, with_primary=True, with_frigate_row=True
):
    """Set up a row in t2_status='direct' state, ready for swap.

    Optionally omit the sibling, primary, or frigate.recordings row to
    exercise the swap_t2 fallback / error paths.
    """
    src = tmp_path / "recordings" / "cam1" / "clip.mp4"
    src.parent.mkdir(parents=True, exist_ok=True)
    if with_primary:
        src.write_bytes(b"x" * 5000)  # tier-1 file
    sib = fc.sibling_path(src)
    if with_sibling:
        sib.write_bytes(b"y" * 2000)  # parked tier-2 file

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    if with_frigate_row:
        _insert_recording(
            frigate_conn, "r1", "cam1", str(src), time.time() - 100 * 86400, motion=5
        )
    frigate_conn.close()

    # Declare cam1 in YAML so the config has it even when the frigate.db
    # has no recording row for cam1 (the with_frigate_row=False case).
    cfg = _make_config(
        tmp_path,
        frigate_db=str(frigate_db),
        yaml_cameras={"cam1": {}},
    )
    compress_conn = _open_compress_db(tmp_path)
    fc._attach_frigate(compress_conn, cfg, "frigate")
    ctx = fc.CompressorContext(cfg=cfg, compress_db=compress_conn)
    fc._record(
        ctx.compress_db,
        recording_id="r1",
        camera="cam1",
        path=str(src),
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=10000,
        size_after=5000,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    fc._record(
        ctx.compress_db,
        recording_id="r1",
        camera="cam1",
        path=str(src),
        tier=2,
        recording_type="motion",
        encoder="cpu",
        size_before=None,
        size_after=2000,
        duration_sec=1.0,
        status=fc.STATUS_DIRECT,
    )
    return ctx, src, sib


def test_swap_t2_renames_sibling_onto_primary(tmp_path):
    ctx, src, sib = _setup_swap_ready(tmp_path)
    result = fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is True
    # Primary now has tier-2 content
    assert src.read_bytes() == b"y" * 2000
    # Sibling consumed
    assert not sib.exists()
    row = _db_row(ctx)
    assert row["t2_status"] == fc.STATUS_OK
    _close_ctx(ctx)


def test_swap_t2_updates_frigate_segment_size(tmp_path):
    ctx, src, _ = _setup_swap_ready(tmp_path)
    fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    seg_row = ctx.compress_db.execute(
        "SELECT segment_size FROM frigate.recordings WHERE id='r1'"
    ).fetchone()
    assert seg_row["segment_size"] == pytest.approx(2000 / (1024 * 1024), rel=1e-4)
    _close_ctx(ctx)


def test_swap_t2_falls_back_to_chained_when_sibling_missing(tmp_path):
    """If the sibling is gone (corruption, admin delete), swap_t2 falls
    back to compress_one(tier=2) which re-encodes from the primary."""
    ctx, src, _ = _setup_swap_ready(tmp_path, with_sibling=False)

    def fake_run(cmd, **kwargs):
        # Single-output (chained tier-2) writes one file
        Path(cmd[-1]).write_bytes(b"z" * 1500)
        m = MagicMock()
        m.returncode = 0
        m.stderr = ""
        return m

    # store_probe is needed because compress_one path requires probe data
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
            "file_size": 5000,
        },
    )
    ctx.compress_db.commit()

    with patch("subprocess.run", side_effect=fake_run):
        result = fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is True
    # Primary now holds the chained-encoded tier-2 content
    assert src.read_bytes() == b"z" * 1500
    row = _db_row(ctx)
    assert row["t2_status"] == fc.STATUS_OK
    _close_ctx(ctx)


def test_swap_t2_cleans_up_sibling_when_primary_missing(tmp_path):
    """Frigate retired the segment between direct-encode and swap; primary
    is gone but sibling remains.  Clean up the orphan and mark error."""
    ctx, src, sib = _setup_swap_ready(tmp_path, with_primary=False)
    result = fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is False
    assert not sib.exists()  # orphan deleted
    row = _db_row(ctx)
    assert row["t2_status"] == fc.STATUS_ERROR
    _close_ctx(ctx)


def test_swap_t2_aborts_when_recording_gone_from_frigate_db(tmp_path):
    """Frigate dropped the recordings row mid-flight: don't replace primary,
    delete orphan sibling, mark error."""
    ctx, src, sib = _setup_swap_ready(tmp_path, with_frigate_row=False)
    result = fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is False
    # Primary untouched (still tier-1 content)
    assert src.read_bytes() == b"x" * 5000
    # Sibling cleaned up
    assert not sib.exists()
    row = _db_row(ctx)
    assert row["t2_status"] == fc.STATUS_ERROR
    _close_ctx(ctx)


def test_swap_t2_skips_rename_when_dry_run(tmp_path):
    """Camera in dry-run → swap_t2 logs only; no rename, no DB write."""
    ctx, src, sib = _setup_swap_ready(tmp_path)
    ctx.cfg.cameras["cam1"].dry_run = True

    result = fc.swap_t2("r1", str(src), "cam1", "motion", "cpu", ctx)
    assert result is True
    # Primary still holds tier-1 content (rename was skipped).
    assert src.read_bytes() == b"x" * 5000
    # Sibling untouched.
    assert sib.exists()
    assert sib.read_bytes() == b"y" * 2000
    # DB row still in 'direct' state — no UPDATE happened.
    row = _db_row(ctx)
    assert row["t2_status"] == fc.STATUS_DIRECT
    _close_ctx(ctx)


# ═══════════════════════════════════════════════════════════════════════════════
# _pace_then_compress dispatch — chained tier-2 (swap rows go through
# run_swap_loop, not _pace_then_compress)
# ═══════════════════════════════════════════════════════════════════════════════


def test_pace_then_compress_dispatches_to_one_for_chained_tier2(monkeypatch):
    """tier=2 → compress_one (chained encode).  Direct-swap rows never
    reach this path; they're filtered out of get_eligible_recordings."""
    calls: list[str] = []
    monkeypatch.setattr(
        fc.app, "compress_direct", lambda *a, **k: calls.append("direct") or True
    )
    monkeypatch.setattr(
        fc.app, "compress_one", lambda *a, **k: calls.append("one") or True
    )
    fake_ctx = MagicMock()
    fake_ctx.rate_limiter.acquire = lambda _stopping: None
    fake_ctx.cfg.cameras = {"cam": MagicMock()}
    fc._pace_then_compress(
        threading.Event(),
        "r",
        "/p",
        "cam",
        2,
        "continuous",
        "cpu",
        fake_ctx,
    )
    assert calls == ["one"]
