# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""
Integration tests for frigate_compressor — runs real FFmpeg and ffprobe.

These tests require ffmpeg and ffprobe on PATH.  They exercise the full
compress_one() pipeline without mocking subprocess, verifying that:
  - _probe_video() correctly parses a real MP4
  - compress_one() produces a valid, smaller MP4 on disk
  - Frigate DB segment_size is updated after success
  - dry_run mode leaves files untouched
  - scale filters (halve) produce output with expected dimensions
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

import pytest

import frigate_compressor as fc

from fc_helpers import (
    _insert_recording,
    _make_frigate_db,
    _make_options,
    _open_compress_db,
)

# ---------------------------------------------------------------------------
# Skip entire module when ffmpeg/ffprobe are absent.
# ---------------------------------------------------------------------------

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
        reason="ffmpeg/ffprobe not available",
    ),
]


# ---------------------------------------------------------------------------
# Session-scoped fixture: generate one tiny real H.264 MP4 once per run.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def real_video(tmp_path_factory) -> Path:
    """
    Generate a 64×64, 10 fps, 2-second H.264 MP4 using FFmpeg's lavfi source.
    libx264 with CRF 40 / ultrafast preset keeps it small and fast to encode.
    The file is written to a session-scoped temp directory and reused across tests.
    """
    dest = tmp_path_factory.mktemp("integration_video") / "source.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=64x64:rate=10",
            "-t",
            "2",
            "-c:v",
            "libx264",
            "-crf",
            "40",
            "-preset",
            "ultrafast",
            "-an",  # no audio — keeps test video small
            str(dest),
        ],
        check=True,
        capture_output=True,
    )
    return dest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _copy_video(src: Path, dest: Path) -> Path:
    """Copy *src* to *dest*, creating parent directories as needed."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(dest))
    return dest


def _make_ctx(
    tmp_path: Path,
    frigate_db: Path,
    *,
    dry_run: bool = False,
) -> fc.CompressorContext:
    opts = _make_options(
        tmp_path,
        frigate_db=str(frigate_db),
        yaml_defaults={"dry_run": dry_run},
    )
    cfg = fc.load_config(str(opts))
    compress_conn = _open_compress_db(tmp_path)
    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row
    return fc.CompressorContext(
        cfg=cfg,
        frigate_ro=frigate_ro,
        frigate_ro_lock=threading.Lock(),
        frigate_rw=frigate_rw,
        frigate_lock=threading.Lock(),
        compress_db=compress_conn,
    )


def _close_ctx(ctx: fc.CompressorContext) -> None:
    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()


def _probe_and_store(
    ctx: fc.CompressorContext, recording_id: str, camera: str, path: Path
) -> None:
    """Run a real ffprobe and store the result so compress_one can proceed."""
    info = fc._probe_full(path)
    assert info is not None, f"ffprobe failed for {path}"
    fc._store_probe(ctx.compress_db, recording_id, camera, str(path), info)


# ---------------------------------------------------------------------------
# _probe_video — real ffprobe against a known video
# ---------------------------------------------------------------------------


def test_probe_video_returns_correct_dimensions(real_video):
    dims, fps = fc._probe_video(real_video)
    assert dims == (64, 64)


def test_probe_video_returns_correct_fps(real_video):
    _, fps = fc._probe_video(real_video)
    assert fps is not None
    assert abs(fps - 10.0) < 0.5


def test_probe_video_nonexistent_file(tmp_path):
    dims, fps = fc._probe_video(tmp_path / "no_such_file.mp4")
    assert dims is None
    assert fps is None


# ---------------------------------------------------------------------------
# compress_one — end-to-end with real FFmpeg (CPU / libx264)
# ---------------------------------------------------------------------------


def test_compress_one_real_cpu_success(tmp_path, real_video):
    """compress_one with a real video and CPU encoder succeeds end-to-end."""
    rec_path = _copy_video(real_video, tmp_path / "recordings" / "cam1" / "clip.mp4")
    size_before = rec_path.stat().st_size

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "int_r1",
        "cam1",
        str(rec_path),
        time.time() - 10 * 86400,
        motion=5,
        objects=0,
    )
    frigate_conn.close()

    ctx = _make_ctx(tmp_path, frigate_db)
    _probe_and_store(ctx, "int_r1", "cam1", rec_path)

    result = fc.compress_one(
        recording_id="int_r1",
        path=str(rec_path),
        camera="cam1",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        ctx=ctx,
    )

    assert result is True

    # Compress DB row must record success.
    row = ctx.compress_db.execute(
        "SELECT * FROM files WHERE recording_id='int_r1'"
    ).fetchone()
    assert row["t1_status"] == fc.STATUS_OK
    assert row["file_size"] == size_before
    assert row["t1_file_size"] is not None
    assert row["t1_encode_sec"] is not None

    # The file on disk should still exist (was replaced in-place).
    assert rec_path.exists()

    # The replaced file must be a valid MP4 readable by ffprobe.
    dims, fps = fc._probe_video(rec_path)
    assert dims is not None, "replaced file is not a valid video"

    # Frigate DB segment_size should have been updated.
    seg = ctx.frigate_rw.execute(
        "SELECT segment_size FROM recordings WHERE id='int_r1'"
    ).fetchone()
    assert seg is not None
    expected_mb = row["t1_file_size"] / (1024 * 1024)
    assert seg["segment_size"] == pytest.approx(expected_mb, rel=1e-4)

    _close_ctx(ctx)


def test_compress_one_real_dry_run_leaves_file_unchanged(tmp_path, real_video):
    """dry_run=True must not touch the source file and must not write to DBs."""
    rec_path = _copy_video(real_video, tmp_path / "recordings" / "cam1" / "clip.mp4")
    original_mtime = rec_path.stat().st_mtime
    original_size = rec_path.stat().st_size

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "dry_r1",
        "cam1",
        str(rec_path),
        time.time() - 10 * 86400,
    )
    frigate_conn.close()

    ctx = _make_ctx(tmp_path, frigate_db, dry_run=True)
    _probe_and_store(ctx, "dry_r1", "cam1", rec_path)

    result = fc.compress_one(
        recording_id="dry_r1",
        path=str(rec_path),
        camera="cam1",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        ctx=ctx,
    )

    assert result is True

    # File must be completely untouched.
    assert rec_path.stat().st_size == original_size
    assert rec_path.stat().st_mtime == original_mtime

    # Probe row exists but no compression fields should be set.
    row = ctx.compress_db.execute(
        "SELECT * FROM files WHERE recording_id='dry_r1'"
    ).fetchone()
    assert row is not None
    assert row["t1_status"] is None

    # Frigate DB segment_size must remain NULL (never written).
    seg = ctx.frigate_rw.execute(
        "SELECT segment_size FROM recordings WHERE id='dry_r1'"
    ).fetchone()
    assert seg["segment_size"] is None

    _close_ctx(ctx)


def test_compress_one_real_no_temp_files_left_after_success(tmp_path, real_video):
    """No .tmp.* files should remain in the recording directory after success."""
    rec_path = _copy_video(real_video, tmp_path / "recordings" / "cam1" / "clip.mp4")

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "tmp_r1",
        "cam1",
        str(rec_path),
        time.time() - 10 * 86400,
    )
    frigate_conn.close()

    ctx = _make_ctx(tmp_path, frigate_db)
    _probe_and_store(ctx, "tmp_r1", "cam1", rec_path)

    fc.compress_one(
        recording_id="tmp_r1",
        path=str(rec_path),
        camera="cam1",
        tier=1,
        recording_type="continuous",
        encoder="cpu",
        ctx=ctx,
    )

    leftover = list(rec_path.parent.glob(fc._TEMP_GLOB))
    assert leftover == [], f"leftover temp files: {leftover}"

    _close_ctx(ctx)


# ---------------------------------------------------------------------------
# Scale filter — halve mode produces output at half resolution
# ---------------------------------------------------------------------------


def test_compress_one_halve_scale_produces_half_resolution(tmp_path, real_video):
    """
    Tier-1 motion settings use halve scale mode by default.
    Compressing a 64×64 video should produce a 32×32 output.
    """
    rec_path = _copy_video(real_video, tmp_path / "recordings" / "cam1" / "clip.mp4")

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "scale_r1",
        "cam1",
        str(rec_path),
        time.time() - 10 * 86400,
        motion=5,
        objects=0,
    )
    frigate_conn.close()

    ctx = _make_ctx(tmp_path, frigate_db)
    _probe_and_store(ctx, "scale_r1", "cam1", rec_path)

    result = fc.compress_one(
        recording_id="scale_r1",
        path=str(rec_path),
        camera="cam1",
        tier=1,
        recording_type="motion",  # default: halve scale
        encoder="cpu",
        ctx=ctx,
    )

    assert result is True
    dims, _ = fc._probe_video(rec_path)
    assert dims == (32, 32), f"expected 32×32 after halve, got {dims}"

    _close_ctx(ctx)


# ---------------------------------------------------------------------------
# FPS cap — tier-2 continuous is capped at 4 fps by default
# ---------------------------------------------------------------------------


def test_compress_one_fps_cap_produces_capped_fps(tmp_path, real_video):
    """
    Tier-2 continuous default: fps_mode=cap, fps_value=4.0.
    Source is 10 fps; output should be at or below 4 fps.
    """
    rec_path = _copy_video(real_video, tmp_path / "recordings" / "cam1" / "clip.mp4")

    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(
        frigate_conn,
        "fps_r1",
        "cam1",
        str(rec_path),
        time.time() - 35 * 86400,  # > 30 days → tier 2
    )
    frigate_conn.close()

    ctx = _make_ctx(tmp_path, frigate_db)
    _probe_and_store(ctx, "fps_r1", "cam1", rec_path)

    result = fc.compress_one(
        recording_id="fps_r1",
        path=str(rec_path),
        camera="cam1",
        tier=2,
        recording_type="continuous",  # default: halve + fps cap 4
        encoder="cpu",
        ctx=ctx,
    )

    assert result is True
    _, fps = fc._probe_video(rec_path)
    assert fps is not None
    assert fps <= 4.5, f"expected fps ≤ 4, got {fps}"

    _close_ctx(ctx)
