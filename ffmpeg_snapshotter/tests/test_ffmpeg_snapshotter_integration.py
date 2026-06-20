# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""
Integration tests for ffmpeg_snapshotter — runs real FFmpeg.

Exercises the Worker snapshot pipeline end-to-end against a local MP4
generated via FFmpeg's lavfi testsrc.  The ``cfg.url`` is a file path;
FFmpeg treats it the same as an RTSP URL for input purposes, so the
production code path (``_build_ffmpeg_cmd`` + ``subprocess.Popen`` +
atomic rename + symlink) is exercised without mocking.

These tests pin behavior that the Phase-2 refactor must preserve.
"""

import shutil
import struct
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

import ffmpeg_snapshotter as fs

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        shutil.which("ffmpeg") is None,
        reason="ffmpeg not available",
    ),
]


# ---------------------------------------------------------------------------
# Session-scoped fixture: a tiny real MP4 the Worker will "snapshot" from.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def source_mp4(tmp_path_factory) -> Path:
    """Generate a 64×64, 5 fps, 1-second H.264 MP4 via ffmpeg lavfi."""
    dest = tmp_path_factory.mktemp("snap_integration") / "source.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=64x64:rate=5",
            "-t",
            "1",
            "-c:v",
            "libx264",
            "-crf",
            "40",
            "-preset",
            "ultrafast",
            "-an",
            str(dest),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert dest.exists() and dest.stat().st_size > 0
    return dest


def _make_worker(tmp_path: Path, url: str, **overrides) -> fs.Worker:
    """Build a Worker pointed at a given URL (file path works for FFmpeg)."""
    cfg_kwargs: dict[str, Any] = dict(
        name="integration_cam",
        url=url,
        interval_seconds=1,
        output_dir=tmp_path,
        filename_format="%Y%m%d-%H%M%S-%f.jpg",
        date_dir_format="%Y/%m/%d",
        latest_name="latest.jpg",
        retain_count=0,
        retain_days=0,
        extra_input_args="",
        extra_output_args="",
    )
    cfg_kwargs.update(overrides)
    cfg = fs.StreamCfg(**cfg_kwargs)
    ffmpeg_cfg = {
        "global_hwaccel_args": "",
        "global_input_args": "",
        "global_output_args": "",
    }
    return fs.Worker(cfg, ffmpeg_cfg, log_level="INFO")


def _is_jpeg(path: Path) -> bool:
    """JPEG files start with the SOI marker 0xFF 0xD8."""
    with open(path, "rb") as f:
        head = f.read(2)
    return head == b"\xff\xd8"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_single_snapshot_produces_valid_jpeg(tmp_path, source_mp4):
    """End-to-end: one snapshot writes a valid JPEG and updates latest.jpg."""
    w = _make_worker(tmp_path, url=str(source_mp4))
    rc = w._run_one_snapshot()
    assert rc == 0

    jpgs = [
        p
        for p in tmp_path.rglob("*.jpg")
        if ".tmp" not in p.parts and not p.is_symlink()
    ]
    assert len(jpgs) == 1, f"expected 1 JPEG, got {jpgs}"
    assert jpgs[0].stat().st_size > 0
    assert _is_jpeg(jpgs[0])

    latest = tmp_path / "latest.jpg"
    assert latest.is_symlink()
    assert latest.resolve() == jpgs[0].resolve()

    # Tmp dir should be cleaned of any stragglers from this run.
    assert list((tmp_path / ".tmp").glob("*.jpg")) == []


def test_snapshot_reports_jpeg_dimensions(tmp_path, source_mp4):
    """The produced JPEG should match the source dimensions (64×64)."""
    w = _make_worker(tmp_path, url=str(source_mp4))
    rc = w._run_one_snapshot()
    assert rc == 0
    jpg = next(
        p
        for p in tmp_path.rglob("*.jpg")
        if ".tmp" not in p.parts and not p.is_symlink()
    )
    # Scan for the SOF0 marker (0xFFC0) and read height/width from it.
    data = jpg.read_bytes()
    idx = data.find(b"\xff\xc0")
    assert idx != -1, "SOF0 marker not found"
    # SOF0 layout: FF C0 <len:2> <precision:1> <height:2> <width:2>
    height, width = struct.unpack(">HH", data[idx + 5 : idx + 9])
    assert (width, height) == (64, 64)


def test_snapshot_with_nonexistent_url_fails_cleanly(tmp_path):
    """Bad input URL → non-zero rc, no final JPEG, no symlink, tmp cleaned."""
    missing = tmp_path / "no_such_source.mp4"
    w = _make_worker(tmp_path, url=str(missing))
    rc = w._run_one_snapshot()
    assert rc != 0
    jpgs = [p for p in tmp_path.rglob("*.jpg") if not p.is_symlink()]
    assert jpgs == []
    assert not (tmp_path / "latest.jpg").exists()
    assert list((tmp_path / ".tmp").glob("*.jpg")) == []


def test_worker_loop_produces_multiple_snapshots(tmp_path, source_mp4):
    """Running the Worker loop for ~3s with a 1s interval yields ≥2 files."""
    w = _make_worker(tmp_path, url=str(source_mp4), interval_seconds=1)
    w.start()
    try:
        # Poll for files rather than sleeping a fixed amount — fail fast if
        # something is wrong, but don't oversleep when it works.
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            jpgs = [
                p
                for p in tmp_path.rglob("*.jpg")
                if ".tmp" not in p.parts and not p.is_symlink()
            ]
            if len(jpgs) >= 2:
                break
            time.sleep(0.2)
    finally:
        w.stop()

    jpgs = [
        p
        for p in tmp_path.rglob("*.jpg")
        if ".tmp" not in p.parts and not p.is_symlink()
    ]
    assert len(jpgs) >= 2, f"expected ≥2 JPEGs, got {len(jpgs)}"
    for p in jpgs:
        assert _is_jpeg(p)
    # Latest symlink resolves to one of the produced JPEGs.
    latest = tmp_path / "latest.jpg"
    assert latest.is_symlink()
    assert latest.resolve() in {p.resolve() for p in jpgs}


def test_worker_loop_backoff_kicks_in_on_repeated_failure(tmp_path):
    """A broken URL causes the loop to back off rather than hot-spin."""
    w = _make_worker(
        tmp_path,
        url=str(tmp_path / "missing.mp4"),
        interval_seconds=1,
    )
    w.start()
    try:
        time.sleep(2.5)
    finally:
        w.stop()
    # After repeated failures the backoff should have grown beyond the
    # initial 1.0 (it doubles per failure, capped at 60).
    assert w.backoff > 1.0
