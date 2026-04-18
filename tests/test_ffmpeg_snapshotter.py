# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for pure/utility functions in ffmpeg_snapshotter.ffmpeg_snapshotter."""

from __future__ import annotations

import os
import time
from pathlib import Path


import ffmpeg_snapshotter as fs


# ---------------------------------------------------------------------------
# redact_url
# ---------------------------------------------------------------------------


def test_redact_url_with_credentials():
    url = "rtsp://admin:secret@192.168.1.100/stream"
    result = fs.redact_url(url)
    assert "secret" not in result
    assert "admin" not in result
    assert "***:***@" in result
    assert result.endswith("192.168.1.100/stream")


def test_redact_url_without_credentials():
    url = "rtsp://192.168.1.100/stream"
    result = fs.redact_url(url)
    assert result == url


def test_redact_url_empty():
    assert fs.redact_url("") == ""


def test_redact_url_only_scheme():
    url = "rtsp://"
    result = fs.redact_url(url)
    assert result == url


def test_redact_url_complex_password():
    url = "rtsp://user:p@$$w0rd@host/cam"
    result = fs.redact_url(url)
    assert "p@$$w0rd" not in result
    assert "***:***@" in result


# ---------------------------------------------------------------------------
# ensure_media_path
# ---------------------------------------------------------------------------


def test_ensure_media_path_already_absolute_under_media():
    p = fs.ensure_media_path("/media/cameras/front")
    assert str(p) == "/media/cameras/front"


def test_ensure_media_path_relative_gets_prefixed():
    p = fs.ensure_media_path("cameras/front")
    assert str(p).startswith("/media/")
    assert "cameras/front" in str(p)


def test_ensure_media_path_absolute_not_under_media():
    # ensure_media_path only prepends /media/ for relative paths or paths
    # that don't already start with /media/; /tmp/... is left as-is.
    p = fs.ensure_media_path("/tmp/snapshots")
    assert str(p) == "/tmp/snapshots"


def test_ensure_media_path_returns_path_object():
    p = fs.ensure_media_path("foo")
    assert isinstance(p, Path)


# ---------------------------------------------------------------------------
# apply_retention_count
# ---------------------------------------------------------------------------


def test_apply_retention_count_zero_does_nothing(tmp_path):
    for i in range(5):
        (tmp_path / f"snap_{i:03d}.jpg").write_bytes(b"x")
    fs.apply_retention_count(tmp_path, 0, "latest.jpg")
    assert len(list(tmp_path.glob("*.jpg"))) == 5


def test_apply_retention_count_keeps_newest(tmp_path):
    # Create 5 files with different modification times.
    files = []
    base_mtime = time.time() - 500
    for i in range(5):
        p = tmp_path / f"snap_{i:03d}.jpg"
        p.write_bytes(b"x")
        os.utime(p, (base_mtime + i * 10, base_mtime + i * 10))
        files.append(p)

    # Keep only the 3 newest.
    fs.apply_retention_count(tmp_path, 3, "latest.jpg")

    remaining = sorted(tmp_path.glob("*.jpg"))
    assert len(remaining) == 3
    # The three newest are snap_002, snap_003, snap_004.
    names = {p.name for p in remaining}
    assert names == {"snap_002.jpg", "snap_003.jpg", "snap_004.jpg"}


def test_apply_retention_count_skips_latest_name(tmp_path):
    base_mtime = time.time() - 500
    for i in range(4):
        p = tmp_path / f"snap_{i:03d}.jpg"
        p.write_bytes(b"x")
        os.utime(p, (base_mtime + i, base_mtime + i))

    latest = tmp_path / "latest.jpg"
    latest.write_bytes(b"x")
    os.utime(latest, (base_mtime - 1000, base_mtime - 1000))  # oldest mtime

    # retain_count=2 — latest.jpg must not be deleted regardless of mtime.
    fs.apply_retention_count(tmp_path, 2, "latest.jpg")

    assert latest.exists(), "latest.jpg must never be deleted by retention_count"
    remaining_snaps = [p for p in tmp_path.glob("*.jpg") if p.name != "latest.jpg"]
    assert len(remaining_snaps) == 2


def test_apply_retention_count_nonexistent_dir(tmp_path):
    missing = tmp_path / "no_such_dir"
    # Should return silently without raising.
    fs.apply_retention_count(missing, 5, "latest.jpg")


def test_apply_retention_count_exact_count_no_deletion(tmp_path):
    base_mtime = time.time() - 100
    for i in range(3):
        p = tmp_path / f"snap_{i}.jpg"
        p.write_bytes(b"x")
        os.utime(p, (base_mtime + i, base_mtime + i))

    fs.apply_retention_count(tmp_path, 3, "latest.jpg")
    assert len(list(tmp_path.glob("*.jpg"))) == 3


def test_apply_retention_count_ignores_non_jpg(tmp_path):
    base_mtime = time.time() - 100
    for i in range(5):
        p = tmp_path / f"snap_{i}.jpg"
        p.write_bytes(b"x")
        os.utime(p, (base_mtime + i, base_mtime + i))

    # A non-jpg file should never be counted or deleted.
    txt = tmp_path / "readme.txt"
    txt.write_text("hello")

    fs.apply_retention_count(tmp_path, 2, "latest.jpg")

    assert txt.exists(), "non-jpg files must not be deleted"
    assert len(list(tmp_path.glob("*.jpg"))) == 2


def test_apply_retention_count_count_one(tmp_path):
    base_mtime = time.time() - 100
    for i in range(3):
        p = tmp_path / f"snap_{i}.jpg"
        p.write_bytes(b"x")
        os.utime(p, (base_mtime + i, base_mtime + i))

    fs.apply_retention_count(tmp_path, 1, "latest.jpg")

    remaining = list(tmp_path.glob("*.jpg"))
    assert len(remaining) == 1
    assert remaining[0].name == "snap_2.jpg"


# ---------------------------------------------------------------------------
# set_latest_symlink
# ---------------------------------------------------------------------------


def test_set_latest_symlink_creates_symlink(tmp_path):
    target = tmp_path / "snap_001.jpg"
    target.write_bytes(b"img")
    latest = tmp_path / "latest.jpg"

    fs.set_latest_symlink(target, latest)

    assert latest.is_symlink()
    assert latest.resolve() == target.resolve()


def test_set_latest_symlink_replaces_existing(tmp_path):
    old_target = tmp_path / "snap_001.jpg"
    old_target.write_bytes(b"old")
    new_target = tmp_path / "snap_002.jpg"
    new_target.write_bytes(b"new")
    latest = tmp_path / "latest.jpg"

    fs.set_latest_symlink(old_target, latest)
    assert latest.resolve() == old_target.resolve()

    fs.set_latest_symlink(new_target, latest)
    assert latest.resolve() == new_target.resolve()


# ---------------------------------------------------------------------------
# apply_retention_days
# ---------------------------------------------------------------------------


def test_apply_retention_days_zero_does_nothing(tmp_path):
    (tmp_path / "snap.jpg").write_bytes(b"x")
    fs.apply_retention_days(tmp_path, 0, "latest.jpg")
    assert (tmp_path / "snap.jpg").exists()


def test_apply_retention_days_nonexistent_dir(tmp_path):
    missing = tmp_path / "no_such_dir"
    # Should return silently without raising.
    fs.apply_retention_days(missing, 7, "latest.jpg")


# ---------------------------------------------------------------------------
# StreamCfg dataclass
# ---------------------------------------------------------------------------


def test_streamcfg_construction():
    cfg = fs.StreamCfg(
        name="front",
        url="rtsp://host/stream",
        interval_seconds=30,
        output_dir=Path("/media/front"),
        filename_format="%Y%m%d-%H%M%S.jpg",
        date_dir_format="%Y/%m/%d",
        latest_name="latest.jpg",
        retain_count=100,
        retain_days=7,
        extra_input_args="-rtsp_transport tcp",
        extra_output_args="-q:v 2",
    )
    assert cfg.name == "front"
    assert cfg.url == "rtsp://host/stream"
    assert cfg.interval_seconds == 30
    assert cfg.output_dir == Path("/media/front")
    assert cfg.retain_count == 100
    assert cfg.retain_days == 7
    assert cfg.extra_input_args == "-rtsp_transport tcp"
    assert cfg.extra_output_args == "-q:v 2"


# ---------------------------------------------------------------------------
# Worker helpers
# ---------------------------------------------------------------------------


def _make_worker(tmp_path, **overrides):
    """Build a Worker with sensible defaults for testing."""
    cfg_kwargs = dict(
        name="test_cam",
        url="rtsp://user:pass@192.168.1.10/stream",
        interval_seconds=60,
        output_dir=tmp_path,
        filename_format="%Y%m%d-%H%M%S.jpg",
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
        "global_input_args": "-fflags nobuffer",
        "global_output_args": "",
    }
    return fs.Worker(cfg, ffmpeg_cfg, log_level="INFO")


def test_worker_is_vaapi_true(tmp_path):
    w = _make_worker(tmp_path)
    w.ffmpeg_cfg["global_hwaccel_args"] = (
        "-hwaccel vaapi -hwaccel_device /dev/dri/renderD128"
    )
    assert w._is_vaapi() is True


def test_worker_is_vaapi_false(tmp_path):
    w = _make_worker(tmp_path)
    w.ffmpeg_cfg["global_hwaccel_args"] = "-hwaccel cuda"
    assert w._is_vaapi() is False


def test_worker_is_vaapi_empty(tmp_path):
    w = _make_worker(tmp_path)
    assert w._is_vaapi() is False


def test_worker_build_ffmpeg_cmd_basic(tmp_path):
    w = _make_worker(tmp_path)
    out = tmp_path / "out.jpg"
    cmd = w._build_ffmpeg_cmd(out)

    assert cmd[0] == "ffmpeg"
    assert "-nostdin" in cmd
    assert "-i" in cmd
    assert w.cfg.url in cmd
    assert "-an" in cmd
    assert "-frames:v" in cmd
    assert "1" in cmd
    assert "-update" in cmd
    assert "-atomic_writing" in cmd
    assert str(out) in cmd


def test_worker_build_ffmpeg_cmd_vaapi_adds_hwdownload(tmp_path):
    w = _make_worker(tmp_path)
    w.ffmpeg_cfg["global_hwaccel_args"] = "-hwaccel vaapi"
    out = tmp_path / "out.jpg"
    cmd = w._build_ffmpeg_cmd(out)

    assert "-vf" in cmd
    vf_idx = cmd.index("-vf")
    assert cmd[vf_idx + 1] == "hwdownload,format=nv12"


def test_worker_build_ffmpeg_cmd_includes_global_input_args(tmp_path):
    w = _make_worker(tmp_path)
    w.ffmpeg_cfg["global_input_args"] = "-fflags nobuffer"
    out = tmp_path / "out.jpg"
    cmd = w._build_ffmpeg_cmd(out)

    assert "-fflags" in cmd
    assert "nobuffer" in cmd


def test_worker_final_snapshot_path(tmp_path):
    w = _make_worker(
        tmp_path, date_dir_format="%Y/%m/%d", filename_format="%Y%m%d-%H%M%S.jpg"
    )
    now_ts = 1700000000.0  # 2023-11-14 22:13:20 UTC
    path = w._final_snapshot_path(now_ts)

    # Path should be under output_dir / date_dir / filename
    assert str(path).startswith(str(tmp_path))
    assert path.suffix == ".jpg"
    # The date directory structure should exist
    assert path.parent.exists()


def test_worker_tmp_snapshot_path(tmp_path):
    w = _make_worker(tmp_path)
    path = w._tmp_snapshot_path()

    assert ".tmp" in str(path)
    assert path.parent == tmp_path / ".tmp"
    assert path.parent.exists()
    assert path.name.startswith("test_cam-")
    assert path.suffix == ".jpg"


# ---------------------------------------------------------------------------
# log()
# ---------------------------------------------------------------------------


def test_log_prints(capsys):
    fs.log("INFO", "hello world")
    captured = capsys.readouterr()
    assert "[INFO] hello world" in captured.out


# ---------------------------------------------------------------------------
# Worker.start / stop / poll
# ---------------------------------------------------------------------------


def test_worker_start_creates_output_dir(tmp_path):
    out = tmp_path / "new_dir"
    w = _make_worker(tmp_path, output_dir=out)
    # Monkey-patch _run to do nothing so the thread exits immediately.
    w._run = lambda: None
    w.start()
    w.stop()
    assert out.exists()


def test_worker_start_clears_tmp(tmp_path):
    tmp_dir = tmp_path / ".tmp"
    tmp_dir.mkdir()
    stale = tmp_dir / "leftover.jpg"
    stale.write_bytes(b"x")

    w = _make_worker(tmp_path)
    w._run = lambda: None
    w.start()
    w.stop()

    assert not stale.exists()


def test_worker_start_starts_thread(tmp_path):
    w = _make_worker(tmp_path)
    w._run = lambda: None
    w.start()
    # Thread should have been created (may already be finished).
    assert w.thread is not None
    w.stop()


def test_worker_stop_sets_event_and_joins(tmp_path):
    w = _make_worker(tmp_path)
    w._run = lambda: None
    w.start()
    w.stop()
    assert w.stop_event.is_set()
    assert w.thread is not None
    assert not w.thread.is_alive()


def test_worker_poll_no_thread(tmp_path):
    w = _make_worker(tmp_path)
    assert w.poll() is None


def test_worker_poll_alive_thread(tmp_path):
    import threading

    evt = threading.Event()
    w = _make_worker(tmp_path)
    w._run = lambda: evt.wait()
    w.start()
    try:
        assert w.poll() is None
    finally:
        evt.set()
        w.stop()


def test_worker_poll_dead_thread(tmp_path):
    w = _make_worker(tmp_path)
    w._run = lambda: None
    w.start()
    w.thread.join(timeout=2)
    assert w.poll() == 0


# ---------------------------------------------------------------------------
# apply_retention_days — subprocess.run path
# ---------------------------------------------------------------------------


def test_apply_retention_days_calls_find(tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    mock_run = MagicMock()
    monkeypatch.setattr("subprocess.run", mock_run)

    fs.apply_retention_days(tmp_path, 7, "latest.jpg")

    mock_run.assert_called_once()
    args = mock_run.call_args[0][0]
    assert args[0] == "find"
    assert str(tmp_path) in args
    assert "-mtime" in args
    assert "+7" in args
    assert "-delete" in args
    assert "latest.jpg" in args
