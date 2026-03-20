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
