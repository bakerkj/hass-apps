# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the ffprobe profiling pass."""

from __future__ import annotations

import sqlite3
import time
from unittest.mock import MagicMock, patch

import pytest

import frigate_compressor as fc

from fc_helpers import (
    _insert_recording,
    _make_config,
    _make_frigate_db,
    _open_compress_db,
)


# ═══════════════════════════════════════════════════════════════════════════════
# _probe
# ═══════════════════════════════════════════════════════════════════════════════


def _fake_probe_result(stdout: str, returncode: int = 0) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = ""
    return m


def test_probe_valid(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"fake")
    stdout = (
        "codec_name=h264\n"
        "width=1920\n"
        "height=1080\n"
        "r_frame_rate=30000/1001\n"
        "bit_rate=5000000\n"
        "duration=10.5\n"
        "size=6562500\n"
    )
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert info is not None
    assert info["codec"] == "h264"
    assert info["width"] == 1920
    assert info["height"] == 1080
    assert info["fps"] == pytest.approx(30000 / 1001)
    assert info["bitrate"] == 5000000
    assert info["duration_sec"] == pytest.approx(10.5)
    assert info["file_size"] == 6562500


def test_probe_missing_fields(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"fake")
    stdout = "codec_name=h264\nwidth=1920\nheight=1080\n"
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert info is not None
    assert info["codec"] == "h264"
    assert info["fps"] is None
    assert info["bitrate"] is None
    assert info["duration_sec"] is None


def test_probe_nonzero_returncode(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"fake")
    with patch("subprocess.run", return_value=_fake_probe_result("", returncode=1)):
        info = fc._probe(f)
    assert info is None


def test_probe_empty_output(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"fake")
    with patch("subprocess.run", return_value=_fake_probe_result("")):
        info = fc._probe(f)
    assert info is None


def test_probe_file_size_fallback(tmp_path):
    f = tmp_path / "clip.mp4"
    f.write_bytes(b"x" * 1234)
    stdout = "codec_name=h264\nwidth=640\nheight=480\n"
    with patch("subprocess.run", return_value=_fake_probe_result(stdout)):
        info = fc._probe(f)
    assert info is not None
    assert info["file_size"] == 1234


# ═══════════════════════════════════════════════════════════════════════════════
# _get_unprobed_recordings
# ═══════════════════════════════════════════════════════════════════════════════


class _ProbeTestCtx:
    """Lightweight test context holding cfg + compress_db for probe tests."""

    def __init__(self, cfg, compress_db, frigate_ro, frigate_rw):
        self.cfg = cfg
        self.compress_db = compress_db
        self.frigate_ro = frigate_ro
        self.frigate_rw = frigate_rw


def _make_probe_ctx(tmp_path, frigate_db, compress_conn=None):
    if compress_conn is None:
        compress_conn = _open_compress_db(tmp_path)
    cfg = _make_config(tmp_path, frigate_db=str(frigate_db))
    # Mirror the daemon: attach Frigate to compress_db read-only as
    # ``frigate``.  The probe-loop helpers require this attach.
    fc._attach_frigate_ro(compress_conn, cfg, "frigate")
    frigate_ro = sqlite3.connect(str(frigate_db))
    frigate_ro.row_factory = sqlite3.Row
    frigate_rw = sqlite3.connect(str(frigate_db))
    frigate_rw.row_factory = sqlite3.Row
    return _ProbeTestCtx(cfg, compress_conn, frigate_ro, frigate_rw)


def test_get_unprobed_returns_all_when_none_probed(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(frigate_conn, "r1", "cam1", "/path/a.mp4", time.time())
    _insert_recording(frigate_conn, "r2", "cam1", "/path/b.mp4", time.time())

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    unprobed = fc._get_unprobed_recordings(ctx.compress_db)
    assert len(unprobed) == 2
    ids = {r["recording_id"] for r in unprobed}
    assert ids == {"r1", "r2"}

    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()
    frigate_conn.close()


def test_get_unprobed_skips_already_probed(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(frigate_conn, "r1", "cam1", "/path/a.mp4", time.time())
    _insert_recording(frigate_conn, "r2", "cam1", "/path/b.mp4", time.time())

    ctx = _make_probe_ctx(tmp_path, frigate_db)

    # Mark r1 as already probed
    ctx.compress_db.execute(
        "INSERT INTO files (recording_id, camera, path, scanned_at)"
        " VALUES (?, ?, ?, ?)",
        ("r1", "cam1", "/path/a.mp4", "2026-01-01T00:00:00"),
    )
    ctx.compress_db.commit()

    unprobed = fc._get_unprobed_recordings(ctx.compress_db)
    assert len(unprobed) == 1
    assert unprobed[0]["recording_id"] == "r2"

    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()
    frigate_conn.close()


def test_get_unprobed_returns_empty_when_all_probed(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(frigate_conn, "r1", "cam1", "/path/a.mp4", time.time())

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    ctx.compress_db.execute(
        "INSERT INTO files (recording_id, camera, path, scanned_at)"
        " VALUES (?, ?, ?, ?)",
        ("r1", "cam1", "/path/a.mp4", "2026-01-01T00:00:00"),
    )
    ctx.compress_db.commit()

    unprobed = fc._get_unprobed_recordings(ctx.compress_db)
    assert unprobed == []

    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()
    frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _get_unprobed_recordings — cursor / incremental semantics
# ═══════════════════════════════════════════════════════════════════════════════


def test_get_unprobed_full_scan_returns_start_time(tmp_path):
    """Full-scan mode (cursor=None) includes start_time so the loop can
    advance its cursor from the probed batch."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(frigate_conn, "r1", "cam1", "/path/a.mp4", 1000.0)
    _insert_recording(frigate_conn, "r2", "cam1", "/path/b.mp4", 2000.0)

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    try:
        unprobed = fc._get_unprobed_recordings(ctx.compress_db, None)
        by_id = {r["recording_id"]: r for r in unprobed}
        assert by_id["r1"]["start_time"] == 1000.0
        assert by_id["r2"]["start_time"] == 2000.0
    finally:
        ctx.compress_db.close()
        ctx.frigate_ro.close()
        ctx.frigate_rw.close()
        frigate_conn.close()


def test_get_unprobed_incremental_cursor_filters_by_start_time(tmp_path):
    """With a cursor set, only recordings inside the safety window are returned."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    # Three unprobed recordings spread across time:
    _insert_recording(frigate_conn, "old", "cam1", "/path/old.mp4", 1000.0)
    _insert_recording(frigate_conn, "mid", "cam1", "/path/mid.mp4", 5000.0)
    _insert_recording(frigate_conn, "new", "cam1", "/path/new.mp4", 6000.0)

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    try:
        from frigate_compressor.probe_loop import _PROBE_SAFETY_WINDOW_SEC

        # cursor=6000, safety window=15min=900s → floor=5100 → keeps only "new"
        cursor_t = 6000.0
        assert cursor_t - _PROBE_SAFETY_WINDOW_SEC == pytest.approx(6000.0 - 900.0)
        unprobed = fc._get_unprobed_recordings(ctx.compress_db, cursor_t)
        ids = {r["recording_id"] for r in unprobed}
        assert ids == {"new"}

        # Looser cursor pulls "mid" in as well (5000 >= 5000-900=4100).
        unprobed = fc._get_unprobed_recordings(ctx.compress_db, 5000.0)
        ids = {r["recording_id"] for r in unprobed}
        assert ids == {"mid", "new"}
    finally:
        ctx.compress_db.close()
        ctx.frigate_ro.close()
        ctx.frigate_rw.close()
        frigate_conn.close()


def test_max_recording_start_time(tmp_path):
    """``_max_recording_start_time`` returns the newest start_time across
    the recordings table (used to seed the cursor after an empty full scan)."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)
    _insert_recording(frigate_conn, "r1", "cam1", "/path/a.mp4", 1000.0)
    _insert_recording(frigate_conn, "r2", "cam1", "/path/b.mp4", 3000.0)
    _insert_recording(frigate_conn, "r3", "cam1", "/path/c.mp4", 2000.0)

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    try:
        from frigate_compressor.probe_loop import _max_recording_start_time

        assert _max_recording_start_time(ctx.compress_db) == 3000.0
    finally:
        ctx.compress_db.close()
        ctx.frigate_ro.close()
        ctx.frigate_rw.close()
        frigate_conn.close()


def test_max_recording_start_time_empty(tmp_path):
    """Returns None when the recordings table has no rows."""
    frigate_db = tmp_path / "frigate.db"
    frigate_conn = _make_frigate_db(frigate_db)

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    try:
        from frigate_compressor.probe_loop import _max_recording_start_time

        assert _max_recording_start_time(ctx.compress_db) is None
    finally:
        ctx.compress_db.close()
        ctx.frigate_ro.close()
        ctx.frigate_rw.close()
        frigate_conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _store_probe
# ═══════════════════════════════════════════════════════════════════════════════


def test_store_probe_writes_row(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    _make_frigate_db(frigate_db).close()

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    info = {
        "codec": "h264",
        "width": 1920,
        "height": 1080,
        "fps": 20.0,
        "bitrate": 5000000,
        "duration_sec": 10.0,
        "file_size": 6250000,
    }
    fc._store_probe(ctx.compress_db, "r1", "cam1", "/path/a.mp4", info)

    row = ctx.compress_db.execute(
        "SELECT * FROM files WHERE recording_id = 'r1'"
    ).fetchone()
    assert row is not None
    assert row["codec"] == "h264"
    assert row["width"] == 1920
    assert row["height"] == 1080
    assert row["fps"] == pytest.approx(20.0)
    assert row["bitrate"] == 5000000
    assert row["file_size"] == 6250000
    assert row["scanned_at"] is not None

    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()


def test_store_probe_replaces_on_conflict(tmp_path):
    frigate_db = tmp_path / "frigate.db"
    _make_frigate_db(frigate_db).close()

    ctx = _make_probe_ctx(tmp_path, frigate_db)
    info1 = {
        "codec": "h264",
        "width": 1920,
        "height": 1080,
        "fps": 20.0,
        "bitrate": 5000000,
        "duration_sec": 10.0,
        "file_size": 100,
    }
    info2 = {
        "codec": "h264",
        "width": 1920,
        "height": 1080,
        "fps": 20.0,
        "bitrate": 5000000,
        "duration_sec": 10.0,
        "file_size": 200,
    }

    fc._store_probe(ctx.compress_db, "r1", "cam1", "/path/a.mp4", info1)
    fc._store_probe(ctx.compress_db, "r1", "cam1", "/path/a.mp4", info2)

    rows = ctx.compress_db.execute(
        "SELECT * FROM files WHERE recording_id = 'r1'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["file_size"] == 200

    ctx.compress_db.close()
    ctx.frigate_ro.close()
    ctx.frigate_rw.close()
