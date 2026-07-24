# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for compress DB schema, the _record upsert, and the _fmt utility."""

import sqlite3

import pytest
from fc_helpers import _open_compress_db

import frigate_compressor as fc

# ═══════════════════════════════════════════════════════════════════════════════
# DB schema
# ═══════════════════════════════════════════════════════════════════════════════


def test_open_compress_db_creates_table(tmp_path):
    conn = _open_compress_db(tmp_path)
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "files" in tables
    conn.close()


def test_open_compress_db_creates_views(tmp_path):
    conn = _open_compress_db(tmp_path)
    views = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
    }
    assert "savings_by_camera" in views
    assert "recent_errors" in views
    conn.close()


def test_open_compress_db_idempotent(tmp_path):
    conn1 = _open_compress_db(tmp_path)
    conn1.close()
    conn2 = _open_compress_db(tmp_path)
    conn2.close()


def test_schema_has_tier_columns(tmp_path):
    conn = _open_compress_db(tmp_path)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(files)").fetchall()}
    assert "recording_type" in cols
    assert "t1_status" in cols
    assert "t2_status" in cols
    assert "codec" in cols
    assert "scanned_at" in cols
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# check_frigate_schema
# ═══════════════════════════════════════════════════════════════════════════════


def _conn_with_schema(tmp_path, ddl: str) -> sqlite3.Connection:
    """Open an in-memory-ish SQLite connection with the given DDL applied."""
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(ddl)
    return conn


def test_check_frigate_schema_passes_with_all_columns(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER, segment_size REAL"
        ");",
    )
    fc.check_frigate_schema(conn)  # must not raise
    conn.close()


def test_check_frigate_schema_passes_with_extra_columns(tmp_path):
    """Extra columns Frigate may add in the future must not cause a failure."""
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER, segment_size REAL,"
        "  some_new_column TEXT"
        ");",
    )
    fc.check_frigate_schema(conn)  # must not raise
    conn.close()


def test_check_frigate_schema_raises_when_table_absent(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    conn.row_factory = sqlite3.Row
    with pytest.raises(RuntimeError, match="does not contain a 'recordings' table"):
        fc.check_frigate_schema(conn)
    conn.close()


def test_check_frigate_schema_raises_on_missing_column(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        # segment_size deliberately omitted
        "CREATE TABLE recordings ("
        "  id TEXT, camera TEXT, path TEXT, start_time REAL,"
        "  motion INTEGER, objects INTEGER"
        ");",
    )
    with pytest.raises(RuntimeError, match="segment_size"):
        fc.check_frigate_schema(conn)
    conn.close()


def test_check_frigate_schema_raises_on_multiple_missing_columns(tmp_path):
    conn = _conn_with_schema(
        tmp_path,
        "CREATE TABLE recordings (id TEXT);",
    )
    with pytest.raises(RuntimeError, match="schema drift"):
        fc.check_frigate_schema(conn)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _record
# ═══════════════════════════════════════════════════════════════════════════════


def test_record_inserts_tier1_row(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/front/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.5,
        status=fc.STATUS_OK,
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchone()
    assert row is not None
    assert row["t1_status"] == fc.STATUS_OK
    assert row["recording_type"] == "motion"
    assert row["file_size"] == 1000
    assert row["t1_file_size"] == 500
    assert row["t2_status"] is None
    conn.close()


def test_record_inserts_tier2_row(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/front/a.mp4",
        tier=2,
        recording_type="continuous",
        encoder="qsv",
        size_before=500,
        size_after=200,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchone()
    assert row is not None
    assert row["t2_status"] == fc.STATUS_OK
    assert row["t2_file_size"] == 200
    assert row["t1_status"] is None
    conn.close()


def test_record_tier1_then_tier2_preserves_both(tmp_path):
    conn = _open_compress_db(tmp_path)
    # Tier 1
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=500,
        duration_sec=1.5,
        status=fc.STATUS_OK,
    )
    # Tier 2
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=2,
        recording_type="motion",
        encoder="cpu",
        size_before=500,
        size_after=200,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    rows = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["file_size"] == 1000  # original size preserved
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t1_file_size"] == 500
    assert row["t2_status"] == fc.STATUS_OK
    assert row["t2_file_size"] == 200
    conn.close()


def test_record_upserts_same_tier_on_retry(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=None,
        duration_sec=None,
        status=fc.STATUS_ERROR,
        error_msg="timeout",
    )
    fc._record(
        conn,
        recording_id="abc123",
        camera="front",
        path="/media/a.mp4",
        tier=1,
        recording_type="motion",
        encoder="cpu",
        size_before=1000,
        size_after=400,
        duration_sec=2.0,
        status=fc.STATUS_OK,
    )
    rows = conn.execute("SELECT * FROM files WHERE recording_id='abc123'").fetchall()
    assert len(rows) == 1
    assert rows[0]["t1_status"] == fc.STATUS_OK
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _record_failure (retry counter + backoff + give_up cap)
# ═══════════════════════════════════════════════════════════════════════════════


def test_record_failure_first_attempt_sets_error_and_attempts_one(tmp_path):
    conn = _open_compress_db(tmp_path)
    new_n, status = fc._record_failure(
        conn,
        recording_id="r1",
        camera="cam",
        path="/m/r1.mp4",
        tier=1,
        recording_type="continuous",
        encoder="qsv",
        error_msg="boom",
    )
    assert (new_n, status) == (1, fc.STATUS_ERROR)
    row = conn.execute("SELECT * FROM files WHERE recording_id='r1'").fetchone()
    assert row["t1_status"] == fc.STATUS_ERROR
    assert row["t1_attempts"] == 1
    assert row["t1_next_retry_at"] is not None
    conn.close()


def test_record_failure_increments_attempts_on_repeat(tmp_path):
    conn = _open_compress_db(tmp_path)
    for _ in range(3):
        fc._record_failure(
            conn,
            recording_id="r1",
            camera="cam",
            path="/m/r1.mp4",
            tier=1,
            recording_type="continuous",
            encoder="qsv",
        )
    row = conn.execute(
        "SELECT t1_attempts FROM files WHERE recording_id='r1'"
    ).fetchone()
    assert row["t1_attempts"] == 3
    conn.close()


def test_record_failure_flips_to_give_up_at_cap(tmp_path):
    conn = _open_compress_db(tmp_path)
    last = None
    for _ in range(fc._MAX_ATTEMPTS):
        last = fc._record_failure(
            conn,
            recording_id="r1",
            camera="cam",
            path="/m/r1.mp4",
            tier=1,
            recording_type="continuous",
            encoder="qsv",
        )
    assert last == (fc._MAX_ATTEMPTS, fc.STATUS_GIVE_UP)
    row = conn.execute("SELECT * FROM files WHERE recording_id='r1'").fetchone()
    assert row["t1_status"] == fc.STATUS_GIVE_UP
    # give_up rows have no next_retry_at — they're terminally excluded.
    assert row["t1_next_retry_at"] is None
    conn.close()


def test_record_failure_backoff_doubles_per_attempt(tmp_path):
    """Backoff = base * 2^(attempts-1) capped at max.  Verify by parsing the
    next_retry_at delta against the row's t1_compressed_at."""
    import datetime as _dt

    conn = _open_compress_db(tmp_path)
    for expected_attempts in range(1, 7):  # delays 60, 120, 240, 480, 960, 1920
        fc._record_failure(
            conn,
            recording_id="r1",
            camera="cam",
            path="/m/r1.mp4",
            tier=1,
            recording_type="continuous",
            encoder="qsv",
        )
        row = conn.execute(
            "SELECT t1_compressed_at, t1_next_retry_at, t1_attempts"
            " FROM files WHERE recording_id='r1'"
        ).fetchone()
        compressed = _dt.datetime.fromisoformat(row["t1_compressed_at"])
        next_retry = _dt.datetime.fromisoformat(row["t1_next_retry_at"])
        delta = (next_retry - compressed).total_seconds()
        expected_delay = fc._BACKOFF_BASE_SEC * (2 ** (expected_attempts - 1))
        # ±1s tolerance for the seconds-resolution timestamp rounding.
        assert abs(delta - expected_delay) <= 1.0, (
            f"attempt {expected_attempts}: expected ~{expected_delay}s, got {delta}s"
        )
    conn.close()


def test_record_success_after_failure_resets_attempts(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record_failure(
        conn,
        recording_id="r1",
        camera="cam",
        path="/m/r1.mp4",
        tier=1,
        recording_type="continuous",
        encoder="qsv",
    )
    fc._record(
        conn,
        recording_id="r1",
        camera="cam",
        path="/m/r1.mp4",
        tier=1,
        recording_type="continuous",
        encoder="qsv",
        size_before=1000,
        size_after=500,
        duration_sec=1.0,
        status=fc.STATUS_OK,
    )
    row = conn.execute(
        "SELECT t1_status, t1_attempts, t1_next_retry_at FROM files WHERE recording_id='r1'"
    ).fetchone()
    assert row["t1_status"] == fc.STATUS_OK
    assert row["t1_attempts"] == 0
    assert row["t1_next_retry_at"] is None
    conn.close()


def test_record_error_with_message(tmp_path):
    conn = _open_compress_db(tmp_path)
    fc._record(
        conn,
        recording_id="xyz",
        camera="back",
        path="/media/b.mp4",
        tier=2,
        recording_type="continuous",
        encoder="qsv",
        size_before=2000,
        size_after=None,
        duration_sec=None,
        status=fc.STATUS_ERROR,
        error_msg="file missing",
    )
    row = conn.execute("SELECT * FROM files WHERE recording_id='xyz'").fetchone()
    assert row["t2_error_msg"] == "file missing"
    assert row["recording_type"] == "continuous"
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# _fmt
# ═══════════════════════════════════════════════════════════════════════════════


def test_fmt_bytes():
    assert fc._fmt(512) == "512.0B"


def test_fmt_kilobytes():
    assert fc._fmt(2048) == "2.0KB"


def test_fmt_megabytes():
    assert fc._fmt(1024 * 1024) == "1.0MB"


def test_fmt_gigabytes():
    assert fc._fmt(1024**3) == "1.0GB"


def test_fmt_none():
    assert fc._fmt(None) == "N/A"


def test_fmt_float():
    assert fc._fmt(1536.0) == "1.5KB"


# ═══════════════════════════════════════════════════════════════════════════════
# files_stats materialised aggregate + triggers
# ═══════════════════════════════════════════════════════════════════════════════


def _stats_rows(conn: sqlite3.Connection) -> dict:
    """Read files_stats into a dict keyed by (camera, rtype)."""
    return {
        (r[0], r[1]): {
            "files_count": r[2],
            "tier0_bytes": r[3],
            "tier1_bytes": r[4],
            "tier2_bytes": r[5],
            "tier2_pre_encoded_bytes": r[6],
        }
        for r in conn.execute(
            "SELECT camera, rtype, files_count,"
            " tier0_bytes, tier1_bytes, tier2_bytes, tier2_pre_encoded_bytes"
            " FROM files_stats"
        )
    }


def _verify(conn: sqlite3.Connection) -> None:
    """Assert the trigger-maintained stats match a fresh aggregation."""
    assert fc.verify_files_stats(conn), "files_stats drifted from files"


def test_files_stats_insert_tier0(tmp_path):
    """INSERT of an unprobed-looking row bumps tier0_bytes + files_count."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")] == {
        "files_count": 1,
        "tier0_bytes": 1000,
        "tier1_bytes": 0,
        "tier2_bytes": 0,
        "tier2_pre_encoded_bytes": 0,
    }
    _verify(conn)


def test_files_stats_default_rtype_is_continuous(tmp_path):
    """A row inserted with NULL recording_type is bucketed as continuous."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, file_size)"
        " VALUES ('r1', 'cam', '/p', 500)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["files_count"] == 1
    assert s[("cam", "continuous")]["tier0_bytes"] == 500
    _verify(conn)


def test_files_stats_update_tier0_to_tier1(tmp_path):
    """Promoting a row to tier-1 moves bytes from tier0 to tier1."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000)"
    )
    conn.execute(
        "UPDATE files SET t1_status = 'ok', t1_file_size = 400"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 400,
        "tier2_bytes": 0,
        "tier2_pre_encoded_bytes": 0,
    }
    _verify(conn)


def test_files_stats_update_tier1_to_tier2(tmp_path):
    """Promoting tier-1 to tier-2 moves bytes from tier1 to tier2."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400)"
    )
    conn.execute(
        "UPDATE files SET t2_status = 'ok', t2_file_size = 100"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 0,
        "tier2_bytes": 100,
        "tier2_pre_encoded_bytes": 0,
    }
    _verify(conn)


def test_files_stats_update_segment_update_failed_is_compressed(tmp_path):
    """'segment_update_failed' is treated the same as 'ok' for bucketing."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000, 'segment_update_failed', 300)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["tier1_bytes"] == 300
    assert s[("cam", "continuous")]["tier0_bytes"] == 0
    _verify(conn)


def test_files_stats_update_error_stays_tier0(tmp_path):
    """status='error' doesn't count as compressed — stays in tier0."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000)"
    )
    conn.execute(
        "UPDATE files SET t1_status = 'error', t1_file_size = 0"
        " WHERE recording_id = 'r1'"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["tier0_bytes"] == 1000
    assert s[("cam", "motion")]["tier1_bytes"] == 0
    _verify(conn)


def test_files_stats_update_rtype_rebuckets(tmp_path):
    """Changing recording_type moves all of the file's bytes to the new bucket."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000)"
    )
    conn.execute("UPDATE files SET recording_type = 'motion' WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    assert ("cam", "continuous") in s
    assert s[("cam", "continuous")]["files_count"] == 0
    assert s[("cam", "continuous")]["tier0_bytes"] == 0
    assert s[("cam", "motion")]["files_count"] == 1
    assert s[("cam", "motion")]["tier0_bytes"] == 1000
    _verify(conn)


def test_files_stats_delete(tmp_path):
    """Deleting a row decrements its bucket."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 300)"
    )
    conn.execute("DELETE FROM files WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    # Row stays in stats but counts drop to zero.
    if ("cam", "motion") in s:
        assert s[("cam", "motion")]["files_count"] == 0
        assert s[("cam", "motion")]["tier0_bytes"] == 0
        assert s[("cam", "motion")]["tier1_bytes"] == 0
    _verify(conn)


def test_files_stats_multiple_cameras_and_tiers(tmp_path):
    """End-to-end: mix of cameras, rtypes, and tier states agree with aggregation."""
    conn = _open_compress_db(tmp_path)
    conn.executescript(
        """
        INSERT INTO files (recording_id, camera, path, recording_type, file_size)
        VALUES
          ('a', 'front', '/a', 'continuous', 100),
          ('b', 'front', '/b', 'motion',     200),
          ('c', 'back',  '/c', 'object',     300);
        UPDATE files SET t1_status = 'ok', t1_file_size = 50 WHERE recording_id = 'b';
        UPDATE files SET t1_status = 'ok', t1_file_size = 100,
                         t2_status = 'ok', t2_file_size = 40
            WHERE recording_id = 'c';
        """
    )
    conn.commit()
    _verify(conn)
    s = _stats_rows(conn)
    # front/continuous: 100 @ tier0
    assert s[("front", "continuous")] == {
        "files_count": 1,
        "tier0_bytes": 100,
        "tier1_bytes": 0,
        "tier2_bytes": 0,
        "tier2_pre_encoded_bytes": 0,
    }
    # front/motion: 50 @ tier1
    assert s[("front", "motion")]["tier1_bytes"] == 50
    # back/object: 40 @ tier2
    assert s[("back", "object")]["tier2_bytes"] == 40


def test_files_stats_backfill_from_existing_files(tmp_path):
    """If a DB pre-dates the triggers, reopening it populates files_stats."""
    # First open: install schema + triggers.  Insert rows with triggers active.
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 700)"
    )
    conn.commit()
    # Simulate a DB that pre-dates the stats table by emptying it.
    conn.execute("DELETE FROM files_stats")
    conn.commit()
    conn.close()

    # Reopening should notice stats is empty and rebuild from files.
    conn = _open_compress_db(tmp_path)
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["files_count"] == 1
    assert s[("cam", "motion")]["tier0_bytes"] == 700
    _verify(conn)


def test_files_stats_sibling_on_direct_status(tmp_path):
    """A row in t2_status='direct' contributes its t2_file_size to the
    sibling counter on the same camera/rtype bucket.  tier1_bytes still
    reflects the primary (tier-1) file separately."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size, t2_status, t2_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400, 'direct', 80)"
    )
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 400,
        "tier2_bytes": 0,
        "tier2_pre_encoded_bytes": 80,
    }
    _verify(conn)


def test_files_stats_sibling_drops_on_swap_to_ok(tmp_path):
    """When swap_t2 flips 'direct' → 'ok' the sibling counter drops and
    bytes move into tier2_bytes.  Mirrors the on-disk swap exactly."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size, t2_status, t2_file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 1000, 'ok', 400, 'direct', 80)"
    )
    conn.commit()
    conn.execute("UPDATE files SET t2_status = 'ok' WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "continuous")] == {
        "files_count": 1,
        "tier0_bytes": 0,
        "tier1_bytes": 0,
        "tier2_bytes": 80,
        "tier2_pre_encoded_bytes": 0,
    }
    _verify(conn)


def test_files_stats_sibling_drops_on_direct_to_error(tmp_path):
    """If a direct row's swap fails and t2_status flips to 'error', the
    sibling counter drops back to 0 (the on-disk file is gone or unusable)."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size, t2_status, t2_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400, 'direct', 80)"
    )
    conn.commit()
    conn.execute("UPDATE files SET t2_status = 'error' WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["tier2_pre_encoded_bytes"] == 0
    _verify(conn)


def test_files_stats_sibling_drops_on_delete(tmp_path):
    """Deleting a 'direct' row removes its sibling-bytes contribution."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size, t2_status, t2_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400, 'direct', 80)"
    )
    conn.commit()
    conn.execute("DELETE FROM files WHERE recording_id = 'r1'")
    conn.commit()
    s = _stats_rows(conn)
    if ("cam", "motion") in s:
        assert s[("cam", "motion")]["tier2_pre_encoded_bytes"] == 0
    _verify(conn)


def test_files_stats_migrates_existing_db(tmp_path):
    """Reopening a DB that pre-dates ``tier2_pre_encoded_bytes`` adds the
    column, drops the stale triggers, and backfills via the audit pass.
    """
    db_path = tmp_path / "compress.db"
    # Hand-build a pre-migration files_stats schema (no sibling column,
    # old triggers).  The ``files`` schema can be the current one because
    # the migration only touches files_stats.
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(fc.SCHEMA)
    conn.executescript(
        """
        CREATE TABLE files_stats (
            camera       TEXT    NOT NULL,
            rtype        TEXT    NOT NULL,
            files_count  INTEGER NOT NULL DEFAULT 0,
            tier0_bytes  INTEGER NOT NULL DEFAULT 0,
            tier1_bytes  INTEGER NOT NULL DEFAULT 0,
            tier2_bytes  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (camera, rtype)
        );
        """
    )
    # A 'direct' row that should contribute to sibling bytes after migration.
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size,"
        " t1_status, t1_file_size, t2_status, t2_file_size)"
        " VALUES ('r1', 'cam', '/p', 'motion', 1000, 'ok', 400, 'direct', 80)"
    )
    conn.commit()
    conn.close()

    # Migration runs on reopen — adds the column, recreates triggers,
    # detects drift, backfills.
    conn = _open_compress_db(tmp_path)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(files_stats)").fetchall()}
    assert "tier2_pre_encoded_bytes" in cols
    s = _stats_rows(conn)
    assert s[("cam", "motion")]["tier1_bytes"] == 400
    assert s[("cam", "motion")]["tier2_pre_encoded_bytes"] == 80
    _verify(conn)


def test_files_stats_audit_rebuilds_on_drift(tmp_path):
    """If files_stats drifts out of sync, reopening detects it and rebuilds."""
    conn = _open_compress_db(tmp_path)
    conn.execute(
        "INSERT INTO files (recording_id, camera, path, recording_type, file_size)"
        " VALUES ('r1', 'cam', '/p', 'continuous', 500)"
    )
    conn.commit()
    # Manually corrupt files_stats to simulate trigger drift.
    conn.execute("UPDATE files_stats SET tier0_bytes = 9999 WHERE camera = 'cam'")
    conn.commit()
    conn.close()

    conn = _open_compress_db(tmp_path)
    s = _stats_rows(conn)
    assert s[("cam", "continuous")]["tier0_bytes"] == 500  # rebuilt, not 9999
    _verify(conn)
