# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Regression: every production query that should hit a partial index does.

Pins ``EXPLAIN QUERY PLAN`` for each query that targets a ``files``-table
partial index (``idx_files_t{1,2}_pending_age``, ``idx_files_seg_retry``,
``idx_files_t{1,2}_error``).

Motivation — the t2 eligibility bug (PR #182): SQLite's partial-index
implication prover is *syntactic*, so a query that is logically more
restrictive than the index's WHERE can still be rejected.  Both encode
and swap fell back to a full ``files`` SCAN at ~2M rows for that reason
and the regression sat latent because nothing asserted the plan.  These
tests fail loudly if any production query stops hitting its expected
index, *without* needing a 1M-row DB to surface the cost.

Scale here is intentionally tiny (one row per status class).  Partial-
index usability is a syntactic implication check that doesn't depend on
table size, and ``ANALYZE`` is intentionally not run so the planner
uses its default "large table" heuristic — matching the cost regime
production actually sees.  Each test runs in milliseconds.
"""

from __future__ import annotations

import sqlite3
import time

import frigate_compressor as fc
from fc_helpers import _insert_recording, _make_options, _open_compress_db


# ════════════════════════════════════════════════════════════════════════
# helpers
# ════════════════════════════════════════════════════════════════════════


def _populated_ctx(tmp_path):
    """``CompressorContext`` with frigate attached and one ``files`` row per
    interesting (t1_status, t2_status) class so every partial index is
    non-empty.

    Status mix covers: tier-1 pending (NULL), tier-2 chained pending
    (NULL), swap pending (``direct``), tier-2 error/retry (``error``),
    fully done (``ok``), segment-retry on either side
    (``segment_update_failed``), and tier-1 error.
    """
    cfg = fc.load_config(str(_make_options(tmp_path)))
    conn = _open_compress_db(tmp_path)
    fc._attach_frigate(conn, cfg, "frigate")
    cam = next(iter(cfg.cameras))
    iso = "2026-01-01T00:00:00"
    now = time.time()
    rows = [
        # rid,    t1_status,                t2_status
        ("r1", None, None),  # t1 pending
        ("r2", "ok", None),  # t2 chained pending
        ("r3", "ok", "direct"),  # swap pending
        ("r4", "ok", "error"),  # t2 retry / t2_error idx
        ("r5", "ok", "ok"),  # done
        ("r6", "segment_update_failed", None),  # seg-retry, t1 side
        ("r7", "ok", "segment_update_failed"),  # seg-retry, t2 side
        ("r8", "error", None),  # t1_error idx
    ]
    for rid, t1s, t2s in rows:
        conn.execute(
            "INSERT INTO files(recording_id, camera, path, recording_type, "
            "start_time, t1_status, t2_status, "
            "t1_compressed_at, t2_compressed_at, scanned_at, file_size) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                rid,
                cam,
                f"/r/{rid}.mp4",
                "continuous",
                now - 60 * 86400,
                t1s,
                t2s,
                iso,
                iso,
                iso,
                5_000_000,
            ),
        )
    conn.commit()
    # Frigate-side recordings: a few rows per camera so queries against
    # the attached ``frigate.recordings`` (probe loop, mqtt_stats,
    # time_until_next_eligible) have something to plan against.
    frig_path = cfg.frigate_db
    frig_conn = sqlite3.connect(str(frig_path))
    for i in range(5):
        _insert_recording(
            frig_conn, f"fr{i}", cam, f"/r/fr{i}.mp4", now - i * 60, motion=0, objects=0
        )
    frig_conn.close()
    return fc.CompressorContext(cfg=cfg, compress_db=conn)


def _plans(conn, sql, params=()):
    """``EXPLAIN QUERY PLAN`` for *sql* — list of plan-row ``detail`` strings."""
    return [
        row["detail"]
        for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    ]


def _has_unindexed_scan(plans, table):
    """True iff any plan line is ``SCAN <table>`` without ``USING INDEX``.

    A line like ``SCAN f USING INDEX idx_…`` is fine (a covering walk of
    the partial index); ``SCAN f`` alone means a full table scan.
    """
    return any(
        (p == f"SCAN {table}" or p.startswith(f"SCAN {table} "))
        and "USING INDEX" not in p
        for p in plans
    )


def _trace_executed(conn, fn):
    """Run *fn* while capturing the SQL of every statement the connection
    executes.  Python's ``set_trace_callback`` hands back the SQL with
    parameter values already substituted (since 3.12), so each captured
    string can be fed straight to EXPLAIN QUERY PLAN.
    """
    captured: list[str] = []
    conn.set_trace_callback(captured.append)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    return captured


# ════════════════════════════════════════════════════════════════════════
# encode_eligibility — both branches share the union built by
# _build_eligible_where, so one test covers t1 + t2 in a single plan
# ════════════════════════════════════════════════════════════════════════


def test_encode_eligibility_uses_both_partial_indexes(tmp_path):
    """Both branches of ``_build_eligible_where`` must hit their
    partial indexes — ``idx_files_t1_pending_age`` and
    ``idx_files_t2_pending_age``.

    **Regression for PR #182**: before the fix, the t2 branch silently
    fell back to ``SCAN f`` because its NOT IN list had an extra
    ``'direct'`` that the index's NOT IN lacked, and SQLite's prover
    can't reason about list inclusion.  Folding the ``'direct'``
    exclusion back into the index's NOT IN list, or anywhere else that
    makes the t2 branch's WHERE no longer contain the index's WHERE
    verbatim, breaks this test.
    """
    ctx = _populated_ctx(tmp_path)
    where, params = fc._build_eligible_where(ctx.cfg, time.time())
    assert where, "fixture has at least one enabled (camera, tier)"
    plans = _plans(ctx.compress_db, f"SELECT rid FROM ({where})", params)
    assert any("idx_files_t1_pending_age" in p for p in plans), plans
    assert any("idx_files_t2_pending_age" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "f"), plans


# ════════════════════════════════════════════════════════════════════════
# swap_eligibility — traced so the test exercises the live call path
# ════════════════════════════════════════════════════════════════════════


def test_swap_eligibility_uses_t2_pending_age_index(tmp_path):
    """``get_eligible_swaps`` must hit ``idx_files_t2_pending_age``.

    **Regression for PR #182 (latent twin)**: the swap query's
    ``t2_status = 'direct'`` predicate doesn't chain through SQLite's
    prover to the index's ``(IS NULL OR NOT IN (...))`` WHERE, so the
    plan fell back to ``SCAN f`` despite a code comment claiming it
    used the index.  The fix put the index's WHERE in as a verbatim
    AND-conjunct alongside the ``= 'direct'`` filter.
    """
    ctx = _populated_ctx(tmp_path)
    captured = _trace_executed(ctx.compress_db, lambda: fc.get_eligible_swaps(ctx))
    # The function executes one SELECT against `files`.  Pick that out
    # of whatever else SQLite traced (savepoints, etc.).
    select = next(
        s
        for s in captured
        if "FROM files" in s and s.lstrip().lower().startswith("select")
    )
    plans = _plans(ctx.compress_db, select)
    assert any("idx_files_t2_pending_age" in p for p in plans), (select, plans)
    assert not _has_unindexed_scan(plans, "f"), (select, plans)


# ════════════════════════════════════════════════════════════════════════
# housekeeping partial-index queries
# ════════════════════════════════════════════════════════════════════════


def test_segment_retry_uses_partial_index(tmp_path):
    """``_hk_retry_segment_updates`` issues
    ``WHERE t1_status = ? OR t2_status = ?`` with both placeholders
    bound to ``'segment_update_failed'`` — must hit
    ``idx_files_seg_retry``.

    This is the strictest prover case in the codebase: the parameter
    values are bound at execution time, not present as literals in the
    SQL text, so the planner has to accept the partial index based on
    placeholder symmetry rather than literal-equality.  Worth pinning
    explicitly because a future "harmless" rewrite (e.g. switching to
    ``IN (?, ?)``) could quietly break it.
    """
    ctx = _populated_ctx(tmp_path)
    sql = (
        "SELECT recording_id, camera, path FROM files "
        "WHERE t1_status = ? OR t2_status = ?"
    )
    plans = _plans(ctx.compress_db, sql, ("segment_update_failed",) * 2)
    assert any("idx_files_seg_retry" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "files"), plans


def test_recent_errors_view_uses_error_partial_indexes(tmp_path):
    """The ``recent_errors`` view (used by housekeeping) filters by
    ``t1_status = 'error'`` and ``t2_status = 'error'`` — should hit
    ``idx_files_t{1,2}_error`` (one branch per OR side).
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(ctx.compress_db, "SELECT * FROM recent_errors LIMIT 20")
    assert any("idx_files_t1_error" in p for p in plans), plans
    assert any("idx_files_t2_error" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "files"), plans


# ════════════════════════════════════════════════════════════════════════
# probe_loop — incremental scan + MAX-cursor seed
# ════════════════════════════════════════════════════════════════════════


def test_probe_incremental_uses_start_time_index(tmp_path):
    """Steady-state probe poll (``_get_unprobed_recordings(cursor=value)``)
    seeks ``frigate.recordings`` by ``start_time >= ?`` — must hit
    ``recordings_start_time``.  This runs every ``PROBE_SLEEP_SEC``
    (60s) on every running daemon, so a regression to a full scan
    would be 2M+ row reads every minute.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        """
        SELECT r.id, r.camera, r.path, r.start_time, r.motion, r.objects
        FROM   frigate.recordings r
        LEFT JOIN files f ON f.recording_id = r.id
        WHERE  r.start_time >= ?
          AND (f.recording_id IS NULL OR f.scanned_at IS NULL)
        ORDER BY r.start_time ASC LIMIT 5000
        """,
        (time.time() - 3600,),
    )
    assert any("recordings_start_time" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "r"), plans


def test_probe_cursor_seed_uses_covering_index(tmp_path):
    """``_max_recording_start_time`` (``SELECT MAX(start_time) FROM
    frigate.recordings``) must use a covering index — without it the
    seed query would scan all recordings on startup.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(ctx.compress_db, "SELECT MAX(start_time) FROM frigate.recordings")
    assert any("COVERING INDEX" in p.upper() for p in plans), plans


# ════════════════════════════════════════════════════════════════════════
# encode_eligibility — time_until_next_eligible's per-camera MIN seek
# ════════════════════════════════════════════════════════════════════════


def test_time_until_next_eligible_uses_camera_start_time_index(tmp_path):
    """``time_until_next_eligible`` issues, per enabled (camera, tier),
    ``SELECT start_time FROM frigate.recordings WHERE camera = ? AND
    start_time > ? ORDER BY start_time ASC LIMIT 1`` — must hit
    ``recordings_camera_start_time_end_time``.  Code comment in
    encode_eligibility documents that ``GROUP BY camera`` would force a
    full SCAN, so this per-camera seek is the deliberate shape.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        "SELECT start_time FROM frigate.recordings "
        "WHERE camera = ? AND start_time > ? ORDER BY start_time ASC LIMIT 1",
        (next(iter(ctx.cfg.cameras)), time.time() - 8 * 86400),
    )
    assert any("recordings_camera_start_time_end_time" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "recordings"), plans


# ════════════════════════════════════════════════════════════════════════
# mqtt_stats — runs every mqtt_publish_interval_seconds (300s production)
# ════════════════════════════════════════════════════════════════════════


def test_mqtt_recent_rows_uses_indexed_by_hint(tmp_path):
    """``collect_frigate_stats``'s rate-window aggregate uses an explicit
    ``INDEXED BY recordings_start_time`` hint to force range-scan over a
    small recent window rather than the planner's default of scanning
    a camera index over all 800K+ rows.  Pin the hint actually takes
    effect — if the index is renamed or the hint deleted, this fails.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        "SELECT camera, SUM(COALESCE(segment_size, 0)) "
        "FROM frigate.recordings INDEXED BY recordings_start_time "
        "WHERE start_time >= ? GROUP BY camera",
        (time.time() - 450,),
    )
    assert any("recordings_start_time" in p for p in plans), plans


def test_mqtt_oldest_per_camera_uses_covering_index(tmp_path):
    """``collect_frigate_stats`` does one ``SELECT MIN(start_time) FROM
    frigate.recordings WHERE camera = ?`` per camera (deliberately
    avoiding ``GROUP BY camera`` to keep the planner on the covering
    index).  Must hit ``recordings_camera_start_time_end_time``.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        "SELECT MIN(start_time) FROM frigate.recordings WHERE camera = ?",
        (next(iter(ctx.cfg.cameras)),),
    )
    assert any("recordings_camera_start_time_end_time" in p for p in plans), plans


def test_mqtt_t1_backlog_exists_uses_pending_age_index(tmp_path):
    """Per-camera ``EXISTS`` backlog check for tier-1 in
    ``collect_frigate_stats``.  Must hit ``idx_files_t1_pending_age``
    so the 2-column range seek (camera=X, start_time<threshold) short-
    circuits on ``LIMIT 1`` (the code comment notes an earlier shape
    walked ~70K rows per camera before LIMIT fired).
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        """
        SELECT 1 FROM files f
        WHERE f.camera = ? AND f.start_time < ?
          AND (f.t1_status IS NULL
               OR f.t1_status NOT IN ('ok', 'segment_update_failed', 'give_up'))
        LIMIT 1
        """,
        (next(iter(ctx.cfg.cameras)), time.time() - 8 * 86400),
    )
    assert any("idx_files_t1_pending_age" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "f"), plans


def test_mqtt_t2_backlog_exists_uses_pending_age_index(tmp_path):
    """Per-camera ``EXISTS`` backlog check for tier-2.  Must hit
    ``idx_files_t2_pending_age``.  Shape mirrors the encode-t2 branch's
    verbatim-conjunct fix from PR #182 — if the query is ever
    refactored to use the old folded-NOT-IN shape, this fails.
    """
    ctx = _populated_ctx(tmp_path)
    plans = _plans(
        ctx.compress_db,
        """
        SELECT 1 FROM files f
        WHERE f.camera = ? AND f.start_time < ?
          AND f.t1_status IN ('ok', 'segment_update_failed')
          AND (f.t2_status IS NULL
               OR f.t2_status NOT IN ('ok', 'segment_update_failed', 'give_up'))
        LIMIT 1
        """,
        (next(iter(ctx.cfg.cameras)), time.time() - 30 * 86400),
    )
    assert any("idx_files_t2_pending_age" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "f"), plans


def test_mqtt_error_counts_use_error_partial_indexes(tmp_path):
    """Per-camera retry COUNTs in ``collect_frigate_stats`` —
    ``SELECT COUNT(*) FROM files WHERE camera=? AND t{1,2}_status='error'``
    must hit ``idx_files_t{1,2}_error``.  The mqtt_stats code comment
    asserts this works; pin it so a future schema/index change can't
    silently regress to a 2M-row scan every 5 min.
    """
    ctx = _populated_ctx(tmp_path)
    cam = next(iter(ctx.cfg.cameras))
    t1_plans = _plans(
        ctx.compress_db,
        "SELECT COUNT(*) FROM files WHERE camera = ? AND t1_status = 'error'",
        (cam,),
    )
    t2_plans = _plans(
        ctx.compress_db,
        "SELECT COUNT(*) FROM files WHERE camera = ? AND t2_status = 'error'",
        (cam,),
    )
    assert any("idx_files_t1_error" in p for p in t1_plans), t1_plans
    assert any("idx_files_t2_error" in p for p in t2_plans), t2_plans
