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

Scale **matches the running production database** (~2.04 M rows in
each of ``files`` and ``frigate.recordings``, observed 2026-05-19):
8 classes × 250 000 = 2 000 000 rows in ``files``, 2 000 000 in
``frigate.recordings``.  ``ANALYZE`` runs after the bulk load so the
planner has the same selectivity stats production does — a regression
that only manifests at scale (the t2 ``SCAN f`` trap was an example
of exactly that) actually shows up here, not just in production.

Fixture cost is amortised across the module via ``scope="module"``: the
2 M-row insert + index rebuild + ANALYZE runs **once** per test run,
not per test.  The bulk load uses the standard SQLite pattern (drop
partial indexes → bulk ``executemany`` → recreate indexes → ANALYZE)
which is several × faster than maintaining the indexes per row.  The
12 EXPLAIN tests themselves remain sub-millisecond against the loaded
DB.
"""

import sqlite3
import time

import pytest

import frigate_compressor as fc
from fc_helpers import _make_options, _open_compress_db

# Per-class row count and total ``frigate.recordings`` count sized to
# match the production database (~2.04 M rows in each table on the live
# HASS instance, observed 2026-05-19).  ANALYZE on a 2 M-row table
# produces the same selectivity stats production has, so the planner's
# cost-based decisions in the test match what production sees — a
# regression that only manifests at scale (the t2 SCAN-f trap was an
# example) actually shows up here.
#
# Built once per module via the ``ctx`` fixture; bulk-loaded with the
# partial indexes temporarily dropped (the standard SQLite bulk-load
# pattern — rebuilding a single index on a populated table is roughly
# 3–5× faster than maintaining it incrementally per row).  Total
# fixture wall time is dominated by the inserts + index rebuild +
# ANALYZE, not by anything the tests do.
_PER_CLASS = 250_000  # 8 classes × 250 K = 2 000 000 rows in ``files``
_RECORDINGS = 2_000_000


# ════════════════════════════════════════════════════════════════════════
# fixtures
# ════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def ctx(tmp_path_factory):
    """``CompressorContext`` populated at scale, shared across the module.

    All tests in this file are read-only EXPLAIN-QUERY-PLAN queries that
    don't mutate the DB, so the fixture is module-scoped: built once,
    reused for every test.

    Population:

      * ``files`` — ``_PER_CLASS`` rows per (t1_status, t2_status) status
        class, 8 classes covering every code path the indexes target
        (t1 pending, t2 chained pending, swap pending, t2 error/retry,
        done, seg-retry on either side, t1 error).  ``start_time`` is
        spread monotonically across the class so range-scan plans have
        meaningful work.
      * ``frigate.recordings`` — ``_RECORDINGS`` rows, ``start_time``
        spread minute-by-minute.
      * ``files_stats`` triggers dropped before the bulk load (we test
        plans, not the rollup table — and per-row triggers would turn
        the 8 000-row insert into a multi-second operation).
      * ``ANALYZE`` run after both loads so the planner's stats reflect
        actual selectivity, not its big-table default.
    """
    tmp_path = tmp_path_factory.mktemp("fcqp")
    cfg = fc.load_config(str(_make_options(tmp_path)))
    conn = _open_compress_db(tmp_path)
    fc._attach_frigate(conn, cfg, "frigate")
    cam = next(iter(cfg.cameras))
    iso = "2026-01-01T00:00:00"
    now = time.time()

    # Bulk-load preparation:
    #   * drop ``files_stats`` AFTER-* triggers (per-row trigger work would
    #     dominate a 2 M-row insert; the rollup table is irrelevant to plan
    #     tests);
    #   * drop the ``files`` partial indexes (``idx_files_*``) and the
    #     ``frigate.recordings`` indexes, then recreate them after the bulk
    #     insert — rebuilding a single index on a populated table is 3–5×
    #     faster than maintaining it row-by-row during insert.  Each
    #     index's ``sql`` is stashed from ``sqlite_master`` so we don't
    #     have to repeat the WHERE-clause definitions here.
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger'"
    ).fetchall():
        conn.execute(f"DROP TRIGGER IF EXISTS {row['name']}")

    files_idx_ddl = [
        r["sql"]
        for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND tbl_name='files' AND name LIKE 'idx_files_%'"
        ).fetchall()
        if r["sql"]
    ]
    for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='files' AND name LIKE 'idx_files_%'"
    ).fetchall():
        conn.execute(f"DROP INDEX IF EXISTS {r['name']}")

    classes = [
        # (t1_status, t2_status, comment)
        (None, None),  # t1 pending
        ("ok", None),  # t2 chained pending
        ("ok", "direct"),  # swap pending
        ("ok", "error"),  # t2 retry / t2_error index
        ("ok", "ok"),  # done (excluded from all partials)
        ("segment_update_failed", None),  # seg-retry, t1 side
        ("ok", "segment_update_failed"),  # seg-retry, t2 side
        ("error", None),  # t1_error index
    ]

    def _gen_files():
        # Generator (not a materialised list) — 2 M tuples × ~250 B each
        # would peak ~500 MB; streaming through ``executemany`` stays flat.
        rid = 0
        for ci, (t1s, t2s) in enumerate(classes):
            for j in range(_PER_CLASS):
                rid += 1
                yield (
                    f"r{rid}",
                    cam,
                    f"/r/r{rid}.mp4",
                    "continuous",
                    now - 60 * 86400 - (ci * _PER_CLASS + j),
                    t1s,
                    t2s,
                    iso,
                    iso,
                    iso,
                    5_000_000,
                )

    conn.execute("BEGIN")
    conn.executemany(
        "INSERT INTO files(recording_id, camera, path, recording_type, "
        "start_time, t1_status, t2_status, "
        "t1_compressed_at, t2_compressed_at, scanned_at, file_size) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        _gen_files(),
    )
    conn.commit()
    # Rebuild the partial indexes now that the table is populated.
    for sql in files_idx_ddl:
        conn.execute(sql)
    conn.commit()

    frig_conn = sqlite3.connect(str(cfg.frigate_db))
    # Same drop/recreate pattern on the frigate side.  Skip the
    # ``sqlite_autoindex_*`` for PRIMARY KEY — that's needed for
    # uniqueness during insert.
    frig_idx_ddl = [
        r[0]
        for r in frig_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND tbl_name='recordings' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        if r[0]
    ]
    for r in frig_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='recordings' AND name NOT LIKE 'sqlite_%'"
    ).fetchall():
        frig_conn.execute(f"DROP INDEX IF EXISTS {r[0]}")
    frig_conn.execute("BEGIN")
    frig_conn.executemany(
        "INSERT INTO recordings(id, camera, path, start_time, motion, objects)"
        " VALUES (?,?,?,?,?,?)",
        (
            (f"fr{i}", cam, f"/r/fr{i}.mp4", now - i * 60, 0, 0)
            for i in range(_RECORDINGS)
        ),
    )
    frig_conn.commit()
    for sql in frig_idx_ddl:
        frig_conn.execute(sql)
    frig_conn.commit()
    frig_conn.close()

    conn.execute("ANALYZE")
    conn.commit()

    yield fc.CompressorContext(cfg=cfg, compress_db=conn)
    conn.close()


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


def test_encode_eligibility_uses_both_partial_indexes(ctx):
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

    where, params = fc._build_eligible_where(ctx.cfg, time.time())
    assert where, "fixture has at least one enabled (camera, tier)"
    plans = _plans(ctx.compress_db, f"SELECT rid FROM ({where})", params)
    assert any("idx_files_t1_pending_age" in p for p in plans), plans
    assert any("idx_files_t2_pending_age" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "f"), plans


# ════════════════════════════════════════════════════════════════════════
# swap_eligibility — traced so the test exercises the live call path
# ════════════════════════════════════════════════════════════════════════


def test_swap_eligibility_uses_t2_pending_age_index(ctx):
    """``get_eligible_swaps`` must hit ``idx_files_t2_pending_age``.

    **Regression for PR #182 (latent twin)**: the swap query's
    ``t2_status = 'direct'`` predicate doesn't chain through SQLite's
    prover to the index's ``(IS NULL OR NOT IN (...))`` WHERE, so the
    plan fell back to ``SCAN f`` despite a code comment claiming it
    used the index.  The fix put the index's WHERE in as a verbatim
    AND-conjunct alongside the ``= 'direct'`` filter.
    """

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


def test_segment_retry_uses_partial_index(ctx):
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

    sql = (
        "SELECT recording_id, camera, path FROM files "
        "WHERE t1_status = ? OR t2_status = ?"
    )
    plans = _plans(ctx.compress_db, sql, ("segment_update_failed",) * 2)
    assert any("idx_files_seg_retry" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "files"), plans


def test_recent_errors_view_uses_error_partial_indexes(ctx):
    """The ``recent_errors`` view (used by housekeeping) filters by
    ``t1_status = 'error'`` and ``t2_status = 'error'`` — should hit
    ``idx_files_t{1,2}_error`` (one branch per OR side).
    """

    plans = _plans(ctx.compress_db, "SELECT * FROM recent_errors LIMIT 20")
    assert any("idx_files_t1_error" in p for p in plans), plans
    assert any("idx_files_t2_error" in p for p in plans), plans
    assert not _has_unindexed_scan(plans, "files"), plans


# ════════════════════════════════════════════════════════════════════════
# probe_loop — incremental scan + MAX-cursor seed
# ════════════════════════════════════════════════════════════════════════


def test_probe_incremental_uses_start_time_index(ctx):
    """Steady-state probe poll (``_get_unprobed_recordings(cursor=value)``)
    seeks ``frigate.recordings`` by ``start_time >= ?`` — must hit
    ``recordings_start_time``.  This runs every ``PROBE_SLEEP_SEC``
    (60s) on every running daemon, so a regression to a full scan
    would be 2M+ row reads every minute.
    """

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


def test_probe_cursor_seed_uses_covering_index(ctx):
    """``_max_recording_start_time`` (``SELECT MAX(start_time) FROM
    frigate.recordings``) must use a covering index — without it the
    seed query would scan all recordings on startup.
    """

    plans = _plans(ctx.compress_db, "SELECT MAX(start_time) FROM frigate.recordings")
    assert any("COVERING INDEX" in p.upper() for p in plans), plans


# ════════════════════════════════════════════════════════════════════════
# encode_eligibility — time_until_next_eligible's per-camera MIN seek
# ════════════════════════════════════════════════════════════════════════


def test_time_until_next_eligible_uses_camera_start_time_index(ctx):
    """``time_until_next_eligible`` issues, per enabled (camera, tier),
    ``SELECT start_time FROM frigate.recordings WHERE camera = ? AND
    start_time > ? ORDER BY start_time ASC LIMIT 1`` — must hit
    ``recordings_camera_start_time_end_time``.  Code comment in
    encode_eligibility documents that ``GROUP BY camera`` would force a
    full SCAN, so this per-camera seek is the deliberate shape.
    """

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


def test_mqtt_recent_rows_uses_indexed_by_hint(ctx):
    """``collect_frigate_stats``'s rate-window aggregate uses an explicit
    ``INDEXED BY recordings_start_time`` hint to force range-scan over a
    small recent window rather than the planner's default of scanning
    a camera index over all 800K+ rows.  Pin the hint actually takes
    effect — if the index is renamed or the hint deleted, this fails.
    """

    plans = _plans(
        ctx.compress_db,
        "SELECT camera, SUM(COALESCE(segment_size, 0)) "
        "FROM frigate.recordings INDEXED BY recordings_start_time "
        "WHERE start_time >= ? GROUP BY camera",
        (time.time() - 450,),
    )
    assert any("recordings_start_time" in p for p in plans), plans


def test_mqtt_oldest_per_camera_uses_covering_index(ctx):
    """``collect_frigate_stats`` does one ``SELECT MIN(start_time) FROM
    frigate.recordings WHERE camera = ?`` per camera (deliberately
    avoiding ``GROUP BY camera`` to keep the planner on the covering
    index).  Must hit ``recordings_camera_start_time_end_time``.
    """

    plans = _plans(
        ctx.compress_db,
        "SELECT MIN(start_time) FROM frigate.recordings WHERE camera = ?",
        (next(iter(ctx.cfg.cameras)),),
    )
    assert any("recordings_camera_start_time_end_time" in p for p in plans), plans


def test_mqtt_t1_backlog_exists_uses_pending_age_index(ctx):
    """Per-camera ``EXISTS`` backlog check for tier-1 in
    ``collect_frigate_stats``.  Must hit ``idx_files_t1_pending_age``
    so the 2-column range seek (camera=X, start_time<threshold) short-
    circuits on ``LIMIT 1`` (the code comment notes an earlier shape
    walked ~70K rows per camera before LIMIT fired).
    """

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


def test_mqtt_t2_backlog_exists_uses_pending_age_index(ctx):
    """Per-camera ``EXISTS`` backlog check for tier-2.  Must hit
    ``idx_files_t2_pending_age``.  Shape mirrors the encode-t2 branch's
    verbatim-conjunct fix from PR #182 — if the query is ever
    refactored to use the old folded-NOT-IN shape, this fails.
    """

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


def test_mqtt_error_counts_use_error_partial_indexes(ctx):
    """Per-camera retry COUNTs in ``collect_frigate_stats`` —
    ``SELECT COUNT(*) FROM files WHERE camera=? AND t{1,2}_status='error'``
    must hit ``idx_files_t{1,2}_error``.  The mqtt_stats code comment
    asserts this works; pin it so a future schema/index change can't
    silently regress to a 2M-row scan every 5 min.
    """

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
