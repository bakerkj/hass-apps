# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Eligible-recordings query + ``time_until_next_eligible``."""

from __future__ import annotations

import time

from .config import Config
from .context import CompressorContext
from .database import (
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
)
from .throttle import MAX_SLEEP_SEC

_ELIGIBLE_BATCH_SIZE = 500


def _build_eligible_where(cfg: Config, effective_now: float) -> tuple[str, list]:
    """Build a WHERE clause for eligible-file IDs driven from ``files``.

    ``effective_now`` is "now" for normal queries; callers can pass a
    future-shifted value to count what *will* be eligible by then.

    Returns ``(where_sql, params)`` to paste into a UNION ALL over the
    two partial eligibility indexes ``idx_files_t{1,2}_pending_age``.
    Each camera+tier combo with work to do contributes one branch; the
    planner range-seeks on (camera=?, start_time<?) in the partial
    index — scans only the handful of actually-eligible rows.

    Returns ``("", [])`` if no enabled camera has any enabled tier.

    The status literals are inlined into the SQL (rather than bound as
    parameters) so the planner can prove the query's WHERE implies the
    partial index's WHERE — which uses the same literal strings.  With
    placeholders, the implication check fails and SQLite errors out
    on ``INDEXED BY`` ("no query solution").
    """
    t1_parts: list[str] = []
    t1_params: list = []
    t2_parts: list[str] = []
    t2_params: list = []
    for name, cam in cfg.cameras.items():
        if not cam.enabled:
            continue
        if cam.tier1.enabled:
            t1_cutoff = effective_now - (cam.tier1.min_days * 86400)
            # Inner branch returns ONLY columns covered by the partial index
            # (rowid + start_time, plus the literal ``tier`` constant).  No
            # ``f.path`` / ``f.recording_type`` here — those would force a
            # row fetch per matched index entry, and at production scale
            # that's 500 row fetches × 12 branches = 6000 fetches per call,
            # which dwarfs everything else in the daemon's IO profile.
            # The outer SELECT in ``get_eligible_recordings`` JOINs back to
            # ``files`` for those columns on only the final ~500 rows.
            #
            # ``INDEXED BY idx_files_t1_pending_age`` forces the planner to
            # use the partial index (otherwise it can regress to
            # idx_files_camera).  ``ORDER BY f.start_time ASC LIMIT N`` is
            # naturally satisfied by the index's (camera, start_time) order
            # so SQLite stops at the LIMIT without an extra sort.
            t1_parts.append(
                f"""
                SELECT * FROM (
                    SELECT f.rowid AS rid, f.start_time, 1 AS tier
                    FROM files f INDEXED BY idx_files_t1_pending_age
                    WHERE f.camera = ? AND f.start_time < ?
                      AND (f.t1_status IS NULL
                           OR f.t1_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}'))
                    ORDER BY f.start_time ASC
                    LIMIT {_ELIGIBLE_BATCH_SIZE}
                )
                """
            )
            t1_params.extend([name, t1_cutoff])
        if cam.tier2.enabled:
            t2_cutoff = effective_now - (cam.tier2.min_days * 86400)
            t2_parts.append(
                f"""
                SELECT * FROM (
                    SELECT f.rowid AS rid, f.start_time, 2 AS tier
                    FROM files f INDEXED BY idx_files_t2_pending_age
                    WHERE f.camera = ? AND f.start_time < ?
                      AND f.t1_status IN
                          ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                      AND (f.t2_status IS NULL
                           OR f.t2_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}'))
                    ORDER BY f.start_time ASC
                    LIMIT {_ELIGIBLE_BATCH_SIZE}
                )
                """
            )
            t2_params.extend([name, t2_cutoff])

    parts = t1_parts + t2_parts
    if not parts:
        return "", []
    return " UNION ALL ".join(parts), t1_params + t2_params


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns up to ``_ELIGIBLE_BATCH_SIZE`` recordings eligible for compression
    that haven't been successfully compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    Driven entirely from the partial eligibility indexes on ``files``.
    Path and recording_type are denormalised onto ``files`` at probe time,
    so this query never touches Frigate's recordings table — it stays
    inside the small partial-index hot set on every call.
    """
    cfg = ctx.cfg
    union_sql, params = _build_eligible_where(cfg, time.time())
    if not union_sql:
        return []
    params.append(_ELIGIBLE_BATCH_SIZE)

    # Two-stage query: inner branches stay index-only and emit (rid,
    # start_time, tier); the outer JOIN fetches path/recording_type +
    # probe metadata (width/height/fps) for only the rows that survive
    # the LIMIT.  Returning probe metadata here lets the worker skip its
    # own per-file ``SELECT width, height, fps FROM files``, which used
    # to be one of the bigger remaining IO sources at the production
    # compression rate.
    with ctx.compress_db_lock:
        rows = ctx.compress_db.execute(
            f"""
            SELECT f.recording_id, f.camera, f.path, f.recording_type,
                   f.width, f.height, f.fps,
                   sub.start_time, sub.tier
            FROM (
                SELECT rid, start_time, tier
                FROM ({union_sql})
                ORDER BY tier ASC, start_time ASC
                LIMIT ?
            ) sub
            JOIN files f ON f.rowid = sub.rid
            ORDER BY sub.tier ASC, sub.start_time ASC
            """,
            params,
        ).fetchall()

    results = []
    for row in rows:
        # ``recording_type`` may be NULL on rows that pre-date the column
        # being populated (the probe-time backfill writes it inline now,
        # but very-old probed rows with NULL still exist).  Default to
        # 'continuous' to match the trigger's COALESCE.
        rtype = row["recording_type"] or "continuous"
        results.append(
            {
                "recording_id": row["recording_id"],
                "camera": row["camera"],
                "path": row["path"],
                "tier": int(row["tier"]),
                "recording_type": rtype,
                # Probe metadata for the worker's pre-flight check (skips
                # a per-file SELECT on the compress DB).
                "width": row["width"],
                "height": row["height"],
                "fps": row["fps"],
            }
        )
    return results


def _min_tier1_min_days(cfg: Config) -> int:
    """Smallest ``tier1.min_days`` across enabled cameras (default 7)."""
    days = [
        cam.tier1.min_days
        for cam in cfg.cameras.values()
        if cam.enabled and cam.tier1.enabled
    ]
    return min(days) if days else 7


def time_until_next_eligible(ctx: CompressorContext) -> float:
    """Seconds until the next recording becomes eligible for tier 1 or
    tier 2 compression.  Returns ``MAX_SLEEP_SEC`` if nothing is pending
    so the loop still re-checks periodically.

    Used only by the no-work sleep path — when there is eligible work,
    the loop sleeps the remainder of ``_THROTTLE_WINDOW_SEC`` instead.
    """
    cfg = ctx.cfg
    now = time.time()
    soonest = MAX_SLEEP_SEC

    min_t1_days = _min_tier1_min_days(cfg)
    t1_cutoff = now - (min_t1_days * 86400)
    with ctx.compress_db_lock:
        row = ctx.compress_db.execute(
            "SELECT start_time FROM frigate.recordings"
            " WHERE start_time > ? ORDER BY start_time ASC LIMIT 1",
            (t1_cutoff,),
        ).fetchone()
    if row is not None:
        soonest = min(soonest, row["start_time"] + min_t1_days * 86400 - now)

    min_t2_days = min(
        (
            cam.tier2.min_days
            for cam in cfg.cameras.values()
            if cam.enabled and cam.tier2.enabled
        ),
        default=None,
    )
    if min_t2_days is not None:
        t2_cutoff = now - (min_t2_days * 86400)
        with ctx.compress_db_lock:
            row = ctx.compress_db.execute(
                "SELECT start_time FROM frigate.recordings"
                " WHERE start_time > ? ORDER BY start_time ASC LIMIT 1",
                (t2_cutoff,),
            ).fetchone()
        if row is not None:
            soonest = min(soonest, row["start_time"] + min_t2_days * 86400 - now)

    return min(MAX_SLEEP_SEC, max(0.0, soonest))
