# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Eligible-recordings query + ``time_until_next_eligible``."""

from __future__ import annotations

import sqlite3
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
            # ``INDEXED BY idx_files_t1_pending_age`` forces the planner to
            # range-scan the partial index — keyed on (camera, start_time)
            # WHERE t1 pending — instead of falling back to ``idx_files_camera``
            # and scanning every row for the camera.  When path and
            # recording_type were fetched via JOIN to Frigate's recordings,
            # the planner picked the partial index automatically; reading
            # those columns from ``files`` itself shifts the cost calculus
            # and the planner regresses to the camera index without the hint.
            #
            # Per-branch ORDER BY + LIMIT: each (camera, tier) contributes
            # at most ``_ELIGIBLE_BATCH_SIZE`` rows.  Without this, the
            # outer ``ORDER BY tier, start_time`` sees a UNION ALL spanning
            # all twelve branches and forces full materialisation of every
            # pending row.  ``ORDER BY start_time`` here matches the
            # partial index's order so SQLite can short-circuit at LIMIT
            # without an extra sort step.
            t1_parts.append(
                f"""
                SELECT * FROM (
                    SELECT f.recording_id, f.camera, f.path, f.recording_type,
                           f.start_time, 1 AS tier
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
                    SELECT f.recording_id, f.camera, f.path, f.recording_type,
                           f.start_time, 2 AS tier
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


def _open_eligible_conn(cfg: Config) -> sqlite3.Connection:
    """Open a read-only compress-db connection.

    The eligibility query is now self-contained on ``files`` (path and
    recording_type are denormalised onto it), so we no longer ATTACH
    Frigate.  Cache budget stays at 128 MB to comfortably hold the partial
    eligibility indexes' top levels and recent leaf pages.
    """
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA cache_size=-131072")
    return conn


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

    # In production ``ctx.eligibility_ro`` is a persistent conn opened at
    # startup; reuse it so the cache stays warm across iterations.  Tests
    # don't set it — fall back to opening a transient connection.
    opened_here = ctx.eligibility_ro is None
    conn = (
        ctx.eligibility_ro
        if ctx.eligibility_ro is not None
        else _open_eligible_conn(cfg)
    )
    try:
        rows = conn.execute(
            f"""
            SELECT recording_id, camera, path, recording_type,
                   start_time, tier
            FROM ({union_sql})
            ORDER BY tier ASC, start_time ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        if opened_here:
            conn.close()

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
    row = ctx.frigate_ro.execute(
        "SELECT start_time FROM recordings"
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
        row = ctx.frigate_ro.execute(
            "SELECT start_time FROM recordings"
            " WHERE start_time > ? ORDER BY start_time ASC LIMIT 1",
            (t2_cutoff,),
        ).fetchone()
        if row is not None:
            soonest = min(soonest, row["start_time"] + min_t2_days * 86400 - now)

    return min(MAX_SLEEP_SEC, max(0.0, soonest))
