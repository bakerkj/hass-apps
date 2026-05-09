# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Eligible-recordings query + ``time_until_next_eligible``."""

from __future__ import annotations

import time

from .config import Config
from .context import CompressorContext
from .database import (
    STATUS_DIRECT,
    STATUS_GIVE_UP,
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
    placeholders, the implication check fails and the plan falls back
    to a full files SCAN + temp-btree sort.
    """
    # ISO timestamp used to gate retry-backoff: rows whose
    # ``next_retry_at`` is in the future are excluded from this batch.
    # Computed once per query so every branch sees the same "now".
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(effective_now))
    t1_parts: list[str] = []
    t1_params: list = []
    t2_parts: list[str] = []
    t2_params: list = []
    for name, cam in cfg.cameras.items():
        if not cam.enabled:
            continue
        if cam.tier1.enabled:
            t1_cutoff = effective_now - (cam.tier1.min_days * 86400)
            t1_parts.append(
                f"""
                SELECT * FROM (
                    SELECT f.rowid AS rid, f.start_time, 1 AS tier
                    FROM files f
                    WHERE f.camera = ? AND f.start_time < ?
                      AND (f.t1_status IS NULL
                           OR f.t1_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}',
                               '{STATUS_GIVE_UP}'))
                      AND (f.t1_next_retry_at IS NULL
                           OR f.t1_next_retry_at <= ?)
                    ORDER BY f.start_time ASC
                    LIMIT {_ELIGIBLE_BATCH_SIZE}
                )
                """
            )
            t1_params.extend([name, t1_cutoff, now_iso])
        if cam.tier2.enabled:
            t2_cutoff = effective_now - (cam.tier2.min_days * 86400)
            # STATUS_DIRECT rows are excluded here — they're sibling-swap
            # work (rename + unlink, no GPU) and surface through
            # ``swap_loop.get_eligible_swaps`` instead.  This branch only
            # surfaces tier-2 rows that need a CHAINED re-encode of an
            # existing tier-1 file (t2_status NULL or error-retry), which
            # is GPU work and belongs alongside tier-1 in the main loop.
            t2_parts.append(
                f"""
                SELECT * FROM (
                    SELECT f.rowid AS rid, f.start_time, 2 AS tier
                    FROM files f
                    WHERE f.camera = ? AND f.start_time < ?
                      AND f.t1_status IN
                          ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                      AND (f.t2_status IS NULL
                           OR f.t2_status NOT IN
                              ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}',
                               '{STATUS_DIRECT}', '{STATUS_GIVE_UP}'))
                      AND (f.t2_next_retry_at IS NULL
                           OR f.t2_next_retry_at <= ?)
                    ORDER BY f.start_time ASC
                    LIMIT {_ELIGIBLE_BATCH_SIZE}
                )
                """
            )
            t2_params.extend([name, t2_cutoff, now_iso])

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
    so this query never touches Frigate's recordings table.

    Ordering is auto-tuned per call.  When the eligible-row count
    exceeds one batch (catch-up), results are sorted ``tier ASC,
    start_time ASC`` so the heavier tier-1 encodes drain ahead of
    tier-2 chained re-encodes.  When the count fits in one batch
    (steady state), results are interleaved by per-tier rank so the
    encoder sees ``t1[0], t2[0], t1[1], t2[1], …`` — pure ``start_time``
    order doesn't work because the two tiers' eligible rows never overlap
    in time (tier-1 is ~min_days old; tier-2 chained is ~tier2.min_days
    old).
    """
    cfg = ctx.cfg
    union_sql, params = _build_eligible_where(cfg, time.time())
    if not union_sql:
        return []
    # Two window columns drive the ordering:
    #
    #  * ``n = COUNT(*) OVER ()`` — total eligible rows in this batch.
    #    When ``n > _ELIGIBLE_BATCH_SIZE`` we have more work than one
    #    batch can carry (catch-up), and the heavy tier-0→1 encodes
    #    should drain ahead of tier-1→2 chained re-encodes.
    #  * ``rk = ROW_NUMBER() OVER (PARTITION BY tier ORDER BY start_time)``
    #    — within-tier rank by age.  Used to interleave tiers in steady
    #    state.  Pure ``start_time ASC`` doesn't interleave because the
    #    two tiers' eligible rows never overlap in time (tier-1 rows are
    #    ~tier1.min_days old; tier-2 chained rows are ~tier2.min_days old).
    #
    # The CASE on n switches between the two regimes:
    #  * Catch-up (n > batch): CASE → tier, primary sort tier ASC.  rk is
    #    secondary and within tier ordered by start_time, so the result
    #    is "all tier-1 oldest-first, then all tier-2 oldest-first".
    #  * Steady state (n ≤ batch): CASE → NULL for every row, primary key
    #    ties, falls through to rk ASC.  Result is rk=1 tier-1, rk=1
    #    tier-2, rk=2 tier-1, rk=2 tier-2, … — true alternation, with
    #    one tier draining alone once the other runs out.
    #
    # (Sibling-swap work is handled by ``swap_loop`` on its own thread.)
    params.extend([_ELIGIBLE_BATCH_SIZE, _ELIGIBLE_BATCH_SIZE, _ELIGIBLE_BATCH_SIZE])

    with ctx.compress_db_lock:
        rows = ctx.compress_db.execute(
            f"""
            SELECT f.recording_id, f.camera, f.path, f.recording_type,
                   f.width, f.height, f.fps,
                   sub.start_time, sub.tier
            FROM (
                SELECT rid, start_time, tier, n, rk
                FROM (
                    SELECT rid, start_time, tier,
                           COUNT(*) OVER () AS n,
                           ROW_NUMBER() OVER (
                               PARTITION BY tier ORDER BY start_time ASC
                           ) AS rk
                    FROM ({union_sql})
                )
                ORDER BY
                    CASE WHEN n > ? THEN tier END ASC,
                    rk ASC,
                    tier ASC,
                    start_time ASC
                LIMIT ?
            ) sub
            JOIN files f ON f.rowid = sub.rid
            ORDER BY
                CASE WHEN sub.n > ? THEN sub.tier END ASC,
                sub.rk ASC,
                sub.tier ASC,
                sub.start_time ASC
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
