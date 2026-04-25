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
    _attach_frigate_ro,
    _recording_type,
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
    """
    _ok_statuses = (STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED)
    ok_placeholders = ",".join("?" for _ in _ok_statuses)

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
                SELECT f.recording_id, f.camera, f.start_time, 1 AS tier
                FROM files f
                WHERE f.camera = ? AND f.start_time < ?
                  AND (f.t1_status IS NULL
                       OR f.t1_status NOT IN ({ok_placeholders}))
                """
            )
            t1_params.extend([name, t1_cutoff, *_ok_statuses])
        if cam.tier2.enabled:
            t2_cutoff = effective_now - (cam.tier2.min_days * 86400)
            t2_parts.append(
                f"""
                SELECT f.recording_id, f.camera, f.start_time, 2 AS tier
                FROM files f
                WHERE f.camera = ? AND f.start_time < ?
                  AND f.t1_status IN ({ok_placeholders})
                  AND (f.t2_status IS NULL
                       OR f.t2_status NOT IN ({ok_placeholders}))
                """
            )
            t2_params.extend([name, t2_cutoff, *_ok_statuses, *_ok_statuses])

    parts = t1_parts + t2_parts
    if not parts:
        return "", []
    # Combined UNION ALL over both tiers; the caller wraps this and
    # joins back to Frigate to fetch motion/objects/path.
    return " UNION ALL ".join(parts), t1_params + t2_params


def _open_eligible_conn(cfg: Config) -> sqlite3.Connection:
    """Open a read-only compress-db connection with frigate.recordings attached."""
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    # The eligibility query is a single statement but a heavy one —
    # UNION ALL over up to 12 camera/tier partial-index branches joined
    # back to Frigate's recordings PK for each hit.  A bigger cache
    # prevents mid-statement page eviction when the access pattern walks
    # many Frigate PK pages in one go.
    conn.execute("PRAGMA cache_size=-131072")
    _attach_frigate_ro(conn, cfg, "frigate_eligible")
    return conn


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns up to ``_ELIGIBLE_BATCH_SIZE`` recordings eligible for compression
    that haven't been successfully compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    Driven from the partial eligibility indexes on ``files`` — scans only
    the pending rows per camera, range-seeking on start_time.  Benchmarked
    at 4000×+ faster than the previous recordings-driven LEFT JOIN.
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
            SELECT sub.recording_id, sub.camera, r.path,
                   sub.start_time, r.motion, r.objects, sub.tier
            FROM ({union_sql}) sub
            JOIN frigate_eligible.recordings r ON r.id = sub.recording_id
            ORDER BY sub.tier ASC, sub.start_time ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        if opened_here:
            conn.close()

    results = []
    for row in rows:
        rtype = _recording_type(row["motion"], row["objects"])
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
