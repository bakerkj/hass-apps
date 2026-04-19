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
    """Build the SQL WHERE clause + params shared by eligible-recordings queries.

    ``effective_now`` is "now" for normal eligibility queries.  Callers can
    pass a future-shifted value to count recordings that *will* be eligible
    by that time (current backlog + incoming).

    Returns ("", []) if no enabled cameras have any enabled tier — caller must
    short-circuit (no rows can match).
    """
    _ok_statuses = (STATUS_OK, STATUS_SEGMENT_UPDATE_FAILED)
    ok_placeholders = ",".join("?" for _ in _ok_statuses)

    # A recording needs work if:
    #   - tier1 enabled AND old enough AND t1 not done, OR
    #   - tier2 enabled AND old enough AND t1 done AND t2 not done
    cam_clauses: list[str] = []
    params: list = []
    for name, cam in cfg.cameras.items():
        if not cam.enabled:
            continue
        subclauses = []
        sub_params: list = []
        if cam.tier1.enabled:
            t1_cutoff = effective_now - (cam.tier1.min_days * 86400)
            subclauses.append(
                f"(r.start_time < ? AND (f.t1_status IS NULL OR f.t1_status NOT IN ({ok_placeholders})))"
            )
            sub_params.extend([t1_cutoff, *_ok_statuses])
        if cam.tier2.enabled:
            t2_cutoff = effective_now - (cam.tier2.min_days * 86400)
            subclauses.append(
                f"(r.start_time < ? AND f.t1_status IN ({ok_placeholders}) AND (f.t2_status IS NULL OR f.t2_status NOT IN ({ok_placeholders})))"
            )
            sub_params.extend([t2_cutoff, *_ok_statuses, *_ok_statuses])
        if not subclauses:
            continue
        combined = " OR ".join(subclauses)
        cam_clauses.append(f"(r.camera = ? AND ({combined}))")
        params.append(name)
        params.extend(sub_params)

    if not cam_clauses:
        return "", []
    return " OR ".join(cam_clauses), params


def _open_eligible_conn(cfg: Config) -> sqlite3.Connection:
    """Open a read-only compress-db connection with frigate.recordings attached."""
    conn = sqlite3.connect(
        f"file:{cfg.compress_db}?mode=ro", uri=True, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    _attach_frigate_ro(conn, cfg, "frigate_eligible")
    return conn


def get_eligible_recordings(ctx: CompressorContext) -> list[dict]:
    """
    Returns up to ``_ELIGIBLE_BATCH_SIZE`` recordings eligible for compression
    that haven't been successfully compressed yet.  Each result dict has keys:
        recording_id, camera, path, tier, recording_type

    All filtering is done in SQL — camera, age, and tier completion status.
    """
    cfg = ctx.cfg
    where, params = _build_eligible_where(cfg, time.time())
    if not where:
        return []
    params.append(_ELIGIBLE_BATCH_SIZE)

    conn = _open_eligible_conn(cfg)
    try:
        rows = conn.execute(
            f"""
            SELECT r.id, r.camera, r.path, r.start_time,
                   r.motion, r.objects,
                   CASE WHEN f.t1_status IN ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                        THEN 2 ELSE 1 END AS tier
            FROM   frigate_eligible.recordings r
            LEFT JOIN files f ON f.recording_id = r.id
            WHERE  ({where})
            ORDER BY tier ASC, r.start_time ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    results = []
    for row in rows:
        rtype = _recording_type(row["motion"], row["objects"])
        results.append(
            {
                "recording_id": row["id"],
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
