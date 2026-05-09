# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Eligible-swaps query for ``run_swap_loop``.

Sibling-swap work is what tier-2 looks like under
``tier2.source='direct'``: the tier-1 encode produced a parked
``.t2.mp4`` next to the primary, and at ``tier2.min_days`` we just
rename the sibling onto the primary path (no encode, no GPU).  These
rows surface here, separately from the encode loop's eligibility, so
they drain on their own thread at filesystem pace instead of competing
with GPU work for worker slots.
"""

from __future__ import annotations

import time

from .context import CompressorContext
from .database import (
    STATUS_DIRECT,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
)

# Upper bound on swaps started per window.  Each swap takes the shared
# ``compress_db_lock`` briefly for the t2 status UPDATE; capping the rate
# keeps that lock from competing with probe-loop / encode-worker writes
# in a tight burst.  At 500/min ≈ 8/sec, the lock is held a few ms per
# tick — well below saturation while comfortably above the daemon's
# steady-state direct-encode intake.
_MAX_SWAPS_PER_WINDOW = 500


def get_eligible_swaps(ctx: CompressorContext) -> list[dict]:
    """Tier-2 rows whose sibling ``.t2.mp4`` is parked and ready to swap.

    Surfaces up to ``_MAX_SWAPS_PER_WINDOW`` rows — the cap is the swap
    loop's per-window throttle, so an unbounded SELECT would let the
    rate limiter dribble work out for many minutes after a backlog
    spike.  Within the cap, oldest-first by ``start_time`` so the
    longest-parked siblings get freed first.

    Driven entirely from the partial eligibility index on ``files``;
    never touches Frigate's recordings table.
    """
    cfg = ctx.cfg
    # Suppress per-camera dry-run cameras when others are running live
    # (their direct rows would be returned every cycle without ever being
    # marked done, since ``swap_t2``'s dry-run path logs and returns
    # without writing status).  See ``encode_eligibility`` for the same
    # reasoning on the encode side.
    suppress_dry_run = not cfg.all_dry_run
    parts: list[str] = []
    params: list = []
    now = time.time()
    for name, cam in cfg.cameras.items():
        if not cam.enabled or not cam.tier2.enabled:
            continue
        if suppress_dry_run and cam.dry_run:
            continue
        cutoff = now - (cam.tier2.min_days * 86400)
        # Same partial index (idx_files_t2_pending_age) the encode-loop
        # eligibility uses.  The added ``t2_status = 'direct'`` filter is
        # an equality check on the already-tiny pending subset.
        parts.append(
            f"""
            SELECT * FROM (
                SELECT f.rowid AS rid, f.start_time
                FROM files f
                WHERE f.camera = ? AND f.start_time < ?
                  AND f.t1_status IN
                      ('{STATUS_OK}', '{STATUS_SEGMENT_UPDATE_FAILED}')
                  AND f.t2_status = '{STATUS_DIRECT}'
                ORDER BY f.start_time ASC
                LIMIT {_MAX_SWAPS_PER_WINDOW}
            )
            """
        )
        params.extend([name, cutoff])

    if not parts:
        return []

    union_sql = " UNION ALL ".join(parts)
    params.append(_MAX_SWAPS_PER_WINDOW)

    with ctx.compress_db_lock:
        rows = ctx.compress_db.execute(
            f"""
            SELECT f.recording_id, f.camera, f.path, f.recording_type,
                   sub.start_time
            FROM (
                SELECT rid, start_time
                FROM ({union_sql})
                ORDER BY start_time ASC
                LIMIT ?
            ) sub
            JOIN files f ON f.rowid = sub.rid
            ORDER BY sub.start_time ASC
            """,
            params,
        ).fetchall()

    return [
        {
            "recording_id": row["recording_id"],
            "camera": row["camera"],
            "path": row["path"],
            "recording_type": row["recording_type"] or "continuous",
        }
        for row in rows
    ]
