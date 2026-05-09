# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Background loop for tier-2 sibling-swap work.

Swaps are rename + unlink + a small DB UPDATE — no GPU, no ffmpeg.
Running them on the encode thread pool would let them compete for worker
slots with the heavy encodes; running them in their own thread keeps the
two tracks independent and lets each drain at its own bottleneck (GPU vs
filesystem).  The cap on ``_MAX_SWAPS_PER_WINDOW`` keeps the shared
``compress_db`` lock from being held in a tight burst.
"""

from __future__ import annotations

import threading
import time
from collections import Counter

from .compressor import swap_t2
from .context import CompressorContext
from .database import (
    STATUS_DIRECT,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
)
from .throttle import RateLimiter
from .util import log

# Poll cadence — matches the encode loop.  The rate limiter inside the
# loop spreads up to ``_MAX_SWAPS_PER_WINDOW`` swaps evenly across this.
_SWAP_WINDOW_SEC = 60.0

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
    parts: list[str] = []
    params: list = []
    now = time.time()
    for name, cam in cfg.cameras.items():
        if not cam.enabled or not cam.tier2.enabled:
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


def run_swap_loop(
    ctx: CompressorContext, stopping: threading.Event, encoder: str
) -> None:
    """Continuously drain pending sibling swaps.

    Mirrors ``run_probe_loop``'s rhythm: poll eligibility, set the rate
    limiter target to the batch size (so the swaps are spread evenly
    across the window), then drain serially.  ``swap_t2`` does the
    rename + unlink + DB UPDATE for one row.

    The ``encoder`` arg is forwarded to ``swap_t2`` (it's recorded on
    the t2 status row for stats parity with chained re-encodes).
    """
    # Persistent local RateLimiter — same idiom as ``run_probe_loop``.
    # Reusing the instance across iterations means ``next_allowed``
    # carries over, so iter N+1's first acquire waits for iter N's last
    # slot to elapse (no explicit window-fill needed).
    limiter = RateLimiter()

    while not stopping.is_set():
        try:
            eligible = get_eligible_swaps(ctx)
        except Exception as e:
            log("ERROR", f"Swap loop: failed to query eligible swaps: {e}")
            stopping.wait(timeout=_SWAP_WINDOW_SEC)
            continue

        if not eligible:
            log("DEBUG", "Swap loop idle — no eligible swaps")
            stopping.wait(timeout=_SWAP_WINDOW_SEC)
            continue

        # Batch announce — same shape as the encode loop's
        # ``Compressing N: cam=X, cam2=Y[ (DRY RUN — ...)]`` line.
        suffix = " (DRY RUN — skipping rename)" if ctx.cfg.all_dry_run else ""
        camera_counts = Counter(r["camera"] for r in eligible)
        breakdown = ", ".join(f"{cam}={n}" for cam, n in sorted(camera_counts.items()))
        log("INFO", f"Swapping {len(eligible)}: {breakdown}{suffix}")
        # Spread the batch across the window: target = len(eligible) per
        # 60s.  When eligible == _MAX_SWAPS_PER_WINDOW the cadence is
        # one swap every 60ms, well under the FS rename + lock budget.
        limiter.set_target(len(eligible))

        for r in eligible:
            if stopping.is_set():
                break
            limiter.acquire(stopping)
            try:
                swap_t2(
                    r["recording_id"],
                    r["path"],
                    r["camera"],
                    r["recording_type"],
                    encoder,
                    ctx,
                )
            except Exception as e:
                log("ERROR", f"[{r['camera']}] swap failed: {e}")
