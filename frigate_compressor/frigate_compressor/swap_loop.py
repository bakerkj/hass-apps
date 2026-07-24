# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Background loop for tier-2 sibling-swap work.

Swaps are rename + unlink + a small DB UPDATE — no GPU, no ffmpeg.
Running them on the encode thread pool would let them compete for worker
slots with the heavy encodes; running them in their own thread keeps the
two tracks independent and lets each drain at its own bottleneck (GPU vs
filesystem).  The eligibility query and its per-window cap live in
``swap_eligibility``.
"""

import threading
import traceback
from collections import Counter

from .compressor import swap_t2
from .context import CompressorContext
from .swap_eligibility import get_eligible_swaps
from .throttle import RateLimiter
from .util import log

# Poll cadence — matches the encode loop.  The rate limiter inside the
# loop spreads the eligible-swap batch evenly across this.
_SWAP_WINDOW_SEC = 60.0


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
        except Exception as e:  # noqa: BLE001 — supervisor loop must survive
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
        # 60s.  When eligible == _MAX_SWAPS_PER_WINDOW (its eligibility
        # cap) the cadence is one swap every 60ms, well under the FS
        # rename + lock budget.
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
            except Exception as e:  # noqa: BLE001 — supervisor loop must survive per-row failures
                # Include the traceback so post-rename failures (e.g.
                # primary holds tier-2 content but the t2 status write
                # never landed) leave a forensic trail — without it,
                # silent orphan-state bugs are hard to diagnose later.
                log(
                    "ERROR",
                    f"[{r['camera']}] swap failed: {e}\n{traceback.format_exc()}",
                )
