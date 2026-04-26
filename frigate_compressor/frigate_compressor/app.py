# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Daemon entrypoint: ``main()`` and the top-level scheduling loop."""

from __future__ import annotations

import argparse
import signal
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import __version__
from .compressor import compress_one
from .config import (
    Config,
    TypeSettings,
    _RECORDING_TYPES,
    _fmt_type,
    load_config,
)
from .context import CompressorContext
from .database import (
    _attach_frigate,
    check_frigate_schema_attached,
    open_compress_db,
)
from .eligibility import (
    get_eligible_recordings,
    time_until_next_eligible,
)
from .ffmpeg import check_encoder_works, detect_encoder
from .housekeeping import run_housekeeping
from .mqtt import MqttPublisher
from .probe_loop import run_probe_loop
from .throttle import MAX_SLEEP_SEC, _THROTTLE_WINDOW_SEC
from .util import log, set_log_level


def _warn_qsv_fps_conflicts(cfg: Config, encoder: str) -> None:
    """
    Emit one WARNING per (camera, tier, recording_type) combination where QSV
    encoding is active alongside both an fps filter and a scale filter.
    Mixed CPU/GPU filter chains can cause FFmpeg to fail with a cryptic
    'Error while filtering' message.  Called once at startup to inform the
    user without spamming a warning for every compressed recording.
    """
    if encoder != "qsv":
        return

    for cam_name, cam_cfg in cfg.cameras.items():
        if not cam_cfg.enabled:
            continue
        for tier_num, tier_cfg in ((1, cam_cfg.tier1), (2, cam_cfg.tier2)):
            if not tier_cfg.enabled:
                continue
            for rtype in _RECORDING_TYPES:
                ts: TypeSettings = getattr(tier_cfg, rtype)
                if ts.fps_mode != "none" and ts.scale_mode != "none":
                    log(
                        "WARNING",
                        f"Config [{cam_name} tier{tier_num}/{rtype}]: QSV encoder"
                        f" + fps_mode='{ts.fps_mode}' + scale_mode='{ts.scale_mode}'"
                        " — mixed CPU/GPU filter chain may cause FFmpeg to fail."
                        " Consider fps_mode='none' with QSV, or encoder='cpu'.",
                    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True)
    args = ap.parse_args()

    cfg = load_config(args.options)
    set_log_level(cfg.log_level)

    encoder = detect_encoder(cfg.encoder)
    _warn_qsv_fps_conflicts(cfg, encoder)

    encoder_ok, encoder_msg = check_encoder_works(encoder)
    if encoder_ok:
        log("INFO", f"Encoder self-test: {encoder} OK")
    elif cfg.all_dry_run:
        log(
            "WARNING",
            f"Encoder self-test: {encoder} FAILED — {encoder_msg}. "
            "Continuing because all cameras are dry_run, but real compression "
            "would fail on every file.",
        )
    else:
        log("ERROR", f"Encoder self-test: {encoder} FAILED — {encoder_msg}")
        log("ERROR", "Hardware acceleration is not available. Aborting startup.")
        return 1

    compress_db = open_compress_db(cfg.compress_db)
    _attach_frigate(compress_db, cfg, "frigate")

    try:
        check_frigate_schema_attached(compress_db)
    except RuntimeError as e:
        log("ERROR", f"Startup aborted: {e}")
        compress_db.close()
        return 1

    log("INFO", "════════════════════════════════════════")
    log("INFO", f"Frigate Compressor v{__version__} starting")
    if cfg.all_dry_run:
        log("INFO", "  *** DRY RUN MODE — no files or databases will be modified ***")
    log("INFO", f"  Encoder        : {encoder}")
    log("INFO", f"  Parallel jobs  : {cfg.max_parallel_jobs}")
    log("INFO", f"  Log level      : {cfg.log_level}")
    log("INFO", f"  Housekeeping   : every {cfg.housekeeping_interval_days}d")
    log("INFO", "  Throttle       : auto (target = pending work/min)")
    log(
        "INFO",
        f"  ffmpeg mode    : {'detached (init-orphan)' if cfg.detached_ffmpeg else 'inline subprocess.run'}",
    )
    log("INFO", f"  Frigate DB     : {cfg.frigate_db}")
    log("INFO", f"  Recordings     : {cfg.recordings_dir}")
    log("INFO", f"  Compress DB    : {cfg.compress_db}")
    if cfg.mqtt.enabled:
        log(
            "INFO",
            f"  MQTT           : {cfg.mqtt.host}:{cfg.mqtt.port}"
            f" base={cfg.mqtt.base_topic}"
            f" interval={cfg.mqtt.publish_interval_seconds}s"
            f" rate_window={cfg.mqtt.rate_window_seconds}s"
            f" disconnect_timeout={cfg.mqtt.disconnect_timeout_seconds}s",
        )
    else:
        log("INFO", "  MQTT           : disabled (mqtt_host empty)")
    log("INFO", f"  Cameras        : {len(cfg.cameras)}")
    for cam_name, cam_cfg in cfg.cameras.items():
        flags = []
        if not cam_cfg.enabled:
            flags.append("DISABLED")
        if cam_cfg.dry_run:
            flags.append("DRY_RUN")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        log("INFO", f"  ── {cam_name}{flag_str}")
        for tier_num, tier_cfg in ((1, cam_cfg.tier1), (2, cam_cfg.tier2)):
            tier_flag = "" if tier_cfg.enabled else " [DISABLED]"
            log("INFO", f"      Tier {tier_num} (>{tier_cfg.min_days}d){tier_flag}:")
            for rtype in _RECORDING_TYPES:
                ts = getattr(tier_cfg, rtype)
                log("INFO", f"        {rtype:<12}: {_fmt_type(ts)}")
    log("INFO", "════════════════════════════════════════")

    ctx = CompressorContext(
        cfg=cfg,
        compress_db=compress_db,
    )

    # Use threading.Event so signal handlers can wake the sleep loop immediately.
    stopping = threading.Event()
    housekeeping_interval_sec = cfg.housekeeping_interval_days * 86400

    def handle_sig(_sig, _frame):
        stopping.set()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    probe_thread = threading.Thread(
        target=run_probe_loop, args=(ctx, stopping), daemon=True
    )
    probe_thread.start()

    publisher: MqttPublisher | None = None
    if cfg.mqtt.enabled:
        try:
            publisher = MqttPublisher(ctx, cfg.mqtt, stopping)
            publisher.start()
        except Exception as e:
            log("ERROR", f"Failed to start MQTT publisher: {e}")
            publisher = None

    # Worker pool: workers no longer hold per-thread connections — they all
    # use ``ctx.compress_db`` under ``ctx.compress_db_lock``.  ffmpeg
    # subprocesses still run concurrently; only the brief DB ops at the
    # start (probe-data SELECT) and end (record + segment_size update) of
    # each compression serialise through the lock.
    pool = ThreadPoolExecutor(
        max_workers=cfg.max_parallel_jobs,
        thread_name_prefix="compress",
    )

    try:
        run_main_loop(ctx, encoder, stopping, housekeeping_interval_sec, pool)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        if publisher is not None:
            try:
                publisher.stop()
            except Exception as e:
                log("WARNING", f"MQTT publisher stop failed: {e}")
        log("INFO", "Frigate Compressor stopped")
        compress_db.close()

    if publisher is not None and publisher.exit_code is not None:
        return publisher.exit_code
    return 0


def _pace_then_compress(
    stopping: threading.Event,
    rid: str,
    path: str,
    camera: str,
    tier: int,
    rtype: str,
    encoder: str,
    ctx: CompressorContext,
    probe_data: dict | None = None,
) -> bool:
    """Worker wrapper: pace via the shared rate limiter, then run compress_one.

    Pacing happens *before* compression so that every file *start* is gated
    by the limiter — including the first ones in a batch.  Pacing after
    compression would let the first ``max_parallel_jobs`` workers in a
    batch each fire one compress with no spacing, producing the bursty
    "process N files in 5s, then idle for 7s" pattern.

    The limiter reads its current target from its own field (set by the
    main loop at the top of each iteration), so the worker doesn't need
    to know it.

    ``probe_data`` is forwarded to ``compress_one`` so the worker can
    skip its per-file ``SELECT width, height, fps`` — the eligibility
    query already fetched those columns.
    """
    ctx.rate_limiter.acquire(stopping)
    return compress_one(
        rid, path, camera, tier, rtype, encoder, ctx, probe_data=probe_data
    )


def run_main_loop(
    ctx: CompressorContext,
    encoder: str,
    stopping: threading.Event,
    housekeeping_interval_sec: float,
    pool: ThreadPoolExecutor,
) -> None:
    """Process eligible recordings forever, sleeping only when caught up.

    Extracted from ``main()`` so the loop's scheduling behavior (run-then-
    re-check vs sleep-until-next) is testable in isolation.
    """
    cfg = ctx.cfg
    last_housekeeping = time.time()

    while not stopping.is_set():
        # Each iteration aims to take exactly ``_THROTTLE_WINDOW_SEC``
        # wall-clock — sleep at the end is whatever's left over.
        iter_start = time.time()

        # ── Housekeeping ──────────────────────────────────────────────────
        if (iter_start - last_housekeeping) >= housekeeping_interval_sec:
            try:
                run_housekeeping(ctx)
            except Exception as e:
                log("ERROR", f"Housekeeping failed: {e}")
            last_housekeeping = time.time()

        # ── Find eligible recordings ──────────────────────────────────────
        try:
            eligible = get_eligible_recordings(ctx)
        except Exception as e:
            log("ERROR", f"Failed to query eligible recordings: {e}")
            stopping.wait(timeout=60)
            continue

        if eligible:
            # Throttle target = exactly the work in this batch over the
            # next minute.  No separate workload measurement, no lookahead
            # — pace just the files we actually have.
            ctx.rate_limiter.set_target(len(eligible))

            suffix = " (DRY RUN — skipping ffmpeg)" if cfg.all_dry_run else ""
            camera_counts = Counter(r["camera"] for r in eligible)
            breakdown = ", ".join(
                f"{cam}={n}" for cam, n in sorted(camera_counts.items())
            )
            log("INFO", f"Compressing {len(eligible)}: {breakdown}{suffix}")

            futures = {
                pool.submit(
                    _pace_then_compress,
                    stopping,
                    r["recording_id"],
                    r["path"],
                    r["camera"],
                    r["tier"],
                    r["recording_type"],
                    encoder,
                    ctx,
                    # Tests that drive the main loop with hand-built eligible
                    # dicts may omit width/height/fps; ``.get`` keeps the
                    # worker on its SELECT-fallback path in that case.
                    {
                        "width": r.get("width"),
                        "height": r.get("height"),
                        "fps": r.get("fps"),
                    },
                ): r
                for r in eligible
                if not stopping.is_set()
            }
            for future in as_completed(futures):
                if stopping.is_set():
                    # Cancel anything still pending so we exit promptly;
                    # leave the pool itself open — main() shuts it down.
                    for f in futures:
                        f.cancel()
                    break
                r = futures[future]
                try:
                    future.result()
                except Exception as e:
                    log("ERROR", f"[{r['camera']}] unhandled error: {e}")

        # ── Sleep until the next iteration ───────────────────────────────
        # Three cases:
        #   1. Work was processed: sleep the remainder of the window so
        #      total iteration = one window.  If processing overran the
        #      window (catchup: batch outran capacity), don't sleep.
        #   2. No work: sleep until the next recording becomes eligible
        #      PLUS one full window — so when we wake there's ~one
        #      window of accumulated work to process as a proper batch,
        #      not a lone first-eligible file.  Capped at MAX_SLEEP_SEC
        #      so pathological states still re-check every 10 minutes.
        #   3. Dry-run with work: recordings are never marked done, so
        #      we force a full window sleep to avoid looping instantly
        #      on the same un-marked files.
        if eligible and cfg.all_dry_run:
            sleep_sec = _THROTTLE_WINDOW_SEC
        elif eligible:
            sleep_sec = max(0.0, _THROTTLE_WINDOW_SEC - (time.time() - iter_start))
        else:
            try:
                sleep_sec = min(
                    time_until_next_eligible(ctx) + _THROTTLE_WINDOW_SEC,
                    MAX_SLEEP_SEC,
                )
            except Exception as e:
                log("WARNING", f"time_until_next_eligible failed: {e}")
                sleep_sec = MAX_SLEEP_SEC
        if sleep_sec > 0:
            stopping.wait(timeout=sleep_sec)
