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
from .compressor import compress_one, init_worker_connections
from .config import (
    Config,
    TypeSettings,
    _RECORDING_TYPES,
    _fmt_type,
    load_config,
)
from .context import CompressorContext
from .database import (
    backfill_files_start_time,
    check_frigate_schema,
    open_compress_db,
    open_frigate_db,
    open_frigate_db_rw,
    vacuum_compress_db,
)
from .eligibility import (
    _open_eligible_conn,
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
    frigate_ro = open_frigate_db(cfg.frigate_db)
    frigate_rw = open_frigate_db_rw(cfg.frigate_db)

    try:
        check_frigate_schema(frigate_ro)
    except RuntimeError as e:
        log("ERROR", f"Startup aborted: {e}")
        return 1

    # Backfill files.start_time from Frigate's recordings for any rows
    # that pre-date the column being added.  New rows get start_time
    # written inline by the probe loop, so this is a one-time cost on
    # the first run after the schema upgrade.  When backfill actually
    # runs (n > 0), follow with VACUUM to reclaim space from both the
    # schema migration in open_compress_db AND the row-rewrite churn
    # of this UPDATE pass — that's the true end of the upgrade.
    try:
        n_backfilled = backfill_files_start_time(compress_db, cfg)
        if n_backfilled:
            log(
                "INFO",
                f"Backfilled files.start_time for {n_backfilled} pre-existing rows",
            )
            try:
                vacuum_compress_db(compress_db)
            except Exception as e:
                log("WARNING", f"VACUUM failed (non-fatal): {e}")
    except Exception as e:
        log("WARNING", f"files.start_time backfill failed (non-fatal): {e}")

    # The main-thread compress_db is only used for startup migrations +
    # backfill + VACUUM (above) and is idle for the rest of the daemon's
    # life.  Close it now so the 128 MB cache cap + fd don't sit unused
    # for days.  Workers, probe loop, eligibility, and MQTT each own
    # their own persistent connections.
    compress_db.close()

    log("INFO", "════════════════════════════════════════")
    log("INFO", f"Frigate Compressor v{__version__} starting")
    if cfg.all_dry_run:
        log("INFO", "  *** DRY RUN MODE — no files or databases will be modified ***")
    log("INFO", f"  Encoder        : {encoder}")
    log("INFO", f"  Parallel jobs  : {cfg.max_parallel_jobs}")
    log("INFO", f"  Log level      : {cfg.log_level}")
    log("INFO", f"  Housekeeping   : every {cfg.housekeeping_interval_days}d")
    log("INFO", "  Throttle       : auto (target = pending work/min)")
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

    # Persistent ro connection for the main loop's eligibility query —
    # opened once and reused so its page cache stays warm across the
    # every-60s query.  Has Frigate attached as ``frigate_eligible``
    # so the UNION-ALL + join query can range-scan our partial indexes
    # and then PK-look up paths from Frigate without re-attaching.
    eligibility_ro = _open_eligible_conn(cfg)

    ctx = CompressorContext(
        cfg=cfg,
        frigate_ro=frigate_ro,
        frigate_rw=frigate_rw,
        eligibility_ro=eligibility_ro,
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

    # Persistent worker pool: one set of DB connections per worker thread
    # for the daemon's lifetime, opened by ``init_worker_connections`` on
    # thread start.  Avoids ~210 sqlite3.connect()/close() cycles per
    # minute when running at full compression rate.
    pool = ThreadPoolExecutor(
        max_workers=cfg.max_parallel_jobs,
        initializer=init_worker_connections,
        initargs=(cfg,),
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
        eligibility_ro.close()
        frigate_ro.close()
        frigate_rw.close()

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
    """
    ctx.rate_limiter.acquire(stopping)
    return compress_one(rid, path, camera, tier, rtype, encoder, ctx)


def run_main_loop(
    ctx: CompressorContext,
    encoder: str,
    stopping: threading.Event,
    housekeeping_interval_sec: float,
    pool: ThreadPoolExecutor,
) -> None:
    """Process eligible recordings forever, sleeping only when caught up.

    Extracted from ``main()`` so the loop's scheduling behavior (run-then-
    re-check vs sleep-until-next) is testable in isolation.  ``pool`` is
    a long-lived ``ThreadPoolExecutor`` whose workers carry their own
    per-thread DB connections — see ``init_worker_connections`` in
    ``compressor.py``.
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
