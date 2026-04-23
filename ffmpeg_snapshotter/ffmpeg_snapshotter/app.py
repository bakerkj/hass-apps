# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point orchestration: config parsing, stream offsets, retention loop."""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time

from .config import StreamCfg, load_mqtt_config
from .mqtt import MqttPublisher
from .retention import apply_retention_count, apply_retention_days
from .stats import SnapshotStats
from .util import ensure_media_path, log, redact_url
from .worker import Worker


def _compute_stream_offsets(streams: list[dict]) -> dict[str, float]:
    """Evenly distribute start offsets for streams sharing an interval.

    Streams with the same ``interval_seconds`` get offsets
    ``0, interval/N, 2*interval/N, …`` so their first snapshots don't all
    fire at the same wall-clock second.
    """
    groups: dict[int, list[str]] = {}
    for s in streams:
        interval = int(s["interval_seconds"])
        groups.setdefault(interval, []).append(s["name"])

    offsets: dict[str, float] = {}
    for interval, names in groups.items():
        n = max(1, len(names))
        for i, name in enumerate(names):
            offsets[name] = (float(i) * float(interval)) / float(n)
    return offsets


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True, help="Path to HA add-on options.json")
    args = ap.parse_args()

    with open(args.options, "r", encoding="utf-8") as f:
        opts = json.load(f)

    log_level = (opts.get("log_level") or "INFO").upper()

    streams = opts.get("streams") or []
    if not streams:
        log("WARNING", "No streams configured. Exiting.")
        return 0

    ff = opts.get("ffmpeg") or {}
    ffmpeg_cfg: dict[str, str] = {
        "global_input_args": ff.get("global_input_args", "") or "",
        "global_hwaccel_args": ff.get("global_hwaccel_args", "") or "",
        "global_output_args": ff.get("global_output_args", "") or "",
    }

    hk = opts.get("housekeeping") or {}
    retention_interval = int(hk.get("retention_interval_seconds", 60) or 60)
    retention_interval = max(5, min(3600, retention_interval))
    last_retention = 0.0

    workers: dict[str, Worker] = {}
    cfgs: dict[str, StreamCfg] = {}

    for s in streams:
        cfg = StreamCfg(
            name=s["name"],
            url=s["url"],
            interval_seconds=int(s["interval_seconds"]),
            output_dir=ensure_media_path(s["output_dir"]),
            filename_format=s.get("filename_format") or "%Y%m%d-%H%M%S.jpg",
            date_dir_format=s.get("date_dir_format") or "%Y/%m/%d",
            latest_name=s.get("latest_name") or "latest.jpg",
            retain_count=int(s.get("retain_count") or 0),
            retain_days=int(s.get("retain_days") or 0),
            extra_input_args=s.get("extra_input_args") or "",
            extra_output_args=s.get("extra_output_args") or "",
        )
        if cfg.name in cfgs:
            log(
                "WARNING",
                f"Duplicate stream name '{cfg.name}' — skipping second definition",
            )
            continue
        cfgs[cfg.name] = cfg

    offsets = _compute_stream_offsets(streams)

    mqtt_cfg = load_mqtt_config(opts)
    stats_by_name: dict[str, SnapshotStats] = {}
    if mqtt_cfg.enabled:
        stats_by_name = {
            name: SnapshotStats(
                rate_window_seconds=mqtt_cfg.rate_window_seconds,
                error_timeout_seconds=mqtt_cfg.snapshot_error_timeout_seconds,
            )
            for name in cfgs
        }

    for name, cfg in cfgs.items():
        workers[name] = Worker(
            cfg,
            ffmpeg_cfg,
            log_level=log_level,
            start_offset_seconds=offsets.get(name, 0.0),
            stats=stats_by_name.get(name),
        )

    _cfg_lines = [
        "Configuration:",
        f"  global_hwaccel_args: {ffmpeg_cfg['global_hwaccel_args'] or '(none)'}",
        f"  global_input_args:   {ffmpeg_cfg['global_input_args'] or '(none)'}",
        f"  global_output_args:  {ffmpeg_cfg['global_output_args'] or '(none)'}",
        f"  log_level:           {log_level}",
        f"  retention_interval:  {retention_interval}s",
        f"  streams:             {len(cfgs)}",
    ]
    for _cfg in cfgs.values():
        _cfg_lines += [
            f"  stream [{_cfg.name}]:",
            f"    date_dir_format: {_cfg.date_dir_format}",
            f"    extra_input:     {_cfg.extra_input_args or '(none)'}",
            f"    extra_output:    {_cfg.extra_output_args or '(none)'}",
            f"    filename_format: {_cfg.filename_format}",
            f"    interval:        {_cfg.interval_seconds}s",
            f"    latest_name:     {_cfg.latest_name}",
            f"    output_dir:      {_cfg.output_dir}",
            f"    retain_count:    {_cfg.retain_count or '(none)'}",
            f"    retain_days:     {_cfg.retain_days or '(none)'}",
            f"    url:             {redact_url(str(_cfg.url))}",
        ]
    log("INFO", "\n".join(_cfg_lines))

    stopping = False
    mqtt_stopping = threading.Event()
    publisher: MqttPublisher | None = None

    def handle_sig(sig, frame):
        nonlocal stopping
        stopping = True
        log("INFO", f"Received signal {sig}, stopping workers...")
        for w in workers.values():
            w.stop(signal.SIGTERM)
        mqtt_stopping.set()

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    for w in workers.values():
        try:
            w.start()
        except Exception as e:
            log("ERROR", f"[{w.cfg.name}] failed to start: {e}")
            return 1

    if mqtt_cfg.enabled:
        publisher = MqttPublisher(mqtt_cfg, stats_by_name, mqtt_stopping)
        try:
            publisher.start()
            log("INFO", f"MQTT publisher started (host={mqtt_cfg.host})")
        except Exception as e:
            log("ERROR", f"MQTT publisher failed to start: {e}")
            publisher = None

    while True:
        now = time.time()
        if (now - last_retention) >= retention_interval:
            last_retention = now
            for cfg in cfgs.values():
                try:
                    apply_retention_days(
                        cfg.output_dir, cfg.retain_days, cfg.latest_name
                    )
                    apply_retention_count(
                        cfg.output_dir, cfg.retain_count, cfg.latest_name
                    )
                except Exception as e:
                    log("WARNING", f"[{cfg.name}] retention error: {e}")

        # Surface an MQTT watchdog exit (11/12) to the supervisor so the
        # add-on gets restarted like the frigate_compressor does.
        if publisher is not None and publisher.exit_code is not None:
            stopping = True
            mqtt_stopping.set()
            log("ERROR", f"MQTT watchdog triggered exit code {publisher.exit_code}")
            for w in workers.values():
                w.stop(signal.SIGTERM)
            publisher.stop()
            return publisher.exit_code

        if stopping:
            if publisher is not None:
                publisher.stop()
            time.sleep(1.0)
            return 0

        time.sleep(0.1)
