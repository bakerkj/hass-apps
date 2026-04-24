# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point: option parsing, initial apply, reconcile loop."""

from __future__ import annotations

import argparse
import logging
import signal
import time

from .config import (
    load_options,
    parse_bool,
    parse_host_process_targets,
    parse_process_targets,
    parse_targets,
)
from .docker import apply_all, cmd_error, run_cmd
from .process import apply_process_tunings


def main() -> int:
    stop = {"v": False}

    def _handle_sig(_sig: int, _frame: object) -> None:
        stop["v"] = True

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    parser = argparse.ArgumentParser()
    parser.add_argument("--options", default="/data/options.json")
    args = parser.parse_args()

    options = load_options(args.options)

    log_level = str(options.get("log_level", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("system_resource_tuner")

    interval_seconds = int(options.get("interval_seconds", 60))
    if interval_seconds < 5:
        interval_seconds = 5

    apply_on_start = parse_bool(options.get("apply_on_start", True), default=True)
    dry_run = parse_bool(options.get("dry_run", False), default=False)

    try:
        targets = parse_targets(options.get("targets"), log)
        process_targets = parse_process_targets(options.get("process_targets"), log)
        host_process_targets = parse_host_process_targets(
            options.get("host_process_targets"),
            log,
        )
    except Exception as e:
        log.error("Invalid configuration: %s", e)
        return 1

    if not targets and not process_targets and not host_process_targets:
        log.warning(
            "No valid tuning configured; running in idle mode (no changes will be applied)."
        )

    _cfg_lines = [
        "Configuration:",
        f"  apply_on_start:       {apply_on_start}",
        f"  dry_run:              {dry_run}",
        f"  interval:             {interval_seconds}s",
        f"  log_level:            {log_level}",
    ]
    if targets:
        _cfg_lines.append(f"  container_targets ({len(targets)}):")
        for _t in targets:
            _parts = []
            if _t.cpuset_cpus is not None:
                _parts.append(f"cpuset_cpus={_t.cpuset_cpus}")
            if _t.cpu_shares is not None:
                _parts.append(f"cpu_shares={_t.cpu_shares}")
            if _t.blkio_weight is not None:
                _parts.append(f"blkio_weight={_t.blkio_weight}")
            _cfg_lines.append(
                f"    [{_t.container}] {' '.join(_parts) or '(no params)'}"
            )
    if process_targets:
        _cfg_lines.append(f"  process_targets ({len(process_targets)}):")
        for _pt in process_targets:
            _parts = []
            if _pt.nice is not None:
                _parts.append(f"nice={_pt.nice}")
            if _pt.cpuset_cpus is not None:
                _parts.append(f"cpuset_cpus={_pt.cpuset_cpus}")
            _cfg_lines.append(
                f"    [{_pt.container or 'any'} | {_pt.process_match_regex or '(any)'}] {' '.join(_parts) or '(no params)'}"
            )
    if host_process_targets:
        _cfg_lines.append(f"  host_process_targets ({len(host_process_targets)}):")
        for _pt in host_process_targets:
            _parts = []
            if _pt.nice is not None:
                _parts.append(f"nice={_pt.nice}")
            if _pt.cpuset_cpus is not None:
                _parts.append(f"cpuset_cpus={_pt.cpuset_cpus}")
            _cfg_lines.append(
                f"    [{_pt.process_match_regex or '(any)'}] {' '.join(_parts) or '(no params)'}"
            )
    log.info("\n".join(_cfg_lines))

    proc = run_cmd(["docker", "info"])
    if proc.returncode != 0:
        err = cmd_error(proc).lower()
        if (
            "docker.sock" in err
            or "connect" in err
            or "permission denied" in err
            or "no such file" in err
        ):
            log.error(
                "Cannot connect to the Docker API at unix:///var/run/docker.sock. "
                "Disable Protection Mode for this addon in the Home Assistant UI "
                "(Addon → Info → Protection mode) and restart."
            )
        else:
            log.error("Docker API check failed: %s", cmd_error(proc))
        return 1

    if apply_on_start:
        apply_all(targets, dry_run, log)
        apply_process_tunings(process_targets, dry_run, log)
        apply_process_tunings(host_process_targets, dry_run, log)

    while not stop["v"]:
        time.sleep(interval_seconds)
        if stop["v"]:
            break
        apply_all(targets, dry_run, log)
        apply_process_tunings(process_targets, dry_run, log)
        apply_process_tunings(host_process_targets, dry_run, log)

    log.info("Shutting down")
    return 0
