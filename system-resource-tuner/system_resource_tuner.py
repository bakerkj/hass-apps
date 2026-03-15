# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class Target:
    container: str
    cpuset_cpus: Optional[str] = None
    cpu_shares: Optional[int] = None
    blkio_weight: Optional[int] = None


def parse_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        val = v.strip().lower()
        if val in {"1", "true", "yes", "on"}:
            return True
        if val in {"0", "false", "no", "off"}:
            return False
    return default


def load_options(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Options file not found: {path}")
    return json.loads(p.read_text(encoding="utf-8"))


def parse_targets(raw_targets: Any, log: logging.Logger) -> list[Target]:
    if raw_targets is None:
        return []
    if not isinstance(raw_targets, list):
        raise ValueError("'targets' must be a list")

    targets: list[Target] = []
    for idx, raw in enumerate(raw_targets):
        if not isinstance(raw, dict):
            raise ValueError(f"targets[{idx}] must be an object")

        container = str(raw.get("container", "")).strip()
        if not container:
            raise ValueError(f"targets[{idx}].container is required")

        cpuset_raw = raw.get("cpuset_cpus")
        cpuset = None
        if cpuset_raw is not None:
            cpuset = str(cpuset_raw).strip()
            if not cpuset:
                cpuset = None

        cpu_shares_raw = raw.get("cpu_shares")
        cpu_shares = int(cpu_shares_raw) if cpu_shares_raw is not None else None

        blkio_raw = raw.get("blkio_weight")
        blkio_weight = int(blkio_raw) if blkio_raw is not None else None

        if cpuset is None and cpu_shares is None and blkio_weight is None:
            log.warning(
                "Skipping targets[%d] (%s): no tuning values specified",
                idx,
                container,
            )
            continue

        targets.append(
            Target(
                container=container,
                cpuset_cpus=cpuset,
                cpu_shares=cpu_shares,
                blkio_weight=blkio_weight,
            )
        )

    return targets


def docker_inspect_limits(
    container: str, log: logging.Logger
) -> Optional[dict[str, Any]]:
    cmd = ["docker", "inspect", container]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout).strip()
        log.warning("docker inspect failed for %s: %s", container, err)
        return None

    try:
        payload = json.loads(proc.stdout)
        if not isinstance(payload, list) or not payload:
            raise ValueError("inspect payload is empty")
        host_cfg = payload[0].get("HostConfig", {})
        if not isinstance(host_cfg, dict):
            host_cfg = {}

        return {
            "cpuset_cpus": str(host_cfg.get("CpusetCpus") or ""),
            "cpu_shares": int(host_cfg.get("CpuShares") or 0),
            "blkio_weight": int(host_cfg.get("BlkioWeight") or 0),
        }
    except Exception as e:
        log.warning("Failed to parse docker inspect for %s: %s", container, e)
        return None


def desired_update_args(target: Target, current: dict[str, Any]) -> list[str]:
    args: list[str] = []

    if (
        target.cpuset_cpus is not None
        and str(current.get("cpuset_cpus", "")) != target.cpuset_cpus
    ):
        args += ["--cpuset-cpus", target.cpuset_cpus]

    if target.cpu_shares is not None and int(current.get("cpu_shares", 0)) != int(
        target.cpu_shares
    ):
        args += ["--cpu-shares", str(int(target.cpu_shares))]

    if target.blkio_weight is not None and int(current.get("blkio_weight", 0)) != int(
        target.blkio_weight
    ):
        args += ["--blkio-weight", str(int(target.blkio_weight))]

    return args


def apply_target(target: Target, dry_run: bool, log: logging.Logger) -> None:
    current = docker_inspect_limits(target.container, log)
    if current is None:
        return

    update_args = desired_update_args(target, current)
    if not update_args:
        log.debug("No changes needed for container=%s", target.container)
        return

    cmd = ["docker", "update", *update_args, target.container]

    if dry_run:
        log.info("DRY RUN: %s", " ".join(shlex.quote(x) for x in cmd))
        return

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout).strip()
        log.error("docker update failed for %s: %s", target.container, err)
        return

    out = (proc.stdout or "").strip()
    if out:
        log.info("docker update ok for %s: %s", target.container, out)
    else:
        log.info("docker update ok for %s", target.container)


def apply_all(targets: list[Target], dry_run: bool, log: logging.Logger) -> None:
    for target in targets:
        apply_target(target, dry_run, log)


def main() -> int:
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
    except Exception as e:
        log.error("Invalid targets configuration: %s", e)
        return 1

    if not targets:
        log.warning(
            "No valid targets configured; running in idle mode (no changes will be applied)."
        )

    log.info(
        "Starting System Resource Tuner: targets=%d interval_seconds=%d dry_run=%s",
        len(targets),
        interval_seconds,
        dry_run,
    )

    if apply_on_start:
        apply_all(targets, dry_run, log)

    try:
        while True:
            time.sleep(interval_seconds)
            apply_all(targets, dry_run, log)
    except KeyboardInterrupt:
        log.info("Shutting down")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
