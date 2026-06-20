# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Docker CLI wrappers + container-level cgroup updates."""

import json
import logging
import shlex
import subprocess
from typing import Any

from .config import Target, cpuset_matches


def cmd_error(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def docker_inspect_limits(
    container: str,
    log: logging.Logger,
) -> dict[str, Any] | None:
    cmd = ["docker", "inspect", container]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning("docker inspect failed for %s: %s", container, cmd_error(proc))
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

    if target.cpuset_cpus is not None and not cpuset_matches(
        str(current.get("cpuset_cpus", "")), target.cpuset_cpus
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
        log.debug(
            "No container-level changes needed for container=%s", target.container
        )
        return

    cmd = ["docker", "update", *update_args, target.container]

    if dry_run:
        log.info("DRY RUN: %s", " ".join(shlex.quote(x) for x in cmd))
        return

    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.error("docker update failed for %s: %s", target.container, cmd_error(proc))
        return

    out = (proc.stdout or "").strip()
    if out:
        log.info("docker update ok for %s: %s", target.container, out)
    else:
        log.info("docker update ok for %s", target.container)


def apply_all(targets: list[Target], dry_run: bool, log: logging.Logger) -> None:
    for target in targets:
        apply_target(target, dry_run, log)


def docker_top_processes(
    container: str,
    log: logging.Logger,
) -> list[tuple[int, str]]:
    cmd = ["docker", "top", container, "-eo", "pid,args"]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning(
            "Failed to list processes in container=%s: %s",
            container,
            cmd_error(proc),
        )
        return []

    rows: list[tuple[int, str]] = []
    for line in proc.stdout.splitlines():
        row = line.strip()
        if not row:
            continue

        parts = row.split(None, 1)
        if len(parts) != 2 or not parts[0].isdigit():
            continue

        rows.append((int(parts[0]), parts[1]))

    return rows
