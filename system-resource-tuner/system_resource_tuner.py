# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from __future__ import annotations

import argparse
import json
import logging
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

DEFAULT_HOMEASSISTANT_CONTAINER = "homeassistant"
DEFAULT_HOMEASSISTANT_PROCESS_REGEX = r"python3 .*homeassistant|homeassistant"


@dataclass(frozen=True)
class Target:
    container: str
    cpuset_cpus: Optional[str] = None
    cpu_shares: Optional[int] = None
    blkio_weight: Optional[int] = None


@dataclass(frozen=True)
class HomeAssistantProcessTuning:
    container: str = DEFAULT_HOMEASSISTANT_CONTAINER
    process_match_regex: str = DEFAULT_HOMEASSISTANT_PROCESS_REGEX
    nice: Optional[int] = None
    cpuset_cpus: Optional[str] = None

    @property
    def is_configured(self) -> bool:
        return self.nice is not None or self.cpuset_cpus is not None


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


def parse_cpuset_expression(cpus: str) -> Optional[set[int]]:
    value = cpus.strip()
    if not value:
        return set()

    out: set[int] = set()
    for token in value.split(","):
        part = token.strip()
        if not part:
            return None

        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            if not start_raw.isdigit() or not end_raw.isdigit():
                return None
            start = int(start_raw)
            end = int(end_raw)
            if end < start:
                return None
            for cpu in range(start, end + 1):
                out.add(cpu)
            continue

        if not part.isdigit():
            return None
        out.add(int(part))

    return out


def cpuset_matches(current: str, desired: str) -> bool:
    cur = current.strip()
    des = desired.strip()

    cur_set = parse_cpuset_expression(cur)
    des_set = parse_cpuset_expression(des)
    if cur_set is not None and des_set is not None:
        return cur_set == des_set

    return cur == des


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
            elif parse_cpuset_expression(cpuset) is None:
                raise ValueError(f"targets[{idx}].cpuset_cpus is invalid: '{cpuset}'")

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


def parse_homeassistant_process_tuning(
    raw_cfg: Any,
) -> HomeAssistantProcessTuning:
    if raw_cfg is None:
        return HomeAssistantProcessTuning()
    if not isinstance(raw_cfg, dict):
        raise ValueError("'homeassistant_process' must be an object")

    container = str(raw_cfg.get("container", DEFAULT_HOMEASSISTANT_CONTAINER)).strip()
    if not container:
        container = DEFAULT_HOMEASSISTANT_CONTAINER

    pattern = str(
        raw_cfg.get("process_match_regex", DEFAULT_HOMEASSISTANT_PROCESS_REGEX)
    ).strip()
    if not pattern:
        pattern = DEFAULT_HOMEASSISTANT_PROCESS_REGEX

    try:
        _ = re.compile(pattern)
    except re.error as e:
        raise ValueError(f"homeassistant_process.process_match_regex is invalid: {e}")

    nice: Optional[int] = None
    if raw_cfg.get("nice") is not None:
        nice = int(raw_cfg["nice"])
        if nice < -20 or nice > 19:
            raise ValueError("homeassistant_process.nice must be between -20 and 19")

    cpuset_cpus: Optional[str] = None
    if raw_cfg.get("cpuset_cpus") is not None:
        cpuset_cpus = str(raw_cfg["cpuset_cpus"]).strip()
        if not cpuset_cpus:
            cpuset_cpus = None
        elif parse_cpuset_expression(cpuset_cpus) is None:
            raise ValueError(
                "homeassistant_process.cpuset_cpus must be a valid CPU set expression"
            )

    return HomeAssistantProcessTuning(
        container=container,
        process_match_regex=pattern,
        nice=nice,
        cpuset_cpus=cpuset_cpus,
    )


def cmd_error(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def docker_inspect_limits(
    container: str,
    log: logging.Logger,
) -> Optional[dict[str, Any]]:
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


def find_matching_pid(
    container: str,
    process_match_regex: str,
    log: logging.Logger,
) -> Optional[int]:
    try:
        matcher = re.compile(process_match_regex)
    except re.error as e:
        log.error("Invalid process_match_regex '%s': %s", process_match_regex, e)
        return None

    cmd = ["docker", "exec", container, "ps", "-o", "pid,args"]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning(
            "Failed to list processes in container=%s: %s",
            container,
            cmd_error(proc),
        )
        return None

    for line in proc.stdout.splitlines():
        row = line.strip()
        if not row:
            continue
        parts = row.split(None, 1)
        if len(parts) != 2 or not parts[0].isdigit():
            continue

        pid = int(parts[0])
        cmdline = parts[1]
        if matcher.search(cmdline):
            return pid

    log.warning(
        "No process matched regex '%s' in container=%s",
        process_match_regex,
        container,
    )
    return None


def read_process_nice(container: str, pid: int, log: logging.Logger) -> Optional[int]:
    cmd = [
        "docker",
        "exec",
        container,
        "sh",
        "-c",
        f"awk '{{print $19}}' /proc/{pid}/stat",
    ]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning(
            "Failed reading /proc/%d/stat in container=%s: %s",
            pid,
            container,
            cmd_error(proc),
        )
        return None

    raw = (proc.stdout or "").strip()
    try:
        return int(raw)
    except Exception:
        log.warning(
            "Unexpected nice value for pid=%d in container=%s: '%s'",
            pid,
            container,
            raw,
        )
        return None


def read_process_cpuset(container: str, pid: int, log: logging.Logger) -> Optional[str]:
    cmd = [
        "docker",
        "exec",
        container,
        "sh",
        "-c",
        f"awk '/^Cpus_allowed_list:/{{print $2}}' /proc/{pid}/status",
    ]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning(
            "Failed reading /proc/%d/status in container=%s: %s",
            pid,
            container,
            cmd_error(proc),
        )
        return None

    return (proc.stdout or "").strip()


def apply_process_nice(
    tuning: HomeAssistantProcessTuning,
    pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    assert tuning.nice is not None

    current_nice = read_process_nice(tuning.container, pid, log)
    if current_nice is not None and current_nice == tuning.nice:
        log.debug(
            "No process nice change needed for container=%s pid=%d",
            tuning.container,
            pid,
        )
        return

    cmd = [
        "docker",
        "exec",
        tuning.container,
        "renice",
        "-n",
        str(tuning.nice),
        "-p",
        str(pid),
    ]

    if dry_run:
        log.info("DRY RUN: %s", " ".join(shlex.quote(x) for x in cmd))
        return

    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.error(
            "Failed setting nice=%d for container=%s pid=%d: %s",
            tuning.nice,
            tuning.container,
            pid,
            cmd_error(proc),
        )
        return

    log.info(
        "Process nice updated for container=%s pid=%d to nice=%d",
        tuning.container,
        pid,
        tuning.nice,
    )


def apply_process_cpuset(
    tuning: HomeAssistantProcessTuning,
    pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    assert tuning.cpuset_cpus is not None

    current_cpuset = read_process_cpuset(tuning.container, pid, log)
    if current_cpuset and cpuset_matches(current_cpuset, tuning.cpuset_cpus):
        log.debug(
            "No process affinity change needed for container=%s pid=%d",
            tuning.container,
            pid,
        )
        return

    cmd = [
        "docker",
        "exec",
        tuning.container,
        "taskset",
        "-apc",
        tuning.cpuset_cpus,
        str(pid),
    ]

    if dry_run:
        log.info("DRY RUN: %s", " ".join(shlex.quote(x) for x in cmd))
        return

    proc = run_cmd(cmd)
    if proc.returncode != 0:
        err = cmd_error(proc)
        if "not found" in err.lower():
            log.error(
                "taskset is not available in container=%s; cannot set process affinity",
                tuning.container,
            )
        else:
            log.error(
                "Failed setting cpuset=%s for container=%s pid=%d: %s",
                tuning.cpuset_cpus,
                tuning.container,
                pid,
                err,
            )
        return

    log.info(
        "Process affinity updated for container=%s pid=%d to cpuset=%s",
        tuning.container,
        pid,
        tuning.cpuset_cpus,
    )


def apply_homeassistant_process_tuning(
    tuning: HomeAssistantProcessTuning,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    if not tuning.is_configured:
        return

    pid = find_matching_pid(tuning.container, tuning.process_match_regex, log)
    if pid is None:
        return

    if tuning.nice is not None:
        apply_process_nice(tuning, pid, dry_run, log)

    if tuning.cpuset_cpus is not None:
        apply_process_cpuset(tuning, pid, dry_run, log)


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
        process_tuning = parse_homeassistant_process_tuning(
            options.get("homeassistant_process")
        )
    except Exception as e:
        log.error("Invalid configuration: %s", e)
        return 1

    if not targets and not process_tuning.is_configured:
        log.warning(
            "No valid tuning configured; running in idle mode (no changes will be applied)."
        )

    log.info(
        "Starting System Resource Tuner: container_targets=%d process_tuning=%s interval_seconds=%d dry_run=%s",
        len(targets),
        process_tuning.is_configured,
        interval_seconds,
        dry_run,
    )

    if apply_on_start:
        apply_all(targets, dry_run, log)
        apply_homeassistant_process_tuning(process_tuning, dry_run, log)

    try:
        while True:
            time.sleep(interval_seconds)
            apply_all(targets, dry_run, log)
            apply_homeassistant_process_tuning(process_tuning, dry_run, log)
    except KeyboardInterrupt:
        log.info("Shutting down")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
