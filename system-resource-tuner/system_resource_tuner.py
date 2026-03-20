# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from __future__ import annotations

import argparse
import json
import os
import logging
import re
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Target:
    container: str
    cpuset_cpus: str | None = None
    cpu_shares: int | None = None
    blkio_weight: int | None = None


@dataclass(frozen=True)
class ProcessTuning:
    container: str | None = None
    process_match_regex: str = ""
    nice: int | None = None
    cpuset_cpus: str | None = None

    @property
    def is_host(self) -> bool:
        return self.container is None

    @property
    def container_label(self) -> str:
        return self.container if self.container is not None else "host"

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


def parse_cpuset_expression(cpus: str) -> set[int] | None:
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


def parse_process_tuning(
    raw_cfg: Any,
    block_name: str,
    require_container: bool = True,
) -> ProcessTuning:
    if raw_cfg is None:
        return ProcessTuning()
    if not isinstance(raw_cfg, dict):
        raise ValueError(f"'{block_name}' must be an object")

    container: str | None = str(raw_cfg.get("container", "")).strip() or None
    pattern = str(raw_cfg.get("process_match_regex", "")).strip()

    if pattern:
        try:
            _ = re.compile(pattern)
        except re.error as e:
            raise ValueError(f"{block_name}.process_match_regex is invalid: {e}")

    nice: int | None = None
    if raw_cfg.get("nice") is not None:
        nice = int(raw_cfg["nice"])
        if nice < -20 or nice > 19:
            raise ValueError(f"{block_name}.nice must be between -20 and 19")

    cpuset_cpus: str | None = None
    if raw_cfg.get("cpuset_cpus") is not None:
        cpuset_cpus = str(raw_cfg["cpuset_cpus"]).strip()
        if not cpuset_cpus:
            cpuset_cpus = None
        elif parse_cpuset_expression(cpuset_cpus) is None:
            raise ValueError(
                f"{block_name}.cpuset_cpus must be a valid CPU set expression"
            )

    is_configured = nice is not None or cpuset_cpus is not None
    if is_configured and require_container and not container:
        raise ValueError(
            f"{block_name}.container is required when tuning is configured"
        )
    if is_configured and not pattern:
        raise ValueError(
            f"{block_name}.process_match_regex is required when tuning is configured"
        )

    return ProcessTuning(
        container=container,
        process_match_regex=pattern,
        nice=nice,
        cpuset_cpus=cpuset_cpus,
    )


def parse_process_targets(raw_cfg: Any, log: logging.Logger) -> list[ProcessTuning]:
    if raw_cfg is None:
        return []
    if not isinstance(raw_cfg, list):
        raise ValueError("'process_targets' must be a list")

    out: list[ProcessTuning] = []
    for idx, raw in enumerate(raw_cfg):
        tuning = parse_process_tuning(raw, block_name=f"process_targets[{idx}]")
        if not tuning.is_configured:
            log.warning(
                "Skipping process_targets[%d]: no process tuning values specified",
                idx,
            )
            continue
        out.append(tuning)

    return out


def parse_host_process_targets(
    raw_cfg: Any,
    log: logging.Logger,
) -> list[ProcessTuning]:
    if raw_cfg is None:
        return []
    if not isinstance(raw_cfg, list):
        raise ValueError("'host_process_targets' must be a list")

    out: list[ProcessTuning] = []
    for idx, raw in enumerate(raw_cfg):
        if not isinstance(raw, dict):
            raise ValueError(f"host_process_targets[{idx}] must be an object")

        tuning = parse_process_tuning(
            raw,
            block_name=f"host_process_targets[{idx}]",
            require_container=False,
        )
        if not tuning.is_configured:
            log.warning(
                "Skipping host_process_targets[%d]: no process tuning values specified",
                idx,
            )
            continue
        out.append(tuning)

    return out


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


def host_top_processes(log: logging.Logger) -> list[tuple[int, str]]:
    cmd = ["ps", "-eo", "pid,args"]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        log.warning("Failed to list host processes: %s", cmd_error(proc))
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


def find_matching_host_pids(
    process_match_regex: str,
    log: logging.Logger,
) -> list[int]:
    try:
        matcher = re.compile(process_match_regex)
    except re.error as e:
        log.error("Invalid process_match_regex '%s': %s", process_match_regex, e)
        return []

    out: list[int] = []
    for host_pid, cmdline in host_top_processes(log):
        if matcher.search(cmdline):
            out.append(host_pid)

    return out


def find_matching_pids(
    container: str,
    process_match_regex: str,
    log: logging.Logger,
) -> list[int]:
    try:
        matcher = re.compile(process_match_regex)
    except re.error as e:
        log.error("Invalid process_match_regex '%s': %s", process_match_regex, e)
        return []

    out: list[int] = []
    for host_pid, cmdline in docker_top_processes(container, log):
        if matcher.search(cmdline):
            out.append(host_pid)

    if not out:
        log.warning(
            "No process matched regex '%s' in container=%s",
            process_match_regex,
            container,
        )
    return out


def read_process_nice(
    host_pid: int,
    container_label: str,
    log: logging.Logger,
) -> int | None:
    try:
        return os.getpriority(os.PRIO_PROCESS, host_pid)
    except ProcessLookupError:
        log.warning(
            "Cannot read nice for container=%s host_pid=%d: process no longer exists",
            container_label,
            host_pid,
        )
        return None
    except PermissionError as e:
        log.warning(
            "Cannot read nice for container=%s host_pid=%d: %s",
            container_label,
            host_pid,
            e,
        )
        return None
    except OSError as e:
        log.warning(
            "Cannot read nice for container=%s host_pid=%d: %s",
            container_label,
            host_pid,
            e,
        )
        return None


def read_task_cpuset(
    task_pid: int,
    container_label: str,
    root_pid: int,
    log: logging.Logger,
) -> set[int] | None:
    try:
        return set(os.sched_getaffinity(task_pid))
    except ProcessLookupError:
        log.debug(
            "Task disappeared while reading affinity: container=%s host_pid=%d task_pid=%d",
            container_label,
            root_pid,
            task_pid,
        )
        return None
    except PermissionError as e:
        log.warning(
            "Cannot read affinity for container=%s host_pid=%d task_pid=%d: %s",
            container_label,
            root_pid,
            task_pid,
            e,
        )
        return None
    except OSError as e:
        log.warning(
            "Cannot read affinity for container=%s host_pid=%d task_pid=%d: %s",
            container_label,
            root_pid,
            task_pid,
            e,
        )
        return None


def list_process_threads(
    host_pid: int,
    container_label: str,
    log: logging.Logger,
) -> list[int] | None:
    task_dir = Path(f"/proc/{host_pid}/task")
    try:
        tids = sorted(
            int(entry.name) for entry in task_dir.iterdir() if entry.name.isdigit()
        )
    except FileNotFoundError:
        log.warning(
            "Cannot list threads for container=%s host_pid=%d: process no longer exists",
            container_label,
            host_pid,
        )
        return None
    except PermissionError as e:
        log.warning(
            "Cannot list threads for container=%s host_pid=%d: %s",
            container_label,
            host_pid,
            e,
        )
        return None
    except OSError as e:
        log.warning(
            "Cannot list threads for container=%s host_pid=%d: %s",
            container_label,
            host_pid,
            e,
        )
        return None

    if not tids:
        log.warning(
            "No threads found for container=%s host_pid=%d",
            container_label,
            host_pid,
        )
        return None

    return tids


def apply_process_nice(
    tuning: ProcessTuning,
    host_pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    assert tuning.nice is not None

    current_nice = read_process_nice(host_pid, tuning.container_label, log)
    if current_nice is not None and current_nice == tuning.nice:
        log.debug(
            "No process nice change needed for container=%s host_pid=%d",
            tuning.container_label,
            host_pid,
        )
        return

    if dry_run:
        log.info(
            "DRY RUN: setpriority container=%s host_pid=%d nice=%d",
            tuning.container_label,
            host_pid,
            tuning.nice,
        )
        return

    try:
        os.setpriority(os.PRIO_PROCESS, host_pid, tuning.nice)
    except ProcessLookupError:
        log.error(
            "Failed setting nice=%d for container=%s host_pid=%d: process no longer exists",
            tuning.nice,
            tuning.container_label,
            host_pid,
        )
        return
    except PermissionError as e:
        log.error(
            "Failed setting nice=%d for container=%s host_pid=%d: %s",
            tuning.nice,
            tuning.container_label,
            host_pid,
            e,
        )
        return
    except OSError as e:
        log.error(
            "Failed setting nice=%d for container=%s host_pid=%d: %s",
            tuning.nice,
            tuning.container_label,
            host_pid,
            e,
        )
        return

    log.info(
        "Process nice updated for container=%s host_pid=%d to nice=%d",
        tuning.container_label,
        host_pid,
        tuning.nice,
    )


def apply_process_cpuset(
    tuning: ProcessTuning,
    host_pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    assert tuning.cpuset_cpus is not None

    desired_cpus = parse_cpuset_expression(tuning.cpuset_cpus)
    if not desired_cpus:
        log.error(
            "Cannot apply process affinity for container=%s host_pid=%d: invalid cpuset '%s'",
            tuning.container_label,
            host_pid,
            tuning.cpuset_cpus,
        )
        return

    thread_ids = list_process_threads(host_pid, tuning.container_label, log)
    if not thread_ids:
        return

    all_match = True
    for tid in thread_ids:
        current = read_task_cpuset(tid, tuning.container_label, host_pid, log)
        if current is None or current != desired_cpus:
            all_match = False
            break

    if all_match:
        log.debug(
            "No process affinity change needed for container=%s host_pid=%d",
            tuning.container_label,
            host_pid,
        )
        return

    if dry_run:
        log.info(
            "DRY RUN: sched_setaffinity container=%s host_pid=%d threads=%d cpuset=%s",
            tuning.container_label,
            host_pid,
            len(thread_ids),
            tuning.cpuset_cpus,
        )
        return

    failures: list[str] = []
    changed = 0
    for tid in thread_ids:
        try:
            os.sched_setaffinity(tid, desired_cpus)
            changed += 1
        except ProcessLookupError:
            log.debug(
                "Task disappeared while setting affinity: container=%s host_pid=%d task_pid=%d",
                tuning.container_label,
                host_pid,
                tid,
            )
        except PermissionError as e:
            failures.append(f"task_pid={tid}: {e}")
        except OSError as e:
            failures.append(f"task_pid={tid}: {e}")

    if failures:
        preview = "; ".join(failures[:3])
        if len(failures) > 3:
            preview += f"; ... (+{len(failures) - 3} more)"
        log.error(
            "Failed applying affinity cpuset=%s for container=%s host_pid=%d: %s",
            tuning.cpuset_cpus,
            tuning.container_label,
            host_pid,
            preview,
        )
        return

    if changed == 0:
        log.warning(
            "No threads were updated for container=%s host_pid=%d",
            tuning.container_label,
            host_pid,
        )
        return

    log.info(
        "Process affinity updated for container=%s host_pid=%d to cpuset=%s across %d thread(s)",
        tuning.container_label,
        host_pid,
        tuning.cpuset_cpus,
        changed,
    )


def apply_tuning_to_pid(
    tuning: ProcessTuning,
    host_pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    if tuning.nice is not None:
        apply_process_nice(tuning, host_pid, dry_run, log)

    if tuning.cpuset_cpus is not None:
        apply_process_cpuset(tuning, host_pid, dry_run, log)


def apply_process_tuning(
    tuning: ProcessTuning,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    if not tuning.is_configured:
        return

    if tuning.is_host:
        host_pids = find_matching_host_pids(tuning.process_match_regex, log)
        if not host_pids:
            log.warning(
                "No host process matched regex '%s'",
                tuning.process_match_regex,
            )
            return

        for pid in host_pids:
            apply_tuning_to_pid(tuning, pid, dry_run, log)
        return

    for pid in find_matching_pids(
        tuning.container,  # type: ignore[arg-type]  # str: is_host was False
        tuning.process_match_regex,
        log,
    ):
        apply_tuning_to_pid(tuning, pid, dry_run, log)


def apply_process_tunings(
    tunings: list[ProcessTuning],
    dry_run: bool,
    log: logging.Logger,
) -> None:
    for tuning in tunings:
        apply_process_tuning(tuning, dry_run, log)


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

    log.info(
        "Starting System Resource Tuner: container_targets=%d process_targets=%d "
        "host_process_targets=%d interval_seconds=%d dry_run=%s",
        len(targets),
        len(process_targets),
        len(host_process_targets),
        interval_seconds,
        dry_run,
    )

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


if __name__ == "__main__":
    raise SystemExit(main())
