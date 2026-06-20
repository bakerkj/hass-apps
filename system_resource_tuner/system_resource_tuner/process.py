# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Process & thread discovery + nice/cpuset application."""

import logging
import os
import re
from pathlib import Path

from .config import ProcessTuning, parse_cpuset_expression
from .docker import docker_top_processes, run_cmd


def host_top_processes(log: logging.Logger) -> list[tuple[int, str]]:
    cmd = ["ps", "-eo", "pid,args"]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        from .docker import cmd_error

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


_NICE_RESCAN_PASSES = 3


def apply_process_nice(
    tuning: ProcessTuning,
    host_pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    """Apply nice value to every thread of the process.

    Linux nice values are per-task, not per-process: ``setpriority(
    PRIO_PROCESS, tid, nice)`` only updates the single task whose TID is
    passed.  Worker threads keep their original nice unless we iterate over
    /proc/<pid>/task/* and call setpriority for each TID.

    To close the race window where a thread is created between scan and
    apply (the new thread inherits its parent's nice via clone(2), which
    may still be the *old* nice if the parent hasn't been updated yet),
    we re-scan up to ``_NICE_RESCAN_PASSES`` times until no new TIDs
    appear.  Bounded so a pathological process spawning short-lived
    threads can't loop us forever.
    """
    if tuning.nice is None:
        raise ValueError("apply_process_nice called with tuning.nice=None")

    thread_ids = list_process_threads(host_pid, tuning.container_label, log)
    if not thread_ids:
        return

    # Fast path: skip the syscalls if every thread already has the desired
    # nice value.  Avoids log noise when nothing has changed since the
    # previous reconcile pass.
    if all(
        read_process_nice(tid, tuning.container_label, log) == tuning.nice
        for tid in thread_ids
    ):
        log.debug(
            "No process nice change needed for container=%s host_pid=%d",
            tuning.container_label,
            host_pid,
        )
        return

    if dry_run:
        log.info(
            "DRY RUN: setpriority container=%s host_pid=%d threads=%d nice=%d",
            tuning.container_label,
            host_pid,
            len(thread_ids),
            tuning.nice,
        )
        return

    failures: list[str] = []
    seen: set[int] = set()
    changed = 0
    rescans = 0

    for _ in range(_NICE_RESCAN_PASSES):
        current = list_process_threads(host_pid, tuning.container_label, log)
        if not current:
            break
        rescans += 1
        new_tids = [tid for tid in current if tid not in seen]
        if not new_tids:
            break
        for tid in new_tids:
            try:
                os.setpriority(os.PRIO_PROCESS, tid, tuning.nice)
                changed += 1
            except ProcessLookupError:
                log.debug(
                    "Task disappeared while setting nice: container=%s host_pid=%d task_pid=%d",
                    tuning.container_label,
                    host_pid,
                    tid,
                )
            except PermissionError as e:
                failures.append(f"task_pid={tid}: {e}")
            except OSError as e:
                failures.append(f"task_pid={tid}: {e}")
        seen.update(current)

    if failures:
        preview = "; ".join(failures[:3])
        if len(failures) > 3:
            preview += f"; ... (+{len(failures) - 3} more)"
        log.error(
            "Failed applying nice=%d for container=%s host_pid=%d: %s",
            tuning.nice,
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
        "Process nice updated for container=%s host_pid=%d to nice=%d "
        "across %d thread(s) in %d scan(s)",
        tuning.container_label,
        host_pid,
        tuning.nice,
        changed,
        rescans,
    )


def apply_process_cpuset(
    tuning: ProcessTuning,
    host_pid: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    if tuning.cpuset_cpus is None:
        raise ValueError("apply_process_cpuset called with tuning.cpuset_cpus=None")

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
