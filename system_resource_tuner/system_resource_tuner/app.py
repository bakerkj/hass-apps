# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point: option parsing, initial apply, reconcile loop.

Runs on a single ``asyncio`` event loop. Daemon I/O (``docker
update``, ``docker top``) goes through ``aiodocker`` against the unix
socket. CPU-bound bits (``os.setpriority``, ``os.sched_setaffinity``,
``/proc`` walks, the host ``ps`` subprocess) are kept synchronous and
pushed to a worker thread via ``asyncio.to_thread`` so the loop stays
responsive while a large thread list is being walked.

Reconcile cadence is unchanged from the pre-async version: one initial
apply if ``apply_on_start`` is set, then ``interval_seconds`` between
full reconcile passes. SIGTERM and SIGINT set an ``asyncio.Event``
that's awaited as the loop's sleep mechanism, so signal latency is
near-zero rather than up to one full interval.
"""

import argparse
import asyncio
import contextlib
import logging
import signal

import aiodocker
import aiohttp
from aiodocker.exceptions import DockerError

from . import __version__
from .config import (
    ProcessTuning,
    Target,
    load_options,
    parse_bool,
    parse_host_process_targets,
    parse_process_targets,
    parse_targets,
)
from .docker import apply_all, docker_url
from .process import apply_process_tunings

# Hard cap on the startup probe so a daemon that's reachable but hung
# (HA Supervisor mid-restart is a known case) doesn't block ``main_async``
# indefinitely with SIGTERM unreachable.
_DOCKER_PROBE_TIMEOUT_SECONDS = 10.0


async def _reconcile_pass(
    docker: aiodocker.Docker,
    targets: list[Target],
    process_targets: list[ProcessTuning],
    host_process_targets: list[ProcessTuning],
    dry_run: bool,
    log: logging.Logger,
) -> None:
    """One full reconcile sweep, with broad-exception safety net.

    Pre-refactor the subprocess shim returned a non-zero rc instead of
    raising on daemon hiccups, so the reconcile loop could keep going.
    aiodocker raises ``aiohttp.ClientError`` / ``asyncio.TimeoutError``
    on the same conditions; the docker.py helpers catch the transient
    set, but this wrapper is the final guard so an unexpected
    exception class still won't kill the addon mid-loop.
    """
    try:
        await apply_all(docker, targets, dry_run, log)
        await apply_process_tunings(docker, process_targets, dry_run, log)
        await apply_process_tunings(docker, host_process_targets, dry_run, log)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 — reconcile must survive any transient surface
        log.exception("reconcile pass failed; will retry next tick")


async def main_async() -> int:
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
    log.info("System Resource Tuner v%s starting", __version__)

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

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    docker = aiodocker.Docker(url=docker_url())
    try:
        try:
            await asyncio.wait_for(
                docker.system.info(), timeout=_DOCKER_PROBE_TIMEOUT_SECONDS
            )
        except TimeoutError:
            # MUST come before the ``OSError`` clause below: in CPython
            # 3.3+ ``TimeoutError`` is a subclass of ``OSError``, so the
            # broader handler would otherwise eat ``asyncio.wait_for``'s
            # timeout signal and misclassify it as a socket failure.
            log.error(
                "Docker API check timed out after %.0fs; daemon reachable but not "
                "responding. Aborting so Supervisor can restart us.",
                _DOCKER_PROBE_TIMEOUT_SECONDS,
            )
            return 1
        except aiohttp.ClientError, OSError:
            # Socket-level errors mean the daemon isn't reachable at all
            # — the most common HA-addon failure mode is Protection Mode
            # blocking the docker.sock bind mount. ``DockerError`` covers
            # daemon-returned HTTP errors (a different failure mode), so
            # the helpful guidance branch keys off the transport-error
            # class rather than string-matching the message.
            log.error(
                "Cannot connect to the Docker API at the unix socket. "
                "Disable Protection Mode for this addon in the Home Assistant UI "
                "(Addon → Info → Protection mode) and restart."
            )
            return 1
        except DockerError as e:
            log.error("Docker API check failed: %s", e)
            return 1
        except Exception as e:  # noqa: BLE001 — final safety net
            log.error("Docker API check failed: %s", e)
            return 1

        if apply_on_start:
            await _reconcile_pass(
                docker, targets, process_targets, host_process_targets, dry_run, log
            )

        while not stop.is_set():
            # ``wait_for`` returns when the event is set (signal) and
            # raises ``TimeoutError`` when ``interval_seconds`` has elapsed
            # — the timeout is the "tick" path. Either way we loop and
            # the top-of-loop check decides whether to apply again or
            # exit.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            if stop.is_set():
                break
            await _reconcile_pass(
                docker, targets, process_targets, host_process_targets, dry_run, log
            )
    finally:
        await docker.close()

    log.info("Shutting down")
    return 0


def main() -> int:
    return asyncio.run(main_async())
