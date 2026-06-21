# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Entry-point: event-driven fast-path + periodic reconcile backstop.

Two concurrent tasks under a single asyncio event loop:

1. **Events task** — subscribes to docker container ``start`` events
   for configured containers and runs a per-container apply chain
   (cgroup-level update + initial process tunings) followed by a
   short retry ladder that catches worker threads that spawn late.

2. **Reconcile task** — every ``interval_seconds``, re-runs the
   full apply over every configured target. This is the backstop
   for ``host_process_targets`` (host PIDs don't emit docker events),
   for the gap window if the events stream drops, and for any drift
   in container-level cgroup limits.

CPU-bound work (``os.setpriority`` etc.) runs in ``asyncio.to_thread``
so the event loop stays responsive while a large thread list is being
walked. SIGTERM / SIGINT set an ``asyncio.Event`` and both tasks observe
it for near-zero shutdown latency.
"""

import argparse
import asyncio
import contextlib
import logging
import signal
import time
from typing import Any

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
from .docker import apply_all, apply_target, docker_events, docker_url
from .process import apply_process_tuning, apply_process_tunings

# Sensible defaults for the post-start retry ladder. Most containers'
# worker threads spawn within a few seconds of entrypoint; 0+1+3+8 covers
# typical cases (ffmpeg/Frigate child workers, multi-threaded servers
# warming up) without sticking around long enough to interfere with the
# periodic reconcile.
_DEFAULT_POST_START_RETRY_SECONDS: tuple[int, ...] = (0, 1, 3, 8)
_DEFAULT_POST_START_RETRY_MAX_SECONDS: int = 30


def _parse_retry_ladder(raw: Any, log: logging.Logger) -> tuple[int, ...]:
    """Normalize ``post_start_retry_seconds`` to a tuple of non-negative ints.

    Three distinct shapes:

    * ``None`` (option absent) → built-in default ladder.
    * Non-list (string, dict, scalar) → warning + default ladder. The
      user almost certainly meant a list and would prefer the safety
      belt over silent no-retries.
    * Any list — including empty, or one that empties out after
      filtering — → tuple of validated ints, possibly empty. An empty
      tuple means "no retry passes; only the initial apply runs",
      which is the intent a user expresses by writing ``[]``. The
      previous behaviour (silent fallback to default for ``[]``) made
      that intent un-expressible.
    """
    if raw is None:
        return _DEFAULT_POST_START_RETRY_SECONDS
    if not isinstance(raw, list):
        log.warning(
            "post_start_retry_seconds must be a list of ints; using default %s",
            list(_DEFAULT_POST_START_RETRY_SECONDS),
        )
        return _DEFAULT_POST_START_RETRY_SECONDS
    out: list[int] = []
    for v in raw:
        try:
            n = int(v)
        except TypeError, ValueError:
            log.warning("post_start_retry_seconds entry %r is not an int; skipping", v)
            continue
        if n < 0:
            log.warning("post_start_retry_seconds entry %d is negative; skipping", n)
            continue
        out.append(n)
    return tuple(out)


def _bucket_by_container(
    targets: list[Target],
    process_targets: list[ProcessTuning],
) -> tuple[dict[str, list[Target]], dict[str, list[ProcessTuning]], set[str]]:
    """Group configured tunings by container for the events fast-path.

    Returns ``(targets_by_container, process_by_container, watch_set)``.
    ``watch_set`` is the union of names the events task cares about —
    used to filter the daemon's event stream in the dispatch loop.
    """
    targets_by_container: dict[str, list[Target]] = {}
    for t in targets:
        targets_by_container.setdefault(t.container, []).append(t)
    process_by_container: dict[str, list[ProcessTuning]] = {}
    for pt in process_targets:
        if pt.container is not None:
            process_by_container.setdefault(pt.container, []).append(pt)
    watch_set = set(targets_by_container) | set(process_by_container)
    return targets_by_container, process_by_container, watch_set


async def _apply_for_container(
    docker: aiodocker.Docker,
    container: str,
    container_targets: list[Target],
    container_processes: list[ProcessTuning],
    retry_ladder: tuple[int, ...],
    retry_max_seconds: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    """Apply cgroup + process tunings for one container, then retry the
    process tunings on the ladder to catch late-spawned worker threads.

    Container-level ``docker update`` runs exactly once: cgroup state is
    persistent across the container's lifetime, so re-applying would be
    pure busywork. Process tunings re-run on each ladder step because
    new threads may have spawned since the last pass; the inner fast
    paths in ``apply_process_nice`` / ``apply_process_cpuset`` no-op
    when nothing has changed, so the cost is bounded.
    """
    try:
        for t in container_targets:
            await apply_target(docker, t, dry_run, log)
        for pt in container_processes:
            await apply_process_tuning(docker, pt, dry_run, log)
        if not container_processes:
            return
        start = time.monotonic()
        for delay in retry_ladder:
            # Check the cap BEFORE sleeping rather than after — a
            # ladder entry of e.g. 8s with cap=1s would otherwise still
            # tie up the apply chain for the full 8 seconds before
            # bailing.
            if time.monotonic() + delay - start > retry_max_seconds:
                log.debug(
                    "post-start retry for %s capped at %ds",
                    container,
                    retry_max_seconds,
                )
                return
            if delay > 0:
                await asyncio.sleep(delay)
            for pt in container_processes:
                await apply_process_tuning(docker, pt, dry_run, log)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 — top-level dispatch safety net
        log.exception("apply chain for %s failed", container)


async def _events_loop(
    docker: aiodocker.Docker,
    watch_set: set[str],
    targets_by_container: dict[str, list[Target]],
    process_by_container: dict[str, list[ProcessTuning]],
    retry_ladder: tuple[int, ...],
    retry_max_seconds: int,
    dry_run: bool,
    in_flight: dict[str, asyncio.Task[None]],
    stop: asyncio.Event,
    log: logging.Logger,
) -> None:
    """Subscribe to docker start events and dispatch the per-container
    apply chain when a watched container starts.

    A fresh start event for the same container cancels any in-flight
    apply chain for that container — PIDs from the prior incarnation
    are dead, and the new chain will re-apply against the fresh PIDs.
    """
    if not watch_set:
        # Nothing configured to listen for; the events task is a no-op.
        # Park on stop so the parent can still cancel us cleanly.
        await stop.wait()
        return

    log.info(
        "subscribing to docker start events for %d container(s): %s",
        len(watch_set),
        sorted(watch_set),
    )
    # ``Any`` because ``docker_events`` is annotated ``AsyncIterator[dict]``
    # but its body uses ``yield``, so the runtime object is an async
    # generator with ``aclose``. Typing as ``Any`` mirrors what
    # container_hooks does and avoids fighting the iterator-vs-generator
    # split for what's effectively a generator throughout.
    events_iter: Any = docker_events(docker, log, events=("start",)).__aiter__()
    stop_task = asyncio.create_task(stop.wait())
    next_task: asyncio.Task[Any] | None = None
    try:
        while not stop.is_set():
            next_task = asyncio.create_task(events_iter.__anext__())
            done, _pending = await asyncio.wait(
                {next_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done:
                next_task.cancel()
                next_task = None
                break
            try:
                event = await next_task
            except StopAsyncIteration:
                next_task = None
                break
            next_task = None
            attrs = event.get("Actor", {}).get("Attributes", {})
            container = attrs.get("name") or ""
            if container not in watch_set:
                continue

            # Cancel any in-flight ladder for this container — the start
            # event means we're now applying against a fresh PID set.
            prev = in_flight.pop(container, None)
            if prev is not None and not prev.done():
                prev.cancel()
            log.info("event_start: applying tuning to %s", container)
            task = asyncio.create_task(
                _apply_for_container(
                    docker,
                    container,
                    targets_by_container.get(container, []),
                    process_by_container.get(container, []),
                    retry_ladder,
                    retry_max_seconds,
                    dry_run,
                    log,
                )
            )
            in_flight[container] = task

            def _on_done(t: asyncio.Task[None], c: str = container) -> None:
                # Only clear our slot if it's still us — a successor
                # already replaced the mapping if it isn't.
                if in_flight.get(c) is t:
                    in_flight.pop(c, None)

            task.add_done_callback(_on_done)
    finally:
        if next_task is not None and not next_task.done():
            next_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await next_task
        if not stop_task.done():
            stop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stop_task
        with contextlib.suppress(Exception):
            await events_iter.aclose()


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


async def _reconcile_loop(
    docker: aiodocker.Docker,
    targets: list[Target],
    process_targets: list[ProcessTuning],
    host_process_targets: list[ProcessTuning],
    interval_seconds: int,
    dry_run: bool,
    stop: asyncio.Event,
    log: logging.Logger,
) -> None:
    """Periodic full reconcile: backstop for host PIDs, drift, and any
    start event the addon missed (e.g. while the events stream was
    reconnecting after a daemon hiccup).
    """
    while not stop.is_set():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        if stop.is_set():
            return
        log.debug("periodic reconcile pass")
        await _reconcile_pass(
            docker, targets, process_targets, host_process_targets, dry_run, log
        )


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
    retry_ladder = _parse_retry_ladder(options.get("post_start_retry_seconds"), log)
    retry_max_seconds = int(
        options.get(
            "post_start_retry_max_seconds", _DEFAULT_POST_START_RETRY_MAX_SECONDS
        )
    )
    if retry_max_seconds < 0:
        retry_max_seconds = 0

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

    targets_by_container, process_by_container, watch_set = _bucket_by_container(
        targets, process_targets
    )

    _cfg_lines = [
        "Configuration:",
        f"  apply_on_start:       {apply_on_start}",
        f"  dry_run:              {dry_run}",
        f"  reconcile_interval:   {interval_seconds}s",
        f"  post_start_retry:     {list(retry_ladder)} (cap {retry_max_seconds}s)",
        f"  events_watch:         {sorted(watch_set)}",
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
    in_flight: dict[str, asyncio.Task[None]] = {}
    events_task: asyncio.Task[None] | None = None
    reconcile_task: asyncio.Task[None] | None = None
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
            log.info("initial apply over all configured targets")
            await _reconcile_pass(
                docker, targets, process_targets, host_process_targets, dry_run, log
            )

        events_task = asyncio.create_task(
            _events_loop(
                docker,
                watch_set,
                targets_by_container,
                process_by_container,
                retry_ladder,
                retry_max_seconds,
                dry_run,
                in_flight,
                stop,
                log,
            )
        )
        reconcile_task = asyncio.create_task(
            _reconcile_loop(
                docker,
                targets,
                process_targets,
                host_process_targets,
                interval_seconds,
                dry_run,
                stop,
                log,
            )
        )

        await stop.wait()
    finally:
        # Stop the two long-running tasks first so they don't spawn new
        # work during shutdown. Then drain any in-flight per-container
        # apply chains before closing the docker client.
        for task in (events_task, reconcile_task):
            if task is not None and not task.done():
                task.cancel()
        if events_task is not None or reconcile_task is not None:
            await asyncio.gather(
                *(t for t in (events_task, reconcile_task) if t is not None),
                return_exceptions=True,
            )
        if in_flight:
            log.info("draining %d in-flight apply chain(s)", len(in_flight))
            for t in list(in_flight.values()):
                if not t.done():
                    t.cancel()
            await asyncio.gather(*in_flight.values(), return_exceptions=True)
        await docker.close()

    log.info("Shutting down")
    return 0


def main() -> int:
    return asyncio.run(main_async())
