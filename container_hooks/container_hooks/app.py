# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Main loop: optional initial sweep, then docker-events-driven dispatch.

Architecture: single ``asyncio`` event loop. The docker events stream
runs as an async iterator; each matching event is fanned out to a
concurrent task so a slow hook for one container doesn't block hooks
for another.

Per-container debounce (start events / post-start ``scripts/`` only)
is leading+trailing: the first event in a window fires immediately;
subsequent events in the same window arm a one-shot trailing fire so
the most recent restart isn't silently lost when no further event
arrives. Create events bypass debounce entirely — they fire once per
container lifecycle.

``last_run`` is updated synchronously *before* a dispatch task is
spawned, so two fast-following start events for the same container
don't both pass the leading-edge check.

Layout: everything for one container lives under
``<base_dir>/<container>/`` — see ``config.py`` for the path helpers
and the conventional subdirectory names.
"""

import argparse
import asyncio
import contextlib
import dataclasses
import datetime
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any

import aiodocker

from . import __version__
from .config import (
    Options,
    load_options,
    post_start_log,
    pre_start_files_dir,
    pre_start_log,
    pre_start_patches_dir,
    pre_start_scripts_dir,
    scripts_dir,
)
from .docker import (
    _async_append,
    apply_patch,
    docker_events,
    docker_ps_running,
    docker_url,
    put_archive_dir,
    run_hook,
    run_pre_start_hook,
    self_container_name,
)


def _lex_sorted_files(dir_path: Path, suffix: str) -> list[Path]:
    """Return regular files in ``dir_path`` with ``suffix`` extension, lex sorted."""
    if not dir_path.is_dir():
        return []
    return sorted(p for p in dir_path.iterdir() if p.suffix == suffix and p.is_file())


def _resolve_scripts(options: Options, container: str) -> list[Path]:
    """Lex-sorted ``*.sh`` files in ``<base>/<container>/scripts/``."""
    return _lex_sorted_files(scripts_dir(options, container), ".sh")


def _resolve_pre_start_scripts(options: Options, container: str) -> list[Path]:
    """Lex-sorted ``*.sh`` files in ``<base>/<container>/pre-start/``."""
    return _lex_sorted_files(pre_start_scripts_dir(options, container), ".sh")


def _resolve_pre_start_patches(options: Options, container: str) -> list[Path]:
    """Lex-sorted ``*.patch`` files in ``<base>/<container>/pre-start-patches/``."""
    return _lex_sorted_files(pre_start_patches_dir(options, container), ".patch")


def _resolve_pre_start_files_subdir(options: Options, container: str) -> Path | None:
    """Return ``<base>/<container>/pre-start-files`` if it exists and is non-empty."""
    candidate = pre_start_files_dir(options, container)
    if candidate.is_dir() and any(candidate.iterdir()):
        return candidate
    return None


def _resolve_debounce(options: Options, container: str) -> int:
    """Return the effective debounce window for ``container``.

    Per-container ``debounce_seconds`` in ``container_overrides`` takes
    precedence; otherwise falls back to the global ``debounce_seconds``.
    """
    for o in options.container_overrides:
        if o.container == container and o.debounce_seconds is not None:
            return o.debounce_seconds
    return options.debounce_seconds


def _max_debounce(options: Options) -> int:
    """Largest debounce window across the global default and all overrides."""
    overrides = (o.debounce_seconds or 0 for o in options.container_overrides)
    return max(options.debounce_seconds, *overrides, 1)


_LAST_RUN_PRUNE_INTERVAL = 256
_LAST_RUN_PRUNE_AGE_MULTIPLIER = 10

# Maximum concurrent dispatches against the docker socket. A burst of
# ``docker compose up`` creating 50 containers at once otherwise spawns
# 50 concurrent put_archive / exec calls — docker daemon happily accepts
# them but the addon's event loop loses responsiveness and the daemon
# socket starts queueing. 10 is enough to overlap dispatch latency
# without crowding the daemon; rest queue cleanly behind the semaphore.
_MAX_CONCURRENT_DISPATCHES = 10


def _prune_last_run(last_run: dict[str, float], options: Options, now: float) -> None:
    """Drop debounce entries older than ``10 × max_debounce`` seconds.

    ``last_run`` would otherwise grow once per ever-seen container; a
    long-lived addon on a churny host (CI agents creating + tearing down
    containers all day) would leak memory. The cutoff is well past any
    debounce window so live entries are never pruned.
    """
    cutoff = now - _LAST_RUN_PRUNE_AGE_MULTIPLIER * _max_debounce(options)
    stale = [k for k, t in last_run.items() if t < cutoff]
    for k in stale:
        del last_run[k]


async def _fire_trailing(
    container: str,
    delay: float,
    docker: aiodocker.Docker,
    options: Options,
    log: logging.Logger,
    spawn: Any,
    last_run: dict[str, float],
    trailing: dict[str, asyncio.Task[None]],
    trailing_event: dict[str, dict[str, Any]],
    stop: asyncio.Event,
) -> None:
    """Sleep ``delay`` seconds, then dispatch ``_dispatch`` with the
    most recent event stashed for this container.

    Two races to guard against, both of the same shape: the cancel call
    cannot preempt synchronous code once ``sleep`` has returned, so a
    cancel arriving in that microsecond window won't stop us from
    dispatching. The post-sleep guards below catch both:

    1. **Shutdown**: ``stop`` is set when the main loop tears down.
       Checked first because it's the cheapest test.
    2. **Outside-window supersede**: the main loop pops + cancels our
       trailing entry before firing its own leading dispatch. After
       our sleep returns, ``trailing.get(container)`` no longer points
       at us — bail rather than dispatch a duplicate.

    We compare task identity (``asyncio.current_task()``) rather than
    just key presence: a brand-new trailing could be armed for the
    same container during the race, and we still need to bail because
    that newer trailing is now the source of truth.
    """
    self_task = asyncio.current_task()
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        # Only clear our entry if it's still us — a successor task
        # registered mid-race shouldn't lose its registration.
        if trailing.get(container) is self_task:
            trailing.pop(container, None)
            trailing_event.pop(container, None)
        raise
    if stop.is_set():
        if trailing.get(container) is self_task:
            trailing.pop(container, None)
            trailing_event.pop(container, None)
        return
    if trailing.get(container) is not self_task:
        # Superseded by outside-window leading fire (or a successor
        # trailing armed against our successor's last_run). Either
        # way the dispatch for this restart already happened.
        return
    event = trailing_event.pop(container, None)
    trailing.pop(container, None)
    if event is None:
        # Shouldn't happen — main loop always sets trailing_event before
        # arming the task — but bail rather than dispatch a stale empty.
        return
    last_run[container] = time.monotonic()
    log.info("debounce: trailing fire for %s", container)
    spawn(
        _dispatch(
            docker,
            container,
            options,
            log,
            reason="event_start",
            event=event,
        )
    )


def _with_self_skip(options: Options, own_name: str) -> Options:
    """Return ``options`` with ``own_name`` unioned into ``skip_containers``."""
    if not own_name:
        return options
    return dataclasses.replace(
        options,
        skip_containers=tuple(set(options.skip_containers) | {own_name}),
    )


def _hook_env(
    container: str,
    reason: str,
    event: dict | None = None,
) -> dict[str, str]:
    """Env-var context handed to the hook script.

    Always includes ``ROCS_REASON`` and ``ROCS_CONTAINER``. When ``event``
    is supplied (from the docker events stream), also forwards the
    container id, image, and event timestamp so the script can branch
    on richer context without re-querying docker.
    """
    env: dict[str, str] = {
        "ROCS_REASON": reason,
        "ROCS_CONTAINER": container,
    }
    if event is not None:
        actor = event.get("Actor") or {}
        attrs = actor.get("Attributes") or {}
        env["ROCS_CONTAINER_ID"] = str(actor.get("ID") or "")
        env["ROCS_IMAGE"] = str(attrs.get("image") or "")
        if "time" in event:
            env["ROCS_TIMESTAMP"] = str(event["time"])
    return env


def _log_hook_result(
    log: logging.Logger,
    kind: str,
    name: str,
    container: str,
    result: Any,
) -> None:
    """One-line success/failure log for a single hook stage."""
    if result.returncode == 0:
        log.info(
            "%s %s for %s completed in %dms",
            kind,
            name,
            container,
            result.duration_ms,
        )
    else:
        log.warning(
            "%s %s for %s exited %d in %dms",
            kind,
            name,
            container,
            result.returncode,
            result.duration_ms,
        )


async def _log_dispatch_failure(
    log_path: Path,
    container: str,
    exc: BaseException,
    log: logging.Logger,
) -> None:
    """Surface a dispatch-level exception both to stderr and per-container log.

    Without this, exceptions escaping ``_dispatch`` / ``_dispatch_pre_start``
    only land in asyncio's task-GC log — an operator reading
    ``logs/post-start.log`` for their container would never know.
    """
    log.exception("dispatch failed for %s", container)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")
        await _async_append(
            log_path,
            f"[{ts}] DISPATCH FAILED for {container}: {type(exc).__name__}: {exc}\n",
        )
    except OSError:
        # If we can't write the log, the logger.exception above has it.
        pass


async def _dispatch(
    docker: aiodocker.Docker,
    container: str,
    options: Options,
    log: logging.Logger,
    reason: str,
    event: dict | None = None,
) -> None:
    """Resolve + run + log the post-start hook chain for one event.

    Debounce and skip checks happen in the caller before this is
    scheduled so concurrent dispatches for the same container can't
    race each other.
    """
    log_path = post_start_log(options, container)
    try:
        scripts = _resolve_scripts(options, container)
        if not scripts:
            log.debug("no post-start hook for %s", container)
            return

        env = _hook_env(container, reason, event)
        for script in scripts:
            log.info("running hook for %s (%s): %s", container, reason, script.name)
            result = await run_hook(docker, container, script, log_path, log, env=env)
            _log_hook_result(log, "hook", script.name, container, result)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — top-level dispatch safety net
        await _log_dispatch_failure(log_path, container, e, log)


async def _dispatch_pre_start(
    docker: aiodocker.Docker,
    container: str,
    options: Options,
    log: logging.Logger,
    event: dict,
) -> None:
    """Run pre-start hooks for a ``create`` event.

    Fires (in order) the put_archive of ``pre-start-files/``, the lex-sorted
    ``pre-start-patches/*.patch``, then the lex-sorted ``pre-start/*.sh``
    scripts. Fast paths run first so any time-critical staging completes
    before the slower bash + docker CLI script path.
    """
    log_path = pre_start_log(options, container)
    env = _hook_env(container, "container_created", event)
    try:
        files_subdir = _resolve_pre_start_files_subdir(options, container)
        if files_subdir is not None:
            log.info(
                "running pre-start put_archive for %s: %s",
                container,
                files_subdir,
            )
            result = await put_archive_dir(
                docker, container, files_subdir, log_path, log
            )
            _log_hook_result(log, "put_archive", "", container, result)

        patches = _resolve_pre_start_patches(options, container)
        for patch_file in patches:
            log.info("applying pre-start patch for %s: %s", container, patch_file.name)
            result = await apply_patch(docker, container, patch_file, log_path, log)
            _log_hook_result(log, "patch", patch_file.name, container, result)

        scripts = _resolve_pre_start_scripts(options, container)
        if not scripts and files_subdir is None and not patches:
            log.debug("no pre-start hook for %s", container)
            return

        for script in scripts:
            log.info("running pre-start hook for %s: %s", container, script.name)
            result = await run_pre_start_hook(container, script, log_path, log, env=env)
            _log_hook_result(log, "pre-start hook", script.name, container, result)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — top-level dispatch safety net
        await _log_dispatch_failure(log_path, container, e, log)


async def main_async() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--options", default="/data/options.json")
    args = parser.parse_args()

    options = load_options(args.options)

    logging.basicConfig(
        level=getattr(logging, options.log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("container_hooks")
    log.info("Container Hooks v%s starting", __version__)

    options.base_dir.mkdir(parents=True, exist_ok=True)

    # last_run tracks the most recent dispatch time per container, used
    # for debounce. Lives in-process; cleared on addon restart. Pruned
    # periodically (see _prune_last_run) so it can't grow unbounded on
    # a churny host.
    last_run: dict[str, float] = {}
    events_since_prune = 0
    # Trailing-edge debounce state. ``trailing`` holds the pending fire
    # task per container; ``trailing_event`` holds the latest in-window
    # event payload so the trailing fire carries fresh metadata, not the
    # original t=1 event.
    trailing: dict[str, asyncio.Task[None]] = {}
    trailing_event: dict[str, dict[str, Any]] = {}

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    in_flight: set[asyncio.Task] = set()

    # Signal escalation: first SIGTERM/SIGINT politely sets `stop` (the
    # main loop notices and drops into the drain phase). A second signal
    # while we're still draining cancels in-flight tasks so gather()
    # returns immediately. A third forces a hard exit, since at that
    # point something is wedged and the operator has been clear about
    # wanting out.
    signal_count = 0

    def _on_signal() -> None:
        nonlocal signal_count
        signal_count += 1
        if signal_count == 1:
            stop.set()
        elif signal_count == 2:
            log.warning(
                "second signal received; cancelling %d in-flight tasks",
                len(in_flight),
            )
            for t in list(in_flight):
                t.cancel()
        else:
            log.warning("third signal received; hard-exiting")
            os._exit(1)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    docker = aiodocker.Docker(url=docker_url())

    # Resolve our own docker name now that we have a Docker client.
    # gethostname returns the short container ID (12 hex), but the
    # events stream reports the full ``app_<slug>_<name>`` form, so
    # we must look up the ID → name via the API before set-membership
    # can match. Without this, the self-skip is silently broken.
    own = await self_container_name(docker)
    if not own:
        log.warning(
            "could not resolve own container name; self-skip is disabled, "
            "this addon may dispatch against itself during initial_sweep"
        )
    options = _with_self_skip(options, own)

    log.info(
        "Configuration: base_dir=%s initial_sweep=%s debounce_seconds=%d skip=%s",
        options.base_dir,
        options.initial_sweep,
        options.debounce_seconds,
        sorted(options.skip_containers),
    )
    dispatch_sem = asyncio.Semaphore(_MAX_CONCURRENT_DISPATCHES)

    async def _gated(coro):
        async with dispatch_sem:
            await coro

    def spawn(coro):
        task = asyncio.create_task(_gated(coro))
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)
        return task

    events_iter: Any = None
    stop_task: asyncio.Task | None = None
    next_task: asyncio.Task | None = None
    try:
        if options.initial_sweep:
            log.info("initial sweep over running containers")
            for container in await docker_ps_running(docker, log):
                if stop.is_set():
                    break
                if container in options.skip_containers:
                    continue
                spawn(
                    _dispatch(docker, container, options, log, reason="initial_sweep")
                )

        if stop.is_set():
            return 0

        # ``create`` fires the pre-start hook path (files / patches /
        # pre-start scripts run before the target's entrypoint);
        # ``start`` fires the post-start ``scripts/`` path. Always
        # subscribe to both — whether a particular dispatch produces
        # any work is decided per-event by the per-container filesystem
        # scan in ``_resolve_*``.
        events_to_watch: tuple[str, ...] = ("create", "start")
        log.info(
            "subscribing to docker container events: %s",
            ",".join(events_to_watch),
        )
        # Wrap the async iteration so SIGTERM unblocks even when the
        # daemon is idle. Plain ``async for`` would park on
        # ``subscriber.get()`` indefinitely; we race the next event
        # against ``stop`` so a signal wakes us within the next event
        # or immediately if one is already pending.
        events_iter = docker_events(docker, log, events=events_to_watch).__aiter__()
        stop_task = asyncio.create_task(stop.wait())
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
            if not container:
                log.debug("event missing container name; ignoring: %s", event)
                continue
            if container in options.skip_containers:
                log.debug("skip_containers match: %s", container)
                continue
            action = event.get("Action") or "start"
            if action == "create":
                # create events are one-shot per container lifecycle; no
                # debounce check.
                spawn(_dispatch_pre_start(docker, container, options, log, event=event))
            else:
                now = time.monotonic()
                debounce = _resolve_debounce(options, container)
                prev = last_run.get(container, 0.0)
                if debounce and now - prev < debounce:
                    # In window: leading fire already happened. Arm or
                    # refresh a trailing fire so the most recent restart
                    # isn't silently lost when no further event arrives.
                    # If a trailing is already armed, just refresh the
                    # stashed event — the timer was set against the
                    # leading fire and shouldn't slide forward.
                    trailing_event[container] = event
                    if container not in trailing:
                        delay = debounce - (now - prev)
                        trailing[container] = asyncio.create_task(
                            _fire_trailing(
                                container,
                                delay,
                                docker,
                                options,
                                log,
                                spawn,
                                last_run,
                                trailing,
                                trailing_event,
                                stop,
                            )
                        )
                    log.debug(
                        "debounce: skipping leading-fire for %s "
                        "(%.1fs since last run, window=%ds, trailing armed)",
                        container,
                        now - prev,
                        debounce,
                    )
                    continue
                # Outside window: any armed trailing is stale (its
                # dispatch would be redundant with the one we're about
                # to spawn). Cancel before firing.
                pending = trailing.pop(container, None)
                if pending is not None:
                    pending.cancel()
                    trailing_event.pop(container, None)
                last_run[container] = now
                events_since_prune += 1
                if events_since_prune >= _LAST_RUN_PRUNE_INTERVAL:
                    events_since_prune = 0
                    _prune_last_run(last_run, options, now)
                spawn(
                    _dispatch(
                        docker,
                        container,
                        options,
                        log,
                        reason="event_start",
                        event=event,
                    )
                )

    finally:
        # Cancel pending trailing-debounce timers FIRST, before any
        # ``await`` that yields to the loop — otherwise an expired
        # timer can run ``_fire_trailing`` to completion during the
        # await for ``next_task``/``stop_task`` or ``events_iter.aclose()``
        # and spawn a dispatch that then runs during the in_flight drain.
        # ``stop`` is set by this point so ``_fire_trailing``'s race
        # guard would catch a timer firing on the same tick as the
        # cancel; the synchronous cancel here covers everything else.
        if trailing:
            log.info("cancelling %d pending trailing debounce timers", len(trailing))
            for trail_task in trailing.values():
                trail_task.cancel()
            await asyncio.gather(*trailing.values(), return_exceptions=True)
            trailing.clear()
            trailing_event.clear()
        # Cancel and await any pending event-loop helper tasks before
        # the drain, so they don't surface as "task was destroyed but
        # pending" warnings at shutdown.
        for t in (next_task, stop_task):
            if t is not None and not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await t
        # Close the events async-generator so aiodocker's subscriber is
        # released back to the daemon before ``docker.close()`` tears
        # down the underlying connection.
        if events_iter is not None:
            with contextlib.suppress(Exception):
                await events_iter.aclose()
        # Drain in-flight dispatches before closing the docker client —
        # ``docker.close()`` invalidates open connections, and any task
        # mid-exec/put_archive would raise.
        if in_flight:
            log.info("draining %d in-flight tasks before exit", len(in_flight))
            results = await asyncio.gather(*in_flight, return_exceptions=True)
            cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
            if cancelled:
                log.warning(
                    "%d/%d in-flight tasks were cancelled mid-flight",
                    cancelled,
                    len(results),
                )
        await docker.close()

    log.info("Container Hooks exiting")
    return 0


def main() -> int:
    return asyncio.run(main_async())
