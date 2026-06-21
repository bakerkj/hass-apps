# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Docker integration via aiodocker.

All daemon I/O goes through a shared ``aiodocker.Docker`` client. The
container-level ``docker update`` (cpuset / cpu-shares / blkio-weight)
translates to a ``POST /containers/{id}/update`` over the unix socket;
``docker top`` translates to ``container.top(...)``.

``docker_events`` exposes the daemon's lifecycle stream as an async
iterator — the fast-path that lets the addon apply tuning shortly
after a target container starts rather than waiting for the next
periodic reconcile tick.

Exception handling here is intentionally broader than ``DockerError``:
``aiohttp``-level transport errors, ``OSError`` (socket gone), and
``asyncio.TimeoutError`` all map to "log + skip this pass" so a
transient daemon hiccup (HA Supervisor restart, container daemon
crash-and-respawn) doesn't crash the reconcile loop.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiodocker
import aiohttp
from aiodocker.exceptions import DockerError

from .config import Target, cpuset_matches

# Class of transient transport errors that should NOT propagate out of
# the docker.py helpers. Broader than ``DockerError`` because aiodocker
# leaks aiohttp/asyncio exceptions on socket-level issues — pre-refactor
# the subprocess shim returned a non-zero rc instead of raising, and
# the reconcile loop relied on that "log + continue" surface.
_TRANSIENT_DAEMON_ERRORS: tuple[type[BaseException], ...] = (
    DockerError,
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
)

# Backoff between reconnect attempts when the events stream exits.
# Keeps the loop from spinning at full speed when the daemon socket is
# unreachable; doubles on consecutive failures up to the cap.
_RECONNECT_BACKOFF_INITIAL_SECONDS = 1.0
_RECONNECT_BACKOFF_MAX_SECONDS = 30.0


def docker_url() -> str:
    """Return the unix-socket URL for the docker daemon.

    HA addons see the daemon socket at ``/run/docker.sock`` when
    ``docker_api: true`` and protection mode is off. Falls back to
    ``/var/run/docker.sock`` in case the base image symlinks the other
    way.
    """
    for candidate in ("/run/docker.sock", "/var/run/docker.sock"):
        if Path(candidate).exists():
            return f"unix://{candidate}"
    return "unix:///run/docker.sock"


async def docker_inspect_limits(
    docker: aiodocker.Docker,
    container: str,
    log: logging.Logger,
) -> tuple[aiodocker.containers.DockerContainer, dict[str, Any]] | None:
    """Resolve ``container`` and read its current cgroup limits.

    Returns ``(ctr, limits)`` so the caller can hand the resolved
    ``DockerContainer`` straight to the update path without paying a
    second ``containers.get`` round-trip. Returns ``None`` on any
    transient daemon issue — the caller treats that the same as
    "skip this target this pass."
    """
    try:
        ctr = await docker.containers.get(container)
        info = await ctr.show()
    except _TRANSIENT_DAEMON_ERRORS as e:
        log.warning("docker inspect failed for %s: %s", container, e)
        return None

    host_cfg = info.get("HostConfig") or {}
    if not isinstance(host_cfg, dict):
        host_cfg = {}

    limits = {
        "cpuset_cpus": str(host_cfg.get("CpusetCpus") or ""),
        "cpu_shares": int(host_cfg.get("CpuShares") or 0),
        "blkio_weight": int(host_cfg.get("BlkioWeight") or 0),
    }
    return ctr, limits


def desired_update_kwargs(target: Target, current: dict[str, Any]) -> dict[str, Any]:
    """Build ``{field: new_value}`` for fields that differ from ``current``.

    Keys are the same snake_case names used everywhere else in this
    module (``Target`` attributes, the ``current`` dict from
    ``docker_inspect_limits``, the schema in ``config.json``). The
    only spot the docker engine's CamelCase form appears is at the
    ``_query_json`` boundary in ``apply_target`` — see ``_to_api_key``.
    """
    out: dict[str, Any] = {}

    if target.cpuset_cpus is not None and not cpuset_matches(
        str(current.get("cpuset_cpus", "")), target.cpuset_cpus
    ):
        out["cpuset_cpus"] = target.cpuset_cpus

    if target.cpu_shares is not None and int(current.get("cpu_shares", 0)) != int(
        target.cpu_shares
    ):
        out["cpu_shares"] = int(target.cpu_shares)

    if target.blkio_weight is not None and int(current.get("blkio_weight", 0)) != int(
        target.blkio_weight
    ):
        out["blkio_weight"] = int(target.blkio_weight)

    return out


def _to_api_key(snake: str) -> str:
    """``cpu_shares`` → ``CpuShares``. The one spot the engine API's
    CamelCase form needs to appear: encoding the update body for the
    daemon. Everywhere else uses snake_case."""
    return "".join(part.title() for part in snake.split("_"))


def _format_target_state(
    target: Target,
    current: dict[str, Any],
    kwargs: dict[str, Any],
) -> str:
    """Render a one-line state summary for an apply log message.

    Fields the target doesn't set are skipped. Fields actually
    changing (present in ``kwargs``) render as ``before → after``;
    fields already at the target value render as the plain current
    value. Empty cpuset renders as ``∅`` on both sides.
    """
    parts: list[str] = []
    if target.cpuset_cpus is not None:
        cur = current.get("cpuset_cpus", "") or "∅"
        if "cpuset_cpus" in kwargs:
            parts.append(f"cpuset_cpus {cur} → {kwargs['cpuset_cpus'] or '∅'}")
        else:
            parts.append(f"cpuset_cpus {cur}")
    if target.cpu_shares is not None:
        cur = current.get("cpu_shares", 0)
        if "cpu_shares" in kwargs:
            parts.append(f"cpu_shares {cur} → {kwargs['cpu_shares']}")
        else:
            parts.append(f"cpu_shares {cur}")
    if target.blkio_weight is not None:
        cur = current.get("blkio_weight", 0)
        if "blkio_weight" in kwargs:
            parts.append(f"blkio_weight {cur} → {kwargs['blkio_weight']}")
        else:
            parts.append(f"blkio_weight {cur}")
    return ", ".join(parts)


async def apply_target(
    docker: aiodocker.Docker,
    target: Target,
    dry_run: bool,
    log: logging.Logger,
    *,
    log_no_change: bool = False,
) -> None:
    """Apply container-level cgroup tuning to ``target``.

    ``log_no_change``: when ``True``, surface the "already at desired
    state" case at INFO instead of DEBUG. The startup initial-apply
    path sets True so the operator sees a confirmation line for every
    configured target even when the cgroup is already correct; the
    event-driven fast path and the periodic reconcile leave it False
    to keep the log quiet between actual changes (an event firing 49
    times during a compose-up restart cycle would otherwise produce
    49 noise lines per cycle).
    """
    inspect = await docker_inspect_limits(docker, target.container, log)
    if inspect is None:
        return
    ctr, current = inspect

    kwargs = desired_update_kwargs(target, current)
    if not kwargs:
        if log_no_change:
            log.info(
                "%s: %s",
                target.container,
                _format_target_state(target, current, kwargs),
            )
        else:
            log.debug(
                "No container-level changes needed for container=%s", target.container
            )
        return

    if dry_run:
        log.info(
            "DRY RUN: %s: %s",
            target.container,
            _format_target_state(target, current, kwargs),
        )
        return

    try:
        # aiodocker 0.27 doesn't wrap ``POST /containers/{id}/update``;
        # call the engine API directly via the low-level ``_query_json``.
        # Reuses the ``DockerContainer`` resolved by docker_inspect_limits
        # so we don't pay a second ``containers.get`` round-trip per pass.
        # ``_to_api_key`` is the one spot snake_case becomes CamelCase.
        result = await docker._query_json(
            f"containers/{ctr.id}/update",
            method="POST",
            data={_to_api_key(k): v for k, v in kwargs.items()},
        )
    except _TRANSIENT_DAEMON_ERRORS as e:
        log.error("docker update failed for %s: %s", target.container, e)
        return

    # The engine returns ``{"Warnings": [...]}`` for non-fatal partial
    # applies — e.g. setting a v1-only cgroup parameter on a v2 host
    # comes back 200 OK with a warning explaining the value was ignored.
    # Surfacing them turns a silent partial-success into something an
    # operator can act on.
    if result and result.get("Warnings"):
        for w in result["Warnings"]:
            log.warning("docker update warning for %s: %s", target.container, w)

    log.info(
        "%s: %s",
        target.container,
        _format_target_state(target, current, kwargs),
    )


async def apply_all(
    docker: aiodocker.Docker,
    targets: list[Target],
    dry_run: bool,
    log: logging.Logger,
    *,
    log_no_change: bool = False,
) -> None:
    """``log_no_change`` propagates to each ``apply_target`` call — the
    initial-apply path in ``main_async`` sets True so the operator sees
    startup confirmation that every configured target is already
    correct; periodic reconcile leaves it False to keep the log quiet.
    """
    for target in targets:
        await apply_target(docker, target, dry_run, log, log_no_change=log_no_change)


async def docker_events(
    docker: aiodocker.Docker,
    log: logging.Logger,
    events: tuple[str, ...] = ("start",),
) -> AsyncIterator[dict]:
    """Async iterator yielding matching container lifecycle events.

    Subscribes without daemon-side filters and does the action filter
    in Python. Daemon-side filters would work, but lifecycle event
    volume is low and the Python filter keeps the call shape identical
    to the ``container_hooks`` source this was lifted from.

    Auto-reconnects with exponential backoff on socket / daemon
    hiccups. The caller is expected to handle its own shutdown signal
    and stop awaiting.
    """
    wanted = set(events)
    backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
    while True:
        saw_event = False
        subscriber = None
        try:
            subscriber = docker.events.subscribe()
            while True:
                event = await subscriber.get()
                if event is None:
                    break
                if event.get("Type") != "container":
                    continue
                saw_event = True
                # Trace every container event so an operator who flips
                # ``log_level: DEBUG`` can confirm the stream is live
                # without adding temporary instrumentation. Matches the
                # container_hooks docker.py source pattern this was
                # lifted from.
                action = event.get("Action")
                log.debug(
                    "rx event Action=%s name=%s",
                    action,
                    event.get("Actor", {}).get("Attributes", {}).get("name", ""),
                )
                if action in wanted:
                    yield event
        except asyncio.CancelledError:
            # Drop the subscriber before re-raising so its queue is
            # detached from the channel immediately on shutdown
            # rather than waiting for the next GC cycle.
            if subscriber is not None:
                del subscriber
            raise
        except Exception as e:  # noqa: BLE001 — aiohttp.ClientError must also retry
            log.warning(
                "docker events stream error: %s; reconnecting in %.1fs",
                e,
                backoff,
            )
        else:
            log.warning("docker events stream ended; reconnecting in %.1fs", backoff)
        # Drop the subscriber before the backoff sleep so aiodocker's
        # ChannelSubscriber.__del__ removes its queue from
        # ``channel.queues`` immediately. aiodocker 0.27 has no
        # ``unsubscribe`` / ``__aexit__`` surface, so ``del`` is the
        # documented exit.
        if subscriber is not None:
            del subscriber
        if saw_event:
            backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SECONDS)


async def docker_top_processes(
    docker: aiodocker.Docker,
    container: str,
    log: logging.Logger,
) -> list[tuple[int, str]]:
    """``docker top -eo pid,args``-equivalent via aiodocker.

    Returns a list of ``(host_pid, cmdline)``. The daemon returns the
    process list as a table with ``Titles`` + ``Processes``; we ask for
    the ``pid,args`` projection and parse the two columns positionally.
    """
    try:
        ctr = await docker.containers.get(container)
        payload = await ctr.top(ps_args="-eo pid,args")
    except _TRANSIENT_DAEMON_ERRORS as e:
        log.warning(
            "Failed to list processes in container=%s: %s",
            container,
            e,
        )
        return []

    titles = payload.get("Titles") or []
    rows_raw = payload.get("Processes") or []
    if not titles or not rows_raw:
        return []

    # The daemon may upper-case the column headers ("PID", "COMMAND") —
    # find the indices we want positionally so the parser is robust to
    # casing differences across docker versions.
    title_norm = [str(t).strip().lower() for t in titles]
    try:
        pid_idx = title_norm.index("pid")
    except ValueError:
        log.warning(
            "docker top response missing PID column for container=%s", container
        )
        return []
    # ``docker top`` collapses whitespace into the final field, so the
    # cmdline column is normally last; but if a daemon ever reorders
    # the headers (e.g. ``[COMMAND, PID]``) and we fell back to
    # ``len(titles) - 1`` blindly, ``pid_idx`` and ``cmd_idx`` would
    # collide on the same slot and every row would be parsed with the
    # PID value used as the cmdline. Search by name first, last-column
    # only as a fallback.
    try:
        cmd_idx = next(
            i for i, t in enumerate(title_norm) if t in ("args", "command", "cmd")
        )
    except StopIteration:
        cmd_idx = len(titles) - 1

    rows: list[tuple[int, str]] = []
    for raw in rows_raw:
        if not isinstance(raw, list) or len(raw) <= max(pid_idx, cmd_idx):
            continue
        pid_str = str(raw[pid_idx]).strip()
        if not pid_str.isdigit():
            continue
        cmdline = str(raw[cmd_idx]).strip()
        rows.append((int(pid_str), cmdline))

    return rows
