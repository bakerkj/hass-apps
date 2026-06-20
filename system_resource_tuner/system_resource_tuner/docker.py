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
import shlex
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
    """Build the JSON body for ``POST /containers/{id}/update``.

    Only includes fields that differ from ``current`` — sending a
    no-change kwargs dict to the daemon would still bump cgroup state,
    so the per-field diff check keeps the round-trip a true no-op when
    everything matches.

    Keys are CamelCase to match the docker engine API directly; the
    DRY RUN log line synthesizes a CLI rendering from this dict.
    """
    kwargs: dict[str, Any] = {}

    if target.cpuset_cpus is not None and not cpuset_matches(
        str(current.get("cpuset_cpus", "")), target.cpuset_cpus
    ):
        kwargs["CpusetCpus"] = target.cpuset_cpus

    if target.cpu_shares is not None and int(current.get("cpu_shares", 0)) != int(
        target.cpu_shares
    ):
        kwargs["CpuShares"] = int(target.cpu_shares)

    if target.blkio_weight is not None and int(current.get("blkio_weight", 0)) != int(
        target.blkio_weight
    ):
        kwargs["BlkioWeight"] = int(target.blkio_weight)

    return kwargs


def _kwargs_to_cli_form(container: str, kwargs: dict[str, Any]) -> str:
    """Render an update kwargs dict as a ``docker update`` CLI line.

    Used only for the human-readable DRY RUN log message. The kwargs
    dict is the source of truth for the actual update body.
    """
    parts = ["docker", "update"]
    if "CpusetCpus" in kwargs:
        parts += ["--cpuset-cpus", str(kwargs["CpusetCpus"])]
    if "CpuShares" in kwargs:
        parts += ["--cpu-shares", str(kwargs["CpuShares"])]
    if "BlkioWeight" in kwargs:
        parts += ["--blkio-weight", str(kwargs["BlkioWeight"])]
    parts.append(container)
    return " ".join(shlex.quote(p) for p in parts)


async def apply_target(
    docker: aiodocker.Docker,
    target: Target,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    inspect = await docker_inspect_limits(docker, target.container, log)
    if inspect is None:
        return
    ctr, current = inspect

    kwargs = desired_update_kwargs(target, current)
    if not kwargs:
        log.debug(
            "No container-level changes needed for container=%s", target.container
        )
        return

    if dry_run:
        log.info("DRY RUN: %s", _kwargs_to_cli_form(target.container, kwargs))
        return

    try:
        # aiodocker 0.27 doesn't wrap ``POST /containers/{id}/update``;
        # call the engine API directly via the low-level ``_query_json``.
        # Reuses the ``DockerContainer`` resolved by docker_inspect_limits
        # so we don't pay a second ``containers.get`` round-trip per pass.
        result = await docker._query_json(
            f"containers/{ctr.id}/update",
            method="POST",
            data=kwargs,
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

    log.info("docker update ok for %s", target.container)


async def apply_all(
    docker: aiodocker.Docker,
    targets: list[Target],
    dry_run: bool,
    log: logging.Logger,
) -> None:
    for target in targets:
        await apply_target(docker, target, dry_run, log)


async def docker_events(
    docker: aiodocker.Docker,
    log: logging.Logger,
    events: tuple[str, ...] = ("start",),
) -> AsyncIterator[dict]:
    """Async iterator yielding matching container lifecycle events.

    Filters by ``Action`` in Python because docker treats multiple
    ``event=`` filter values as a logical AND, not OR. Auto-reconnects
    with exponential backoff on socket / daemon hiccups. The caller is
    expected to handle its own shutdown signal and stop awaiting.
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
                action = event.get("Action")
                if action in wanted:
                    yield event
        except asyncio.CancelledError:
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
