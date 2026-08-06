# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Signal K delta websocket subscriber.

Replaces the HTTP snapshot poller in :mod:`client` for the live vessel tree:
a background coroutine subscribes to Signal K's ``/signalk/v1/stream`` and
applies each incoming ``update`` to an in-memory nested dict shaped like the
polled ``vessels/self`` output. Downstream code (``flatten_with_meta`` and
the entity resolvers) then reads a coherent snapshot per publish tick.

Kept as its own module so the reconnect / bootstrap / lock discipline lives
in one place: :func:`app.main_async` only starts / stops it and reads
:meth:`WSSubscriber.snapshot`.
"""

import asyncio
import contextlib
import copy
import json
import logging
import time
from typing import Any

import websockets
import websockets.exceptions

log = logging.getLogger(__name__)

_STREAM_PATH_SELF = "/signalk/v1/stream?subscribe=self"
_STREAM_PATH_ALL = "/signalk/v1/stream?subscribe=all"

_MMSI_PREFIX = "vessels.urn:mrn:imo:mmsi:"
_ATOM_PREFIX = "atoms."


def _ws_url(base_url: str, *, include_ais: bool = False) -> str:
    """HTTP(S) SK base URL → matching WS(S) stream URL.

    ``include_ais`` widens the subscription from ``vessels.self`` to every
    context SK is publishing, so AIS targets and atoms (AtoNs / MetHydro /
    base stations) flow to the same reader.
    """
    path = _STREAM_PATH_ALL if include_ais else _STREAM_PATH_SELF
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :].rstrip("/") + path
    if base_url.startswith("http://"):
        return "ws://" + base_url[len("http://") :].rstrip("/") + path
    return base_url.rstrip("/") + path


class WSSubscriber:
    """Maintains a live mirror of ``vessels/self`` from SK delta messages.

    ``bootstrap`` seeds the tree from a fresh REST snapshot; deltas only patch
    values in place, so the tree schema (meta, ``values``, ``$source``) stays
    consistent with what the entity resolvers expect. Reads are served from a
    deep copy so callers can iterate without worrying about mid-walk mutation.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        *,
        include_ais: bool = False,
    ) -> None:
        self._url = _ws_url(base_url, include_ais=include_ais)
        self._token = token
        self._include_ais = include_ais
        self._tree: dict[str, Any] = {}
        # AIS targets keyed by MMSI string, each value a nested tree in the
        # same shape as ``_tree`` so the existing flatten/resolve pipeline
        # can be reused per target.
        self._ais: dict[str, dict[str, Any]] = {}
        # Atoms (AtoNs, MetHydro, base stations) keyed by SK id fragment.
        self._atoms: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._dirty: set[str] = set()
        self._dirty_ais: set[str] = set()
        self._last_delta_monotonic: float = 0.0
        # Signals that the tree has been seeded so the publish loop knows
        # it can start; independent from the WS connection status so a
        # WS blip doesn't invalidate the bootstrap.
        self._ready = asyncio.Event()

    def bootstrap(self, tree: dict[str, Any]) -> None:
        """Seed the internal tree from a REST snapshot before deltas start.

        Safe to call before :meth:`start`; unsafe after (the WS task owns the
        tree from that point). Callers that miss the pre-start window should
        just let the deltas populate lazily -- a value comes in on the first
        delta after subscribe.
        """
        self._tree = tree
        self._ready.set()

    async def start(self) -> None:
        """Spawn the reader task. Idempotent."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="signalk-ws")

    async def stop(self) -> None:
        """Signal the reader to exit and await it.

        The reader coroutine has a stop-race inside its recv loop so this
        returns promptly (typically well under 1s) rather than eating the
        full timeout on a quiet server. Any exception raised by the task
        during teardown is swallowed -- the process is exiting and there
        is nothing useful to do with it.
        """
        self._stop.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=5.0)
        except TimeoutError:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reader errors on shutdown are moot
            log.debug("WS reader exited with %r", exc)

    async def snapshot(self) -> dict[str, Any]:
        """Return a deep-copied snapshot of the current tree.

        Deep-copied so downstream flatten / staleness walks never race a delta
        mid-traverse. The tree is small (order of a few hundred leaves), so
        copy cost is sub-millisecond in practice.
        """
        async with self._lock:
            return copy.deepcopy(self._tree)

    async def ais_snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a deep-copied snapshot of all AIS targets.

        Empty if ``include_ais`` was not set at construction.
        """
        async with self._lock:
            return copy.deepcopy(self._ais)

    async def take_dirty_ais(self) -> set[str]:
        """Return the set of MMSIs updated since the last call, then clear it."""
        async with self._lock:
            d, self._dirty_ais = self._dirty_ais, set()
            return d

    async def take_dirty(self) -> set[str]:
        """Return the set of paths updated since the last call, then clear it.

        Lets the publish tick skip resolver work when nothing changed and,
        later, drive per-path publish decisions without walking the whole
        tree.
        """
        async with self._lock:
            d, self._dirty = self._dirty, set()
            return d

    def last_delta_monotonic(self) -> float:
        """Monotonic seconds of the most recent applied delta.

        Zero if no delta has arrived yet. Used by the staleness / bus-health
        logic to detect a silent WS (connected but nothing flowing).
        """
        return self._last_delta_monotonic

    async def _run(self) -> None:
        backoff = 1.0
        headers = [("Authorization", f"Bearer {self._token}")] if self._token else None
        stop_task = asyncio.create_task(self._stop.wait(), name="signalk-ws-stop")
        try:
            while not self._stop.is_set():
                try:
                    async with websockets.connect(
                        self._url,
                        additional_headers=headers,
                        ping_interval=30,
                        ping_timeout=10,
                        max_size=2**20,
                    ) as ws:
                        log.info("Signal K WS connected: %s", self._url)
                        backoff = 1.0
                        # Race each recv against ``stop`` so a quiet server
                        # (test fixtures, real SK during a lull between
                        # pings) can't hold shutdown for the ping window.
                        # ``async for raw in ws:`` alone only checked stop
                        # after each incoming frame.
                        while not self._stop.is_set():
                            recv_task = asyncio.create_task(
                                ws.recv(), name="signalk-ws-recv"
                            )
                            done, _ = await asyncio.wait(
                                {recv_task, stop_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if stop_task in done:
                                recv_task.cancel()
                                with contextlib.suppress(
                                    asyncio.CancelledError, Exception
                                ):
                                    await recv_task
                                break
                            try:
                                raw = recv_task.result()
                            except websockets.exceptions.ConnectionClosed:
                                break
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError as exc:
                                log.debug("SK WS: bad JSON: %s", exc)
                                continue
                            await self._apply(msg)
                except (
                    websockets.exceptions.WebSocketException,
                    OSError,
                ) as exc:
                    log.warning("Signal K WS: %s; reconnecting in %.1fs", exc, backoff)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Signal K WS: unexpected error; reconnecting")
                # Backoff between reconnects, but wake early on stop.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                backoff = min(backoff * 2, 30.0)
        finally:
            stop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stop_task

    async def _apply(self, msg: dict[str, Any]) -> None:
        """Fold one SK delta message into the appropriate store.

        SK delta format::

            {"context": "vessels.self", "updates": [
              {"$source": "...", "timestamp": "...", "values": [
                {"path": "electrical.batteries.0.voltage", "value": 12.75}
              ]}
            ]}

        Routes by ``context``: ``vessels.self`` patches the self tree,
        ``vessels.urn:mrn:imo:mmsi:*`` patches the per-MMSI AIS store,
        ``atoms.*`` patches the atoms store. Any other context is dropped
        rather than silently absorbed into self, which would corrupt the
        entity resolver's view of the boat.
        """
        context = msg.get("context")
        target_tree, dirty_set, dirty_key = self._route(context)
        if target_tree is None:
            return
        updates = msg.get("updates") or []
        if not isinstance(updates, list):
            return
        async with self._lock:
            for u in updates:
                if not isinstance(u, dict):
                    continue
                ts = u.get("timestamp") if isinstance(u.get("timestamp"), str) else None
                src = u.get("$source")
                if not isinstance(src, str):
                    src_meta = u.get("source")
                    if isinstance(src_meta, dict):
                        label = src_meta.get("label")
                        src = label if isinstance(label, str) else None
                    else:
                        src = None
                for v in u.get("values") or []:
                    if not isinstance(v, dict):
                        continue
                    path = v.get("path")
                    if not isinstance(path, str) or not path:
                        continue
                    self._set_leaf_in(target_tree, path, v.get("value"), ts, src)
                    if dirty_set is self._dirty:
                        dirty_set.add(path)
                    elif dirty_key is not None and dirty_set is not None:
                        dirty_set.add(dirty_key)
            self._last_delta_monotonic = time.monotonic()

    def _route(
        self, context: Any
    ) -> tuple[dict[str, Any] | None, set[str] | None, str | None]:
        """Pick the destination tree + dirty tracker for a delta context.

        Returns ``(tree, dirty_set, dirty_key)`` where ``dirty_key`` is a
        per-target key (MMSI, atom id) for the multi-target stores, or
        ``None`` for the self tree (which uses per-path dirty). Returns
        ``(None, None, None)`` for contexts we don't accept.
        """
        if context in (None, "vessels.self"):
            return self._tree, self._dirty, None
        if not isinstance(context, str):
            return None, None, None
        if context.startswith(_MMSI_PREFIX):
            mmsi = context[len(_MMSI_PREFIX) :]
            if not mmsi:
                return None, None, None
            tree = self._ais.setdefault(mmsi, {})
            return tree, self._dirty_ais, mmsi
        if context.startswith(_ATOM_PREFIX):
            atom_id = context[len(_ATOM_PREFIX) :]
            if not atom_id:
                return None, None, None
            tree = self._atoms.setdefault(atom_id, {})
            # Atoms share the AIS dirty set for now -- consumers filter by
            # whether the key exists in _ais vs _atoms.
            return tree, self._dirty_ais, atom_id
        return None, None, None

    def forget_ais(self, mmsi: str) -> None:
        """Remove an AIS target after it expires. Safe to call from the
        publish tick between deltas; the WS reader may race but the worst
        case is one duplicate target-registered event next delta, which
        the publish path idempotents against ``announced``."""
        self._ais.pop(mmsi, None)

    def _set_leaf_in(
        self,
        tree: dict[str, Any],
        path: str,
        value: Any,
        ts: str | None,
        src: str | None,
    ) -> None:
        """Walk to ``path`` and patch the leaf in place.

        Called under ``self._lock``. Creates intermediate dicts as needed so a
        never-before-seen path materialises. If the tree previously carried a
        composite at the same slot (unlikely -- deltas don't collide with the
        polled schema in practice), the existing dict is preserved and only the
        ``value``/``timestamp``/``$source``/``values[src]`` slots are updated.
        """
        parts = path.split(".")
        node = tree
        for p in parts[:-1]:
            child = node.get(p)
            if not isinstance(child, dict):
                child = {}
                node[p] = child
            node = child
        last = parts[-1]
        leaf = node.get(last)
        if not isinstance(leaf, dict):
            leaf = {}
            node[last] = leaf
        leaf["value"] = value
        if ts:
            leaf["timestamp"] = ts
        if src:
            leaf["$source"] = src
            vals = leaf.get("values")
            if not isinstance(vals, dict):
                vals = {}
                leaf["values"] = vals
            vals[src] = {"value": value, "timestamp": ts, "$source": src}
