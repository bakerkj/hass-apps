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
import copy
import json
import logging
import time
from typing import Any

import websockets
import websockets.exceptions

log = logging.getLogger(__name__)

_STREAM_PATH = "/signalk/v1/stream?subscribe=self"


def _ws_url(base_url: str) -> str:
    """HTTP(S) SK base URL → matching WS(S) stream URL."""
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :].rstrip("/") + _STREAM_PATH
    if base_url.startswith("http://"):
        return "ws://" + base_url[len("http://") :].rstrip("/") + _STREAM_PATH
    return base_url.rstrip("/") + _STREAM_PATH


class WSSubscriber:
    """Maintains a live mirror of ``vessels/self`` from SK delta messages.

    ``bootstrap`` seeds the tree from a fresh REST snapshot; deltas only patch
    values in place, so the tree schema (meta, ``values``, ``$source``) stays
    consistent with what the entity resolvers expect. Reads are served from a
    deep copy so callers can iterate without worrying about mid-walk mutation.
    """

    def __init__(self, base_url: str, token: str | None = None) -> None:
        self._url = _ws_url(base_url)
        self._token = token
        self._tree: dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._dirty: set[str] = set()
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
        """Signal the reader to exit and await it."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except TimeoutError, asyncio.CancelledError:
                self._task.cancel()

    async def snapshot(self) -> dict[str, Any]:
        """Return a deep-copied snapshot of the current tree.

        Deep-copied so downstream flatten / staleness walks never race a delta
        mid-traverse. The tree is small (order of a few hundred leaves), so
        copy cost is sub-millisecond in practice.
        """
        async with self._lock:
            return copy.deepcopy(self._tree)

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
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError as exc:
                            log.debug("SK WS: bad JSON: %s", exc)
                            continue
                        await self._apply(msg)
            except (websockets.exceptions.WebSocketException, OSError) as exc:
                log.warning("Signal K WS: %s; reconnecting in %.1fs", exc, backoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Signal K WS: unexpected error; reconnecting")
            # Backoff between reconnects, but wake early on stop so shutdown
            # is prompt regardless of where in the schedule we are.
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except TimeoutError:
                pass
            backoff = min(backoff * 2, 30.0)

    async def _apply(self, msg: dict[str, Any]) -> None:
        """Fold one SK delta message into the tree.

        SK delta format::

            {"context": "vessels.self", "updates": [
              {"$source": "...", "timestamp": "...", "values": [
                {"path": "electrical.batteries.0.voltage", "value": 12.75}
              ]}
            ]}

        Only ``vessels.self`` context is honoured here; the AIS/atoms streams
        are their own subscription.
        """
        context = msg.get("context")
        if context not in (None, "vessels.self"):
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
                        src = (
                            src_meta.get("label")
                            if isinstance(src_meta.get("label"), str)
                            else None
                        )
                    else:
                        src = None
                for v in u.get("values") or []:
                    if not isinstance(v, dict):
                        continue
                    path = v.get("path")
                    if not isinstance(path, str) or not path:
                        continue
                    self._set_leaf(path, v.get("value"), ts, src)
                    self._dirty.add(path)
            self._last_delta_monotonic = time.monotonic()

    def _set_leaf(
        self,
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
        node = self._tree
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
