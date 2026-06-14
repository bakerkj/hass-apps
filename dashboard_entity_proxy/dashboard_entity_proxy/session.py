# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-connection /api/websocket interception.

For each client connection the proxy opens one paired connection to Home
Assistant and relays the auth handshake unchanged (the client authenticates as
itself; no proxy token). After auth the proxy runs a fixed startup sequence
against HA (state mirror, entity/device registry list + update subscriptions,
default dashboard config), and only then begins serving client traffic. The
client's own subscribe_entities is answered from the mirror, scoped to the
current view, with add/remove diffs as the client navigates.

Message-id contract: every id the proxy sends to HA comes from one monotonic
allocator (``self._next_ha_id``), guaranteeing HA's strictly-increasing rule
holds by construction. A single ``self._inflight_table`` (``InflightTable``)
records the purpose of each outbound id; inbound HA messages route off that
table. The proxy enforces the same strictly-increasing rule on the client side
so a buggy client cannot confuse the translation table by reusing ids.

Concurrency model: everything runs on one asyncio event loop, so there are no
locks. Message handlers are synchronous and enqueue output with put_nowait, so
they never yield mid-mutation. Each connection has a writer task draining a
bounded queue; a full queue disconnects the slow peer.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, assert_never

import aiohttp
from aiohttp import WSMsgType, web

from . import wire
from ._msg_utils import subscription_frame
from ._periodic import PeriodicTask
from ._ws_writer import WsWriter
from .dashboard_cache import DashboardCache
from .inflight import InflightTable
from .navigation_manager import NavigationManager
from .options import Options
from .registry_store import RegistryStore
from .scope_resolver import ScopeFilters, ScopeResolver
from .subscription_set import SubscriptionSet
from .phase import Phase
from .scope_watchdog import ScopeWatchdog
from .session_registry import SessionRegistry  # noqa: F401  (re-export for compat)
from .throttle import ThrottleBuffer
from .inflight_types import (
    ClientConfig,
    ClientReq,
    ClientUnsubscribe,
    ConfigFetch,
    DeviceList,
    DeviceUpdates,
    EnergyPrefs,
    EntityGet,
    EntityList,
    EntityUpdates,
    InflightEntry,
    LovelaceUpdates,
    Mirror,
)
from .const import (
    CLEANUP_DRAIN_TIMEOUT,
    HA_SUBSCRIPTION_COMMANDS,
    INFLIGHT_MAX,
    INFLIGHT_SWEEP_INTERVAL,
    OUTBOUND_BUFFER,
    SCOPE_READY_TIMEOUT,
)
from .navigation import (
    View,
    ViewKind,
    is_settings_signal,
    path_from_browser_mod,
    url_path_from_config_request,
)
from .statestore import StateStore


# Exception types that mean "the peer hung up", as opposed to "a bug in a
# handler". The pumps swallow the former silently (a clean disconnect is
# the steady-state exit path for every session) and log the latter at
# WARNING with a traceback so bug reports are actionable.
_EXPECTED_DISCONNECT_EXC: tuple[type[BaseException], ...] = (
    ConnectionResetError,
    aiohttp.ClientConnectionError,
    aiohttp.WSServerHandshakeError,
    aiohttp.ServerDisconnectedError,
)


# ``ScopeSet`` lives in ``dashboard.py`` next to ``ExtractResult`` (its
# only meaningful collaborator). Re-exported here so existing callers
# importing ``from dashboard_entity_proxy.session import ScopeSet`` keep
# working without churn.
from .dashboard import ScopeSet  # noqa: F401, E402


def filter_event(
    event: dict[str, Any], scope: set[str] | None
) -> dict[str, Any] | None:
    """Filter a compressed event to entities in scope (None = all). Returns None
    when nothing in the event is relevant."""
    if scope is None:
        return event
    out: dict[str, Any] = {}
    added = {k: v for k, v in (event.get("a") or {}).items() if k in scope}
    changed = {k: v for k, v in (event.get("c") or {}).items() if k in scope}
    removed = [k for k in (event.get("r") or []) if k in scope]
    if added:
        out["a"] = added
    if changed:
        out["c"] = changed
    if removed:
        out["r"] = removed
    return out or None


# Dispatch-table entry shapes are declared as a sealed dataclass union
# in ``inflight_types.py``. ``Session._dispatch_inflight`` ``match``-es
# on the entry type so mypy verifies exhaustiveness.


class Session:
    """Mediates one client connection and its paired Home Assistant connection.

    Lifecycle phases (see ``phase.py`` for the enum + transitions)::

        CONNECTING ── auth_ok ──▶ STARTUP ── scope-ready ──▶ READY
            │                       │                          │
            └──── _done.set() ──────┴──────────────────────────┴──▶ CLOSING

    Inbound HA-frame dispatch path (``_handle_ha`` → ``_dispatch_inflight``):
        parse → if int id in the inflight table: ``match entry`` on the
        typed ``InflightEntry`` (see ``inflight_types.py``). Each case
        routes to a kind-specific handler:
          * ``ClientReq`` / ``ClientConfig`` / ``ClientUnsubscribe`` →
            rewrite id back to client + ``_forward_to_client``
          * ``Mirror`` → ``_handle_mirror`` (state-cache + propagation)
          * ``EntityUpdates`` / ``DeviceUpdates`` →
            ``RegistryStore.handle_subscription_event`` (debounced
            registry rebuild)
          * ``LovelaceUpdates`` → ``DashboardCache.handle_lovelace_updated``
            (invalidate dashboard cache; refetch if user is on the
            edited dashboard)
          * ``EntityList`` / ``DeviceList`` →
            ``RegistryStore.handle_*_list`` (full registry payload)
          * ``EntityGet`` → ``RegistryStore.handle_entity_get``
            (incremental fetch)
          * ``ConfigFetch`` → ``ScopeResolver.cache_and_reapply``
            (proxy-injected lovelace/config response; gated on
            latest-fetch-id)
          * ``EnergyPrefs`` → ``ScopeResolver.handle_energy_prefs``
            (energy panel entity set)
        Late-ack recall: a result for a swept ``ClientReq`` is still
        forwarded via ``InflightTable.swept_to_client_id``.

    Outbound client-frame dispatch path (``_handle_client``):
        parse → ``_check_client_id`` (id monotonicity; reject reused) →
        per-message-type handlers:
          * ``subscribe_*`` family → ``_handle_client_subscribe`` (cap
            check, scope-ready park, id translation, forward)
          * ``unsubscribe_events`` → ``_try_handle_unsubscribe`` (retag
            inflight, forward; ack triggers ``ClientUnsubscribe`` dispatch)
          * ``lovelace/config`` → retag the inflight ``ClientReq`` to a
            ``ClientConfig`` so the response feeds scope resolution
          * other → id-translate + forward as ``ClientReq``
    """

    def __init__(
        self,
        ws_client: web.WebSocketResponse,
        ws_ha: aiohttp.ClientWebSocketResponse,
        remote: str,
        opts: Options,
        log: logging.Logger,
    ) -> None:
        self.ws_client = ws_client
        self.ws_ha = ws_ha
        self._remote = remote
        self._log = log
        self._registry = opts.registry
        self._dashboard_url_path = opts.dashboard_url_path
        self.throttle = opts.throttle
        self._filters = ScopeFilters(
            extra=opts.extra_entities,
            include=opts.include_globs,
            exclude=opts.exclude_globs,
            customization=opts.customization,
        )

        self._store = StateStore()
        self._connected_at = datetime.now(timezone.utc)
        self._sent_to_client = 0
        # Per-direction byte counters (WS payload length only; frame
        # overhead isn't counted). rx is client→HA; tx is HA-or-mirror→
        # client. Surfaced via status() for the rx/tx columns in the
        # status UI.
        self._bytes_to_client = 0
        self._bytes_to_ha = 0
        self._bytes_from_client = 0
        self._bytes_from_ha = 0

        self._phase: Phase = Phase.CONNECTING
        self._mirror_ready = False
        # Live and pending client ``subscribe_entities`` subscriptions.
        # See ``subscription_set.py``.
        self._subs = SubscriptionSet()

        # Parsed scope per dashboard + the "latest-wins" tracker for
        # proxy-injected lovelace/config fetches. See ``dashboard_cache.py``.
        self._dashboard_cache = DashboardCache()

        # Where the client is now + how the proxy got that signal. See
        # ``navigation_manager.py``.
        # Forward declarations: ``NavigationManager`` and ``RegistryStore``
        # both want a ``resolve_current_view`` callback, but the real one
        # lives on ``self._scope`` which we can't build until ``self._registry_store``
        # exists. Pass a deferred-lookup closure so the call resolves
        # ``self._scope`` at call time.
        def _resolve_current_view() -> None:
            self._scope.resolve_current_view()

        self._nav = NavigationManager(
            initial_view=View(ViewKind.DASHBOARD),
            log=self._log,
            dashboard_cache=self._dashboard_cache,
            inject_config_fetch=self._inject_config_fetch,
            resolve_current_view=_resolve_current_view,
        )

        # Registry tracking: raw row maps, derived index, debouncers,
        # update-event flush pipeline. See ``registry_store.py``.
        self._registry_store = RegistryStore(
            log=self._log,
            send_ha_command=self._send_ha_command,
            resolve_current_view=_resolve_current_view,
            fatal=self._fatal,
            mode=opts.registry_mode,
            burst_threshold=opts.registry_burst_threshold,
        )
        # Periodic safety-net refresh runs at this interval (0 disables).
        # Kept on Session because the PeriodicTask wiring lives here.
        self._registry_refetch_interval = opts.registry_refetch_interval

        # Scope resolution + active scope state. See ``scope_resolver.py``.
        self._scope = ScopeResolver(
            log=self._log,
            store=self._store,
            dashboard_cache=self._dashboard_cache,
            registry_store=self._registry_store,
            filters=self._filters,
            get_current_view=lambda: self._nav.current_view,
            get_subs=lambda: self._subs.live,
            emit_add=self._send_add,
            emit_remove=self._send_remove,
            on_ready=self._on_scope_ready,
            inject_config_fetch=self._inject_config_fetch,
        )

        # Message-id allocator. Every id the proxy ever sends to HA comes
        # from here, monotonically increasing for the life of the session.
        # HA's last_id starts at 0 on a fresh connection; starting at 1
        # leaves no risk of using id 0.
        self._next_ha_id: itertools.count[int] = itertools.count(1)
        # Dispatch table: outbound proxy id -> request kind, plus the five
        # parallel data structures that classify, sweep, and retain
        # inflight entries. See ``inflight.py`` for the encapsulated
        # bookkeeping (insert / retag / pop / sweep / swept-recall /
        # reverse client→ha index / cap counters).
        self._inflight_table = InflightTable()

        # The proxy mirrors HA's strictly-increasing rule on the client side
        # so a buggy client cannot confuse the translation table by reusing
        # ids. last_client_id starts at 0 to match HA's own initial value.
        self._last_client_id: int = 0

        # Per-session counters for client-id reuse rejections. The first
        # rejection logs at INFO with the offending id; further rejections
        # log at DEBUG only (``_id_reuse_logged`` gates the one INFO line).
        # Both surfaces through ``status()``.
        self._id_reuse_rejections: int = 0
        self._id_reuse_logged: bool = False

        self._throttle_buffer = ThrottleBuffer()

        self._to_client: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(OUTBOUND_BUFFER)
        self._to_ha: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(OUTBOUND_BUFFER)
        self._done = asyncio.Event()

        # Peak observed depth of the outbound-to-client queue this
        # session. Surfaced via ``status()`` so the status UI / API can
        # show how close a session is coming to the disconnect cap.
        self._client_queue_high_water = 0
        # Histogram of client-originated command types this session has
        # opened. Lets the status UI attribute reconnect bursts to e.g.
        # ``render_template`` rather than guessing.
        self._opened_command_counts: dict[str, int] = {}

    def _on_scope_ready(self) -> None:
        """Called by ScopeResolver after a scope becomes ready (or after
        the watchdog widens). Bridges the scope subsystem to Session's
        lifecycle: lifts the phase to ``READY`` from either pre-ready
        state (``CONNECTING`` or ``STARTUP``) and serves any client
        subscriptions parked waiting for scope. The ``CONNECTING`` →
        ``READY`` path is reachable only when the watchdog widens before
        HA's ``auth_ok`` lands; vanishingly rare in practice, but the
        previous code left phase stuck on CONNECTING with
        scope.ready=True, an impossible state in status output.
        """
        if self._phase in (Phase.CONNECTING, Phase.STARTUP):
            self._phase = Phase.READY
        self._maybe_serve_pending()

    # --- lifecycle ---------------------------------------------------------

    async def run(self) -> None:
        """Run the session until either side disconnects.

        Spawns the read pumps (one per WebSocket), the writer tasks (one per
        outbound queue), the scope-resolution watchdog, and, when throttle is
        on, the periodic flush loop. Registers the session with the
        ``SessionRegistry`` so the status UI sees it, and de-registers + closes
        both sockets on exit.
        """
        if self._registry is not None:
            self._registry.add(self)
        writer_ha = asyncio.create_task(
            WsWriter(self.ws_ha, self._to_ha, self._done, self._cleanup).run()
        )
        writer_client = asyncio.create_task(
            WsWriter(self.ws_client, self._to_client, self._done, self._cleanup).run()
        )
        writers = [writer_ha, writer_client]
        watchdog = ScopeWatchdog(
            done=self._done,
            timeout=SCOPE_READY_TIMEOUT,
            log=self._log,
            is_scope_ready=lambda: self._scope.ready,
            widen_to_all=self._scope.widen_to_all_from_watchdog,
        )
        tasks = [
            asyncio.create_task(self._pump_ha()),
            asyncio.create_task(self._pump_client()),
            writer_ha,
            writer_client,
            asyncio.create_task(watchdog.run()),
        ]
        if self.throttle > 0:
            throttle = PeriodicTask(
                done=self._done,
                interval=self.throttle,
                tick=self._flush_throttle,
            )
            tasks.append(asyncio.create_task(throttle.run()))
        if self._registry_refetch_interval > 0:
            refetch = PeriodicTask(
                done=self._done,
                interval=self._registry_refetch_interval,
                tick=self._registry_store.tick_periodic_refetch,
            )
            tasks.append(asyncio.create_task(refetch.run()))
        sweeper = PeriodicTask(
            done=self._done,
            interval=INFLIGHT_SWEEP_INTERVAL,
            tick=lambda: self._inflight_table.sweep(time.monotonic()),
        )
        tasks.append(asyncio.create_task(sweeper.run()))
        try:
            await self._done.wait()
        finally:
            if self._registry is not None:
                self._registry.remove(self)
            self._registry_store.cancel_debouncers()
            # Give writers a short window to flush any items already
            # observed from their queues so steady-state final acks land
            # before teardown. Past the deadline, fall through to the
            # hard cancel, bounding teardown latency under a stuck peer.
            for writer in writers:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(writer), timeout=CLEANUP_DRAIN_TIMEOUT
                    )
                except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                    pass
            for task in tasks:
                task.cancel()
            # Bound the overall cleanup so a wedged writer (e.g., stuck
            # in a socket-level send to a stalled peer) can't pin the
            # session indefinitely. Cancelled tasks normally exit
            # promptly; the timeout is the safety net.
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=CLEANUP_DRAIN_TIMEOUT * 4,
                )
            except asyncio.TimeoutError:
                self._log.warning("cleanup deadline expired; some tasks may be leaking")
            await self._close_conns()

    def _cleanup(self) -> None:
        """Signal every task in ``run()`` to exit. Idempotent."""
        self._phase = Phase.CLOSING
        self._done.set()

    def _fatal(self, reason: str) -> None:
        """Log a fatal session error and tear the session down. Used when a
        startup-sequence request or a long-lived subscription fails; the
        session cannot serve correct scope from that point on, so disconnect
        rather than fall back to a silently-permissive default.
        """
        self._log.error("session failure: %s", reason)
        self._cleanup()

    async def _close_conns(self) -> None:
        """Best-effort close of both WebSocket halves. Errors are swallowed
        because the connections may already be torn down by the peer.
        """
        # Close the upstream (HA) side first so any in-flight HA -> client
        # data stops at the source before we tear down the client socket;
        # closing client-first would race with frames already in flight.
        for ws in (self.ws_ha, self.ws_client):
            try:
                await ws.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass

    # --- pumps and writer --------------------------------------------------

    async def _pump_ws(
        self,
        ws: Any,
        *,
        name: str,
        add_bytes_in: Callable[[int], None],
        handle_text: Callable[[dict[str, Any]], None],
        forward_binary: Callable[[bytes], None],
        on_non_json_text: Callable[[str], None],
    ) -> None:
        """Common reader for either side of the proxy. Accounts incoming
        bytes, dispatches each parsed JSON message via ``handle_text``,
        forwards binary frames via ``forward_binary``, hands non-JSON
        text frames to ``on_non_json_text``. Any read failure or close
        ends the session via ``_cleanup``.
        """
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    add_bytes_in(len(msg.data))
                    parsed = wire.parse_messages(msg.data)
                    if parsed is None:
                        on_non_json_text(msg.data)
                        continue
                    for m in parsed:
                        handle_text(m)
                elif msg.type == WSMsgType.BINARY:
                    add_bytes_in(len(msg.data))
                    forward_binary(msg.data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    break
        except _EXPECTED_DISCONNECT_EXC:  # noqa: BLE001 - peer-side close
            pass
        except Exception as exc:  # noqa: BLE001 - bug in a handler, not a disconnect
            self._log.warning("%s pump exception: %r", name, exc, exc_info=True)
        finally:
            self._cleanup()

    async def _pump_ha(self) -> None:
        """HA-side reader. Text frames dispatch via ``_handle_ha``;
        binary frames forward to the client. Non-JSON text from HA is a
        protocol violation; log at debug and drop rather than confuse
        the client.
        """

        def _add_bytes_in(n: int) -> None:
            self._bytes_from_ha += n

        await self._pump_ws(
            self.ws_ha,
            name="ha",
            add_bytes_in=_add_bytes_in,
            handle_text=self._handle_ha,
            forward_binary=self.send_client_bytes,
            on_non_json_text=lambda data: self._log.debug(
                "dropping non-JSON text frame from HA: %r", data[:200]
            ),
        )

    async def _pump_client(self) -> None:
        """Client-side reader. Text frames dispatch via ``_handle_client``;
        binary frames forward to HA. Non-JSON text from the client passes
        through to HA verbatim; clients send the occasional unusual
        frame and HA's WS layer is the right place to reject it.
        """

        def _add_bytes_in(n: int) -> None:
            self._bytes_from_client += n

        await self._pump_ws(
            self.ws_client,
            name="client",
            add_bytes_in=_add_bytes_in,
            handle_text=self._handle_client,
            forward_binary=self.send_ha_bytes,
            on_non_json_text=self.send_ha,
        )

    # --- enqueue -----------------------------------------------------------

    def send_client(self, text: str) -> None:
        """Queue a text frame for the client. Also bumps the status counter."""
        self._sent_to_client += 1
        self._bytes_to_client += len(text)
        self._enqueue(self._to_client, ("text", text))

    def send_client_bytes(self, data: bytes) -> None:
        """Queue a binary frame for the client."""
        self._bytes_to_client += len(data)
        self._enqueue(self._to_client, ("bin", data))

    def send_ha(self, text: str) -> None:
        """Queue a text frame for the HA-side WebSocket."""
        self._bytes_to_ha += len(text)
        self._enqueue(self._to_ha, ("text", text))

    def send_ha_bytes(self, data: bytes) -> None:
        """Queue a binary frame for the HA-side WebSocket."""
        self._bytes_to_ha += len(data)
        self._enqueue(self._to_ha, ("bin", data))

    def _enqueue(
        self, queue: "asyncio.Queue[tuple[str, Any]]", item: tuple[str, Any]
    ) -> None:
        """Non-blocking enqueue onto an outbound queue. If the queue is full
        the slow peer is disconnected; bounded memory beats unbounded
        backlog when a client stops reading.
        """
        if self._done.is_set():
            return
        try:
            queue.put_nowait(item)
            if queue is self._to_client:
                depth = queue.qsize()
                if depth > self._client_queue_high_water:
                    self._client_queue_high_water = depth
            return
        except asyncio.QueueFull:
            if queue is self._to_client:
                self._log.warning(
                    "outbound to-client queue full (depth=%d/%d, "
                    "high_water=%d); disconnecting slow peer",
                    queue.qsize(),
                    OUTBOUND_BUFFER,
                    self._client_queue_high_water,
                )
            else:
                self._log.warning(
                    "outbound to-HA queue full (depth=%d/%d); disconnecting",
                    queue.qsize(),
                    OUTBOUND_BUFFER,
                )
            self._cleanup()

    # --- id allocation -----------------------------------------------------

    def _send_ha_command(
        self, kind: InflightEntry, msg_type: str, **fields: Any
    ) -> int:
        """The single chokepoint for proxy-originated HA requests. Allocates
        a fresh monotonic id from ``_next_ha_id``, records the request kind
        in the dispatch table, and enqueues the command. Returns the
        allocated id so the caller can record it (e.g. for latest-wins
        refetch tracking).

        Long-lived subscriptions (``Mirror()``, ``EntityUpdates()``,
        ``DeviceUpdates()``) keep their entry in ``_inflight`` for the
        life of the session (they don't pop on first response), so the
        inbound dispatcher routes every later event to the same handler.
        """
        mid = next(self._next_ha_id)
        self._inflight_table.insert(mid, kind)
        self.send_ha(wire.command(mid, msg_type, **fields))
        return mid

    # --- HA -> client ------------------------------------------------------

    def _handle_ha(self, msg: dict[str, Any]) -> None:
        """Dispatch one HA-side message. Looks up the id in the inflight
        dispatch table and routes to the right handler; falls back to
        forwarding ``auth_*`` and other unsolicited messages to the client.
        """
        mid = msg.get("id")
        if isinstance(mid, int):
            entry = self._inflight_table.get(mid)
            if entry is not None:
                self._dispatch_inflight(mid, entry, msg)
                return
            # Integer id with no inflight entry. If we recently swept a
            # ``ClientReq(N)`` entry with this id, forward the late
            # ack: the client is still waiting on its original id.
            # Only consume the recall slot for ``result`` frames; a
            # stray non-result frame should not silently discard the
            # recall and force a later real ack to be dropped.
            if (
                msg.get("type") == "result"
                and mid in self._inflight_table.swept_to_client_id
            ):
                client_id = self._inflight_table.recall_swept(mid)
                if client_id is not None:
                    self._forward_to_client(client_id, msg)
                    return
            # Otherwise: late event after explicit unsubscribe, or a HA
            # id we never allocated. Forwarding un-rewritten would leak
            # the proxy's allocator id to the client as "event for
            # unknown subscription". Drop instead.
            self._log.debug(
                "dropping HA message for unknown inflight id %s (type=%r)",
                mid,
                msg.get("type"),
            )
            return

        # Unsolicited or untracked message; forward to the client. This is
        # the ``auth_required`` / ``auth_ok`` / ``auth_invalid`` handshake
        # before any proxy id has been allocated.
        self.send_client(wire.dumps(msg))
        if msg.get("type") == "auth_ok":
            self._on_auth_ok()

    def _dispatch_inflight(
        self, mid: int, entry: InflightEntry, msg: dict[str, Any]
    ) -> None:
        """Route an inbound HA message based on its dispatch-table entry.

        Long-lived subscriptions (``Mirror``, ``EntityUpdates``,
        ``DeviceUpdates``, ``LovelaceUpdates``) keep their entry, since events
        keep arriving. One-shot kinds (``EntityList``, ``DeviceList``,
        ``EntityGet``, ``EnergyPrefs``, ``ConfigFetch``, ``ClientConfig``,
        ``ClientUnsubscribe``) pop on result. ``ClientReq`` is special:
        the proxy can't tell at request time whether the command is a
        streaming subscription, so the entry persists until either a
        late event promotes it to "known subscription" or the
        grace-period sweep reclaims it.

        ``match`` on the sealed ``InflightEntry`` union, with the trailing
        ``case _ as unhandled: assert_never(unhandled)`` makes mypy
        verify exhaustiveness, so a future ``InflightEntry`` kind
        without a case here is a type error rather than a silent drop.
        """
        match entry:
            case Mirror():
                # Long-lived: state-mirror events keep arriving.
                self._handle_mirror(msg)
            case EntityUpdates():
                # Long-lived: registry-update events keep arriving.
                self._registry_store.handle_subscription_event(mid, msg, kind="entity")
            case DeviceUpdates():
                self._registry_store.handle_subscription_event(mid, msg, kind="device")
            case LovelaceUpdates():
                self._dashboard_cache.handle_lovelace_updated(
                    msg,
                    log=self._log,
                    current_view=self._nav.current_view,
                    inject_config_fetch=self._inject_config_fetch,
                )
            case EntityList():
                self._inflight_table.pop(mid)
                self._registry_store.handle_entity_list(mid, msg)
            case DeviceList():
                self._inflight_table.pop(mid)
                self._registry_store.handle_device_list(mid, msg)
            case EntityGet(entity_id=eid):
                self._inflight_table.pop(mid)
                self._registry_store.handle_entity_get(eid, msg)
            case EnergyPrefs():
                self._inflight_table.pop(mid)
                self._scope.handle_energy_prefs(msg)
            case ConfigFetch(url_path=url):
                self._inflight_table.pop(mid)
                self._scope.cache_and_reapply(
                    url, mid, self._scope.scope_from_config(msg)
                )
            case ClientReq(client_id=cid):
                self._inflight_table.classify_inbound(
                    mid, msg.get("type"), now=time.monotonic()
                )
                self._forward_to_client(cid, msg)
            case ClientConfig(client_id=cid, url=url):
                self._forward_to_client(cid, msg)
                self._inflight_table.pop(mid)
                self._scope.cache_and_reapply(
                    url, mid, self._scope.scope_from_config(msg)
                )
            case ClientUnsubscribe(client_id=cid, ha_sub_id=sub_id):
                self._forward_to_client(cid, msg)
                self._inflight_table.pop(mid)
                if msg.get("type") == "result" and msg.get("success") is False:
                    err = msg.get("error") or {}
                    self._log.warning(
                        "client unsubscribe failed on HA side (sub=%s code=%r): %s",
                        sub_id,
                        err.get("code"),
                        err.get("message"),
                    )
                # The unsubscribe is gone from the client's perspective
                # either way (success or HA-side error); retire the
                # subscription's bookkeeping unconditionally.
                self._inflight_table.pop(sub_id)
                self._inflight_table.discard_known(sub_id)
                self._inflight_table.cancel_pop(sub_id)
            case _ as unhandled:
                assert_never(unhandled)

    def _forward_to_client(self, client_id: int, msg: dict[str, Any]) -> None:
        """Rewrite the id back to the client's original id and forward.

        Inflight-entry retention is decided by the caller in
        :meth:`_dispatch_inflight`. One-shot client commands pop their
        entry there on result; client subscriptions keep theirs for the
        life of the subscription.
        """
        msg["id"] = client_id
        self.send_client(wire.dumps(msg))

    def _handle_mirror(self, msg: dict[str, Any]) -> None:
        """Process one event from the proxy's mirror subscription. The first
        event seeds the state store (and triggers a scope re-resolution so
        pattern-based scopes can now expand against real entity ids). Later
        events fold newly-arriving entities into the live scope's pattern
        rules, then propagate (immediately or via the throttle window) to
        every active client subscription.

        A failed subscribe (``success: false``) is fatal: without the
        mirror the session cannot serve correct state.
        """
        event = subscription_frame(
            msg,
            on_failure=lambda: self._fatal("mirror subscribe_entities failed"),
        )
        if event is None:
            return
        self._store.apply(event)
        if not self._mirror_ready:
            self._mirror_ready = True
            # The mirror just gained its initial snapshot; re-resolve the
            # current view so any pattern-based scopes expand against the
            # entity ids that now exist.
            if self._scope.ready:
                self._scope.resolve_current_view()
            self._maybe_serve_pending()
            return
        # An ongoing update: fold any newly-arriving entities that match
        # the current scope's include patterns into the live scope, so
        # auto-entities / flex-table-card cards see new state as it appears.
        self._scope.fold_in_pattern_matches(event)
        if self.throttle > 0:
            self._throttle_buffer.record(event)
            return
        self._propagate(event)

    def _propagate(self, event: dict[str, Any]) -> None:
        """Filter one compressed event to the active scope and emit it once
        per active subscription (id-rewritten to match each subscription).
        Drops the event entirely if nothing in scope changed.
        """
        filtered = filter_event(event, self._scope.set)
        if filtered is None:
            return
        for sub in self._subs.live:
            self.send_client(wire.event_message(sub, filtered))

    # --- client -> HA ------------------------------------------------------

    def _handle_client(self, msg: dict[str, Any]) -> None:
        """Dispatch one client-side message. Enforces HA's strictly-increasing
        id rule on this face of the proxy first, then intercepts the message
        types the proxy needs to react to before forwarding.

        ``subscribe_entities`` and ``unsubscribe_events`` for tracked subs
        are intercepted (we synthesize their responses from the mirror).
        ``browser_mod/update`` and ``lovelace/config`` drive
        navigation/scope updates. Other settings-page signals trigger the
        no-browser_mod widening fallback. Anything not intercepted is
        id-translated and forwarded to HA.
        """
        if not self._check_client_id(msg):
            return
        mtype = msg.get("type") or ""
        if mtype == "subscribe_entities":
            self._handle_client_subscribe(msg)
            return
        if mtype == "unsubscribe_events" and self._try_handle_unsubscribe(msg):
            return
        # Check the inflight cap before any navigation/widening side
        # effects so a rejected message can't leak a navigation that HA
        # never received. ``unsubscribe_events`` is allowed past the cap
        # because its ack retires both itself and the referenced
        # subscription, so the net effect is to shrink the table.
        cid = msg.get("id")
        if (
            isinstance(cid, int)
            and mtype != "unsubscribe_events"
            and len(self._inflight_table) >= INFLIGHT_MAX
        ):
            self._reject_over_cap(cid)
            return
        client_config_url: str | None = None
        if mtype == "browser_mod/update":
            path = path_from_browser_mod(msg)
            if path:
                self._nav.navigate_from_browser_mod(path)
        elif mtype == "lovelace/config":
            # The client wants a dashboard config; remember the url so the
            # response can also feed scope resolution. The proxy doesn't
            # inject its own duplicate fetch (allow_inject=False), but it
            # does intercept the client's response (see the
            # ``client_config`` dispatch path below.
            client_config_url = url_path_from_config_request(msg)
            # A client lovelace/config request always re-navigates to the
            # dashboard view, regardless of whether the current view was
            # set by browser_mod or by the heuristic widen: switching
            # ALL → DASHBOARD here is the reset signal that keeps a
            # cached client from staying wide forever.
            self._nav.navigate(
                View(ViewKind.DASHBOARD, client_config_url),
                allow_inject=False,
                path=f"/{client_config_url}" if client_config_url else "",
            )
        elif is_settings_signal(mtype):
            self._nav.heuristic_widen_to_all()
        if not self._translate_client_id_for_ha(msg):
            # Unknown unsubscribe target: the client referenced a
            # subscription id the proxy never allocated. The "not_found"
            # rejection has already been sent back to the client; do not
            # forward to HA. (The inflight-cap rejection path returns
            # earlier in this function, never reaching this call.)
            return
        # Tag the inflight entry with the url so the client lovelace/config
        # response feeds scope resolution as well as being forwarded to
        # the client. Without this, a dashboard whose default-fetch widens
        # to all (e.g. parser-empty) stays widened forever even though the
        # client's later fetch returns a parseable config.
        if client_config_url is not None:
            ha_id = msg.get("id")
            if isinstance(ha_id, int):
                existing = self._inflight_table.get(ha_id)
                if isinstance(existing, ClientReq):
                    self._inflight_table.retag(
                        ha_id,
                        ClientConfig(existing.client_id, client_config_url),
                    )
                    # Record the proxy-allocated id so the latest-fetch
                    # guard in ``ScopeResolver.cache_and_reapply``
                    # protects against two overlapping client config
                    # requests where the older response arrives last.
                    self._dashboard_cache.record_fetch_id(client_config_url, ha_id)
        self.send_ha(wire.dumps(msg))

    def _check_client_id(self, msg: dict[str, Any]) -> bool:
        """Mirror HA's strictly-increasing id rule on the client face: a
        client message whose id is less than or equal to a previously-seen
        one is rejected with ``ERR_ID_REUSE`` and not forwarded. Returns
        True if the message should proceed, False if it was rejected.

        Messages without a numeric id (the ``auth`` handshake, ``ping``,
        etc.) bypass the check; they have no id-reuse semantics.
        """
        cid = msg.get("id")
        if not isinstance(cid, int):
            return True
        if cid <= self._last_client_id:
            self._id_reuse_rejections += 1
            if not self._id_reuse_logged:
                self._id_reuse_logged = True
                self._log.info(
                    "rejecting client id reuse (id=%s, last_seen=%s); "
                    "further rejections logged at DEBUG",
                    cid,
                    self._last_client_id,
                )
            else:
                self._log.debug(
                    "rejecting client id reuse (id=%s, last_seen=%s)",
                    cid,
                    self._last_client_id,
                )
            self.send_client(
                wire.dumps(
                    {
                        "id": cid,
                        "type": "result",
                        "success": False,
                        "error": {
                            "code": "id_reuse",
                            "message": "Identifier values have to increase.",
                        },
                    }
                )
            )
            return False
        self._last_client_id = cid
        return True

    def _handle_client_subscribe(self, msg: dict[str, Any]) -> None:
        """Intercept a client ``subscribe_entities`` request. Acks it
        immediately, then either serves a snapshot from the mirror right
        away (if mirror + scope are both ready) or parks the subscription
        as "pending" until both are ready.

        Any client-supplied ``entity_ids`` / ``user_ids`` filter on the
        request is deliberately ignored: scope is decided by the proxy
        from the dashboard config and customization, not by the client.
        Treating the client filter as authoritative would let the
        frontend escape the proxy's scope by passing its own
        ``entity_ids`` list.
        """
        cmd_id = msg.get("id")
        if not isinstance(cmd_id, int):
            return
        self._ack(cmd_id)
        if self._mirror_ready and self._scope.ready:
            self._serve_snapshot(cmd_id)
            self._subs.add_live(cmd_id)
        else:
            self._subs.add_pending(cmd_id)

    def _try_handle_unsubscribe(self, msg: dict[str, Any]) -> bool:
        """If the unsubscribe target is one of OUR intercepted subscriptions,
        remove it and ack the client. Returns True when handled locally
        (caller skips forwarding the message to HA), False to forward as-is.
        """
        sub = msg.get("subscription")
        cmd_id = msg.get("id")
        if not isinstance(sub, int) or not self._subs.remove(sub):
            return False
        if isinstance(cmd_id, int):
            self._ack(cmd_id)
        return True

    # --- id translation ----------------------------------------------------

    def _translate_client_id_for_ha(self, msg: dict[str, Any]) -> bool:
        """Translate a forwarded client message's id from its small client-
        side value into a fresh monotonic allocator id so HA's
        strictly-increasing rule holds. Mutates ``msg`` in place. Also
        translates the ``subscription`` field on unsubscribe_events, which
        carries the original subscribe command's id.

        Returns True when the caller should forward ``msg`` to HA, False
        when the per-session inflight cap is hit; in which case a
        protocol-error result has already been sent back to the client and
        ``msg`` MUST NOT be forwarded (the proxy id was never allocated, so
        forwarding would either reuse a stale id or skip the dispatch
        table).
        """
        cid = msg.get("id")
        mtype = msg.get("type")
        if isinstance(cid, int):
            new = next(self._next_ha_id)
            self._inflight_table.insert(new, ClientReq(cid))
            # Known-subscription whitelist match: bypass the
            # grace-period sweep entirely, never pop.
            if isinstance(mtype, str) and mtype in HA_SUBSCRIPTION_COMMANDS:
                self._inflight_table.mark_known(new)
            # Histogram of opened command types so the status UI can
            # attribute reconnect bursts (e.g. a dashboard with hundreds
            # of templated card properties opens hundreds of
            # ``render_template`` subscriptions on first connect).
            if isinstance(mtype, str):
                self._opened_command_counts[mtype] = (
                    self._opened_command_counts.get(mtype, 0) + 1
                )
            msg["id"] = new
        if mtype == "unsubscribe_events":
            sub = msg.get("subscription")
            if isinstance(sub, int):
                ha_sub = self._inflight_table.find_ha_id_for_client(sub)
                if ha_sub is None:
                    # Client references a subscription the proxy never
                    # allocated. Forwarding as-is would send the client's
                    # untranslated id into HA's id space, where it can
                    # collide with one of our proxy-owned subscriptions
                    # (e.g., the mirror at id 1) and silently retire the
                    # wrong stream. Reject locally instead.
                    self.send_client(
                        wire.dumps(
                            {
                                "id": cid,
                                "type": "result",
                                "success": False,
                                "error": {
                                    "code": "not_found",
                                    "message": "unknown subscription",
                                },
                            }
                        )
                    )
                    new_id = msg.get("id")
                    if isinstance(new_id, int):
                        self._inflight_table.pop(new_id)
                    return False
                msg["subscription"] = ha_sub
                # Defer cleanup of the subscription's inflight entry
                # until HA acks the unsubscribe. Popping eagerly would
                # let straggler events (or a ``success: false`` ack)
                # land with no dispatch target. Retag the just-inserted
                # client entry so the dispatcher retires the right
                # subscription when the result arrives.
                new_id = msg.get("id")
                if isinstance(new_id, int):
                    self._inflight_table.retag_as_unsubscribe(new_id, ha_sub)
        return True

    def _reject_over_cap(self, cid: int) -> None:
        """Respond to a client command that hit the per-session inflight
        cap with a protocol-violation result frame, count the hit, and log
        the condition once per session.

        See :data:`INFLIGHT_MAX` for the rationale; a session that lands
        here is almost always a misbehaving (or malicious) client issuing
        large numbers of unique-id one-shots faster than the grace-period
        sweep can reclaim them.
        """
        if self._inflight_table.note_cap_hit():
            self._log.warning(
                "inflight cap reached (max=%d); rejecting further client commands "
                "until existing entries pop",
                INFLIGHT_MAX,
            )
        self.send_client(
            wire.dumps(
                {
                    "id": cid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "inflight_cap",
                        "message": (
                            "Per-session inflight command cap reached; "
                            "wait for outstanding commands to settle."
                        ),
                    },
                }
            )
        )

    # --- navigation & scope resolution ------------------------------------

    def _on_auth_ok(self) -> None:
        """Eager startup sequence after HA's ``auth_ok``: in fixed order,
        before forwarding any client traffic to HA: mirror subscribe,
        entity registry list, entity registry update subscription, device
        registry list, device registry update subscription, lovelace
        update subscription, energy prefs, default dashboard config. All
        eight requests get consecutive ids from the allocator and
        (assuming HA accepts them) HA's last_id steps up monotonically.
        Any later proxy or client traffic uses larger ids.
        """
        if self._phase != Phase.CONNECTING:
            return
        self._phase = Phase.STARTUP
        self._send_ha_command(Mirror(), "subscribe_entities")
        self._registry_store.refetch_entity_registry()
        self._send_ha_command(
            EntityUpdates(),
            "subscribe_events",
            event_type="entity_registry_updated",
        )
        self._registry_store.refetch_device_registry()
        self._send_ha_command(
            DeviceUpdates(),
            "subscribe_events",
            event_type="device_registry_updated",
        )
        # ``lovelace_updated`` fires whenever a user saves a dashboard
        # in the UI editor (or writes a storage-mode dashboard via
        # ``lovelace/config/save``). Subscribing lets us drop the
        # matching ``DashboardCache`` entry so the next navigation back
        # to that dashboard re-fetches its config.
        self._send_ha_command(
            LovelaceUpdates(),
            "subscribe_events",
            event_type="lovelace_updated",
        )
        # Energy dashboard prefs: the /energy panel renders only the
        # entities the user listed in their energy config; we fetch that
        # list once at session start so ``ViewKind.ENERGY`` can scope to
        # those entities rather than widening to all. A failed fetch is
        # not fatal; ``ScopeResolver._energy_scope`` falls back to ALL.
        self._send_ha_command(EnergyPrefs(), "energy/get_prefs")
        self._nav.current_view = View(ViewKind.DASHBOARD, self._dashboard_url_path)
        self._nav.current_path = (
            f"/{self._dashboard_url_path}" if self._dashboard_url_path else ""
        )
        self._inject_config_fetch(self._dashboard_url_path)

    # --- serving -----------------------------------------------------------

    def _maybe_serve_pending(self) -> None:
        """Serve any client subscriptions that were parked waiting for the
        mirror and scope to both be ready. No-op until both are.
        """
        if not (self._mirror_ready and self._scope.ready):
            return
        for cmd_id in self._subs.promote_pending():
            self._serve_snapshot(cmd_id)

    def _serve_snapshot(self, cmd_id: int) -> None:
        """Emit the in-scope contents of the mirror as a single "added"
        event for one client subscription; the initial dump shape clients
        expect immediately after a ``subscribe_entities`` ack.
        """
        snap = self._store.snapshot(self._scope.ids)
        self.send_client(wire.event_message(cmd_id, {"a": snap}))

    def _send_add(self, ids: list[str]) -> None:
        """Emit an "added" event covering ``ids`` to every active client
        subscription. Used for scope-grows and pattern-match folds.
        """
        snap = self._store.snapshot(ids)
        if not snap:
            return
        for sub in self._subs.live:
            self.send_client(wire.event_message(sub, {"a": snap}))

    def _send_remove(self, ids: list[str]) -> None:
        """Emit a "removed" event covering ``ids`` to every active client
        subscription. Used for scope-shrinks and explicit removals.
        """
        if not ids:
            return
        for sub in self._subs.live:
            self.send_client(wire.event_message(sub, {"r": ids}))

    def _ack(self, cmd_id: int) -> None:
        """Send a successful result acknowledgement for a client command."""
        self.send_client(wire.result_ok(cmd_id))

    # --- throttling --------------------------------------------------------

    def _in_scope(self, eid: str) -> bool:
        """Whether ``eid`` would currently be forwarded to the client (i.e.
        is in the active scope, or scope is "all entities").
        """
        return self._scope.set is None or eid in self._scope.set

    def _flush_throttle(self) -> None:
        """Emit accumulated throttled diffs to every active subscription.
        Called from the throttle-loop task at the configured interval.
        """
        add, rem = self._throttle_buffer.drain(self._in_scope)
        if rem:
            self._send_remove(rem)
        if add:
            self._send_add(add)

    # --- config fetch ------------------------------------------------------

    def _inject_config_fetch(self, url_path: str) -> None:
        """Send a ``lovelace/config`` request to HA with a proxy-allocated id
        so the response is routed back into our scope-resolution path instead
        of being forwarded to the client. Used when the proxy needs to fetch
        a dashboard config the client hasn't asked for itself.
        """
        mid = self._send_ha_command(
            ConfigFetch(url_path),
            "lovelace/config",
            url_path=(url_path or None),
            force=False,
        )
        # Track the latest fetch id per dashboard so a slower response from
        # a previous fetch can't overwrite the cache with stale data.
        self._dashboard_cache.record_fetch_id(url_path, mid)

    # --- status ------------------------------------------------------------

    def status(self, *, detail: str = "summary") -> dict[str, Any]:
        """Snapshot dict consumed by the Ingress status UI (and by
        ``SessionRegistry`` when this session disconnects). Includes
        connection metadata, the current view, current scope, queue depth,
        and throttle config.

        Default shape (``detail="summary"``) carries ``scope_count`` and a
        capped ``scope_sample`` (up to 50 ids) instead of the full
        ``scope_entities`` list. On installs with thousands of entities and
        multiple sessions polling every 2s the full list dominates the JSON
        payload. Pass ``detail="full"`` for the legacy shape with the
        complete ``scope_entities`` list.
        """
        scope_all = self._scope.ready and self._scope.set is None
        ids = self._scope.ids if self._scope.ids is not None else []
        out: dict[str, Any] = {
            "kind": "intercept",
            "remote_addr": self._remote,
            "connected_at": self._connected_at.isoformat(),
            "phase": self._phase.value,
            "current_view": self._nav.current_view.label(),
            "current_path": self._nav.current_path,
            "scope_ready": self._scope.ready,
            "scope_all": scope_all,
            "scope_count": len(ids),
            "mirror_entities": len(self._store),
            "queue_depth": self._to_client.qsize(),
            "queue_high_water": self._client_queue_high_water,
            "opened_command_counts": dict(self._opened_command_counts),
            "messages_sent": self._sent_to_client,
            # rx is client→proxy bytes (read by _pump_client); tx is
            # proxy→client bytes (queued by send_client/_bytes). These
            # count WS payload sizes only; frame overhead isn't tracked.
            "rx_bytes": self._bytes_from_client,
            "tx_bytes": self._bytes_to_client,
            "ha_rx_bytes": self._bytes_from_ha,
            "ha_tx_bytes": self._bytes_to_ha,
            "throttle_seconds": self.throttle,
            "id_reuse_rejections": self._id_reuse_rejections,
            **self._inflight_table.status(),
        }
        if not scope_all:
            out["scope_sample"] = list(ids[:50])
        if detail == "full":
            out["scope_entities"] = list(ids)
        return out
