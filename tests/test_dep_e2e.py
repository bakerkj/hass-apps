# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end aiohttp interception tests.

A fake Home Assistant that enforces the same strictly-increasing message-id
rule real HA enforces, plus the real proxy app and a real client. The fake
covers the WebSocket commands the proxy issues during normal operation:
auth, subscribe_entities (state mirror), entity/device registry list +
subscribe_events, and lovelace/config. Tests assert auth relay, scoped
snapshots, coalesced array-frame splitting, scope filtering on propagation,
device-page re-scoping, live registry-update tracking via subscribe_events,
client-side id_reuse rejection, and explicit disconnect on startup failure.

The fake HA enforces strict-increasing ids (cur_id > last_id) and emits
ERR_ID_REUSE when violated — the single change that makes the addon's
original lazy-registry-fetch bug surface in tests instead of silently
serving wrong scope in production.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from dashboard_entity_proxy import proxy
from dashboard_entity_proxy.customization import Customization
from dashboard_entity_proxy.session import SessionRegistry


from _aiohttp_helpers import _start_app


# Default registry payloads. Tests can override via FakeHA.entities / .devices.
_DEFAULT_ENTITIES: list[dict[str, Any]] = [
    {
        "entity_id": "light.kitchen",
        "device_id": "dev1",
        "platform": "hue",
        "area_id": "kitchen",
    },
    {
        "entity_id": "sensor.temp",
        "device_id": "dev2",
        "platform": "mqtt",
    },
]
_DEFAULT_DEVICES: list[dict[str, Any]] = [
    {"id": "dev1", "area_id": "kitchen"},
    {"id": "dev2", "area_id": "garage"},
]

# Default mirror snapshot the fake emits on the first subscribe_entities.
_DEFAULT_MIRROR_ADD: dict[str, dict[str, Any]] = {
    "light.kitchen": {
        "s": "on",
        "a": {"brightness": 255},
        "c": "c1",
        "lc": 100.0,
        "lu": 100.0,
    },
    "sensor.temp": {"s": "21.5", "a": {}, "c": "c2", "lc": 101.0},
    "calendar.workday": {"s": "on", "a": {}, "c": "c3", "lc": 102.0},
}


@dataclass
class FakeHA:
    """Fake HA configurable per test.

    Enforces real HA's strictly-increasing id rule and tracks every id the
    proxy sends so tests can assert ordering. Supports failing the next
    response of a chosen kind (``fail_kinds``) for startup-failure tests,
    and emitting registry update events on demand (``trigger_*_updated``)
    for live-tracking tests.
    """

    entities: list[dict[str, Any]] = field(
        default_factory=lambda: list(_DEFAULT_ENTITIES)
    )
    devices: list[dict[str, Any]] = field(
        default_factory=lambda: list(_DEFAULT_DEVICES)
    )
    mirror_add: dict[str, dict[str, Any]] = field(
        default_factory=lambda: dict(_DEFAULT_MIRROR_ADD)
    )
    # Track every id the proxy sends on this connection, in arrival order.
    seen_ids: list[int] = field(default_factory=list)
    # Map of registry subscription kind ("entity"|"device") -> subscription id
    # that the proxy registered for receiving that registry's update events.
    update_subs: dict[str, int] = field(default_factory=dict)
    # Set to True after the auth handshake completes so tests can wait for it.
    auth_complete: asyncio.Event = field(default_factory=asyncio.Event)
    # Active websocket; tests use this to trigger update events server-side.
    ws: web.WebSocketResponse | None = None
    # If non-empty, the next response of any kind in this set returns
    # success=False. Each fail consumes one entry.
    fail_kinds: set[str] = field(default_factory=set)
    # Hooks called when the fake receives a message of each type, post-
    # validation but before responding. Used by tests for synchronization.
    on_lovelace_config: asyncio.Event = field(default_factory=asyncio.Event)
    last_id: int = 0
    # Per-command call counts the tests assert against.
    entity_list_count: int = 0
    device_list_count: int = 0
    entity_get_count: int = 0


async def _send_id_reuse(ws: web.WebSocketResponse, cur_id: int) -> None:
    """Emit HA's ERR_ID_REUSE error frame for a non-increasing id."""
    await ws.send_json(
        {
            "id": cur_id,
            "type": "result",
            "success": False,
            "error": {
                "code": "id_reuse",
                "message": "Identifier values have to increase.",
            },
        }
    )


def _maybe_fail(fake: FakeHA, kind: str) -> bool:
    """Pop ``kind`` from ``fake.fail_kinds`` if present. Returns True when the
    response for this kind should be a failure (``success: false``).
    """
    if kind in fake.fail_kinds:
        fake.fail_kinds.discard(kind)
        return True
    return False


def _make_fake_handler(fake: FakeHA):
    """Build an aiohttp handler bound to the given FakeHA state.

    Layout: send auth_required, wait for auth, send auth_ok, then loop on
    commands. Each command's id is validated against ``last_id`` before
    being processed; non-increasing ids produce ERR_ID_REUSE and are not
    dispatched.
    """

    async def handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        fake.ws = ws
        await ws.send_json({"type": "auth_required"})
        try:
            msg = await ws.receive()
            if msg.type != aiohttp.WSMsgType.TEXT:
                return ws
            await ws.send_json({"type": "auth_ok"})
            fake.auth_complete.set()
            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                m: dict[str, Any] = json.loads(msg.data)
                mid = m.get("id")
                mtype = m.get("type")
                if isinstance(mid, int):
                    if mid <= fake.last_id:
                        await _send_id_reuse(ws, mid)
                        continue
                    fake.last_id = mid
                    fake.seen_ids.append(mid)
                await _dispatch(fake, ws, m, mid, mtype)
        except Exception:
            pass
        return ws

    return handler


async def _dispatch(
    fake: FakeHA,
    ws: web.WebSocketResponse,
    m: dict[str, Any],
    mid: int | None,
    mtype: str | None,
) -> None:
    """Per-message dispatch for the fake HA."""
    if mtype == "subscribe_entities":
        if _maybe_fail(fake, "subscribe_entities"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "fake",
                        "message": "subscribe_entities forced fail",
                    },
                }
            )
            return
        await ws.send_json(
            {"id": mid, "type": "result", "success": True, "result": None}
        )
        await ws.send_json(
            {"id": mid, "type": "event", "event": {"a": dict(fake.mirror_add)}}
        )

        async def _send_changes() -> None:
            await asyncio.sleep(0.05)
            if ws.closed:
                return
            coalesced = [
                {
                    "id": mid,
                    "type": "event",
                    "event": {
                        "c": {
                            "sensor.temp": {"+": {"s": "22", "lc": 200.0, "lu": 200.0}}
                        }
                    },
                },
                {
                    "id": mid,
                    "type": "event",
                    "event": {
                        "c": {
                            "light.kitchen": {
                                "+": {
                                    "s": "off",
                                    "lc": 210.0,
                                    "lu": 210.0,
                                    "a": {"brightness": 0},
                                }
                            }
                        }
                    },
                },
            ]
            await ws.send_str(json.dumps(coalesced))

        asyncio.create_task(_send_changes())
        return

    if mtype == "subscribe_events":
        # The proxy uses subscribe_events to follow registry mutations. The
        # event_type tells us which registry; we record the subscription id
        # so tests can fire synthetic update events on it.
        event_type = m.get("event_type")
        if _maybe_fail(fake, f"subscribe_events:{event_type}"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "fake",
                        "message": "subscribe_events forced fail",
                    },
                }
            )
            return
        if event_type == "entity_registry_updated" and isinstance(mid, int):
            fake.update_subs["entity"] = mid
        elif event_type == "device_registry_updated" and isinstance(mid, int):
            fake.update_subs["device"] = mid
        await ws.send_json(
            {"id": mid, "type": "result", "success": True, "result": None}
        )
        return

    if mtype == "lovelace/config":
        if _maybe_fail(fake, "lovelace/config"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {"code": "fake", "message": "lovelace/config forced fail"},
                }
            )
            fake.on_lovelace_config.set()
            return
        url = m.get("url_path") or ""
        if url == "":
            cfg: dict[str, Any] = {
                "views": [
                    {
                        "cards": [
                            {"entity": "light.kitchen"},
                            # Invisible-entity card; declares no entity refs
                            # in YAML. Only contributes to scope when a
                            # customization registers an implicit entity
                            # for its type.
                            {"type": "custom:my-implicit-card"},
                        ]
                    }
                ]
            }
        elif url == "my-dashboard":
            cfg = {"views": [{"cards": [{"entity": "sensor.temp"}]}]}
        else:
            cfg = {}
        await ws.send_json(
            {"id": mid, "type": "result", "success": True, "result": cfg}
        )
        fake.on_lovelace_config.set()
        return

    if mtype == "config/entity_registry/get":
        entity_id = m.get("entity_id")
        if _maybe_fail(fake, "config/entity_registry/get"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "fake",
                        "message": "entity_registry/get forced fail",
                    },
                }
            )
            return
        row = next((e for e in fake.entities if e.get("entity_id") == entity_id), None)
        if row is None:
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "not_found",
                        "message": f"entity {entity_id!r} not found",
                    },
                }
            )
            return
        # Count for assertions.
        fake.entity_get_count += 1
        await ws.send_json(
            {"id": mid, "type": "result", "success": True, "result": row}
        )
        return

    if mtype == "config/entity_registry/list":
        fake.entity_list_count += 1
        if _maybe_fail(fake, "config/entity_registry/list"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "fake",
                        "message": "entity_registry/list forced fail",
                    },
                }
            )
            return
        await ws.send_json(
            {
                "id": mid,
                "type": "result",
                "success": True,
                "result": list(fake.entities),
            }
        )
        return

    if mtype == "config/device_registry/list":
        fake.device_list_count += 1
        if _maybe_fail(fake, "config/device_registry/list"):
            await ws.send_json(
                {
                    "id": mid,
                    "type": "result",
                    "success": False,
                    "error": {
                        "code": "fake",
                        "message": "device_registry/list forced fail",
                    },
                }
            )
            return
        await ws.send_json(
            {
                "id": mid,
                "type": "result",
                "success": True,
                "result": list(fake.devices),
            }
        )
        return

    # Unhandled command types are accepted silently — many proxy-forwarded
    # client commands (browser_mod/update, etc.) need no response from HA.


async def emit_entity_registry_event(fake: FakeHA, action: str, entity_id: str) -> None:
    """Fire a synthetic ``entity_registry_updated`` event on the proxy's
    registered subscription. Tests use this to drive the live-tracking path.
    """
    sub_id = fake.update_subs.get("entity")
    ws = fake.ws
    assert sub_id is not None and ws is not None
    await ws.send_json(
        {
            "id": sub_id,
            "type": "event",
            "event": {
                "event_type": "entity_registry_updated",
                "data": {"action": action, "entity_id": entity_id},
            },
        }
    )


async def emit_device_registry_event(fake: FakeHA, action: str, device_id: str) -> None:
    """Fire a synthetic ``device_registry_updated`` event."""
    sub_id = fake.update_subs.get("device")
    ws = fake.ws
    assert sub_id is not None and ws is not None
    await ws.send_json(
        {
            "id": sub_id,
            "type": "event",
            "event": {
                "event_type": "device_registry_updated",
                "data": {"action": action, "device_id": device_id},
            },
        }
    )


def _fake_ha_app(fake: FakeHA) -> web.Application:
    app = web.Application()
    app.router.add_get("/api/websocket", _make_fake_handler(fake))
    return app


async def _setup(
    customization: Customization | None = None,
    fake: FakeHA | None = None,
    *,
    registry_mode: str = "incremental",
    registry_refetch_interval: float = 0.0,
    registry_burst_threshold: int = 50,
) -> tuple[web.AppRunner, web.AppRunner, str, FakeHA]:
    fake = fake or FakeHA()
    fake_runner, fake_url = await _start_app(_fake_ha_app(fake))
    proxy_app = proxy.create_app(
        proxy.Options(
            target_url=fake_url,
            registry=SessionRegistry(),
            customization=customization or Customization(),
            registry_mode=registry_mode,
            registry_refetch_interval=registry_refetch_interval,
            registry_burst_threshold=registry_burst_threshold,
        )
    )
    proxy_runner, proxy_url = await _start_app(proxy_app)
    return fake_runner, proxy_runner, proxy_url, fake


async def _read(ws: aiohttp.ClientWebSocketResponse) -> dict[str, Any]:
    """Read one JSON message from the proxy with a generous timeout."""
    return await asyncio.wait_for(ws.receive_json(), timeout=3.0)


async def _auth(ws: aiohttp.ClientWebSocketResponse) -> None:
    """Complete the auth handshake on the proxy-side connection. Leaves the
    socket ready for the test to send its first command.
    """
    msg = await _read(ws)
    assert msg["type"] == "auth_required"
    await ws.send_json({"type": "auth", "access_token": "x"})
    msg = await _read(ws)
    assert msg["type"] == "auth_ok"


@pytest.mark.asyncio
async def test_scope_snapshot_and_coalesced_changes_filtered() -> None:
    fake_runner, proxy_runner, proxy_url, _fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})

                res = await _read(ws)
                assert res["type"] == "result" and res["success"] and res["id"] == 1

                snap = await _read(ws)
                assert snap["type"] == "event" and snap["id"] == 1
                add = snap["event"]["a"]
                assert "light.kitchen" in add
                assert "sensor.temp" not in add, "out-of-scope entity in snapshot"

                # The coalesced array carries sensor.temp + light.kitchen changes.
                # Sensor is out of scope (filtered); the proxy must split the
                # frame and only propagate the light change.
                chg = await _read(ws)
                assert chg["type"] == "event" and chg["id"] == 1
                changed = chg["event"].get("c", {})
                assert "light.kitchen" in changed
                assert "sensor.temp" not in changed
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_device_page_navigation_rescopes() -> None:
    fake_runner, proxy_runner, proxy_url, _fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})
                await _read(ws)  # result
                snap = await _read(ws)
                assert "light.kitchen" in snap["event"]["a"]
                # Drain the propagated light.kitchen change from the coalesced frame.
                await _read(ws)

                # Navigate to dev2's settings page. The registry index built
                # at startup tells the proxy that dev2 owns sensor.temp;
                # the re-scope emits remove + add diffs.
                await ws.send_json(
                    {
                        "id": 2,
                        "type": "browser_mod/update",
                        "data": {"path": "/config/devices/device/dev2"},
                    }
                )

                removed = await _read(ws)
                assert removed["event"].get("r") == ["light.kitchen"]
                added = await _read(ws)
                assert "sensor.temp" in added["event"].get("a", {})
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_customization_implicit_entity_seeded_into_scope() -> None:
    """A customization that registers an implicit entity for a card type
    seeds that id whenever the card appears in the dashboard config — even
    if the card declares no entity references itself.
    """
    cust = Customization(
        implicit_entities={"custom:my-implicit-card": ("calendar.workday",)}
    )
    fake_runner, proxy_runner, proxy_url, _fake = await _setup(cust)
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})
                await _read(ws)  # result
                snap = await _read(ws)
                add = snap["event"]["a"]
                assert "light.kitchen" in add
                assert "calendar.workday" in add, (
                    "customization-seeded implicit entity missing from scope"
                )
                assert "sensor.temp" not in add
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_calendar_panel_navigation_scopes_to_domain() -> None:
    """Navigating to ``/calendar`` re-scopes to the ``calendar`` domain via
    ``ViewKind.DOMAIN`` — neither all-entities nor empty. The mirror has
    ``calendar.workday``; the initial dashboard scope includes
    ``light.kitchen`` (the dashboard's only entity) PLUS ``calendar.workday``
    (via ``ALWAYS_IN_SCOPE_DOMAINS``). The re-scope drops ``light.kitchen``
    and keeps ``calendar.workday`` — no add event needed because the
    calendar entity was already in scope.
    """
    fake_runner, proxy_runner, proxy_url, _fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})
                await _read(ws)  # result
                snap = await _read(ws)
                assert "light.kitchen" in snap["event"]["a"]
                # calendar.workday is always in scope regardless of view.
                assert "calendar.workday" in snap["event"]["a"]
                # Drain the propagated light.kitchen change from the coalesced frame.
                await _read(ws)

                await ws.send_json(
                    {
                        "id": 2,
                        "type": "browser_mod/update",
                        "data": {"browser": {"path": "/calendar"}},
                    }
                )

                removed = await _read(ws)
                assert removed["event"].get("r") == ["light.kitchen"]
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


# ---- Regression tests for the bugs we surfaced ----------------------------


@pytest.mark.asyncio
async def test_proxy_never_emits_id_reuse_to_ha() -> None:
    """The bug we caught: the original lazy registry-fetch used fixed
    reserved ids (9_000_000_001 / _002) issued AFTER higher ids, which
    real HA rejects with ERR_ID_REUSE. With the fake now enforcing the
    same rule, this test would have failed before the redesign. It also
    covers the full session lifecycle: auth, mirror, registries +
    update subscriptions, dashboard config, two translated client
    requests, and a settings-page navigation that exercises every
    proxy-issued id path.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})
                await _read(ws)  # result
                await _read(ws)  # snapshot
                await _read(ws)  # propagated change

                # A settings-page navigation that, in the buggy design,
                # would have triggered a lazy registry fetch with a stale
                # low id — and been rejected.
                await ws.send_json(
                    {
                        "id": 2,
                        "type": "browser_mod/update",
                        "data": {"path": "/config/devices/device/dev2"},
                    }
                )
                await _read(ws)  # remove
                await _read(ws)  # add

                # Give the writer a moment to deliver any trailing frames.
                await asyncio.sleep(0.05)

        # The fake HA tracks every id it saw in arrival order; with the
        # rule enforced on the fake side, any non-increasing id would
        # have produced an ERR_ID_REUSE response that the proxy would
        # have treated as a fatal startup failure (test would have
        # disconnected before we got here). Belt + suspenders: also
        # assert the captured order directly.
        assert fake.seen_ids == sorted(fake.seen_ids), (
            f"proxy sent non-increasing ids: {fake.seen_ids}"
        )
        # Every id is unique (strict, not just monotonic).
        assert len(set(fake.seen_ids)) == len(fake.seen_ids)
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_client_side_id_reuse_is_rejected() -> None:
    """The proxy mirrors HA's strictly-increasing rule on the client face:
    a client that reuses or regresses an id gets ERR_ID_REUSE back and the
    message is NOT forwarded.
    """
    fake_runner, proxy_runner, proxy_url, _fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 5, "type": "subscribe_entities"})
                await _read(ws)  # result
                await _read(ws)  # snapshot
                await _read(ws)  # propagated change

                # Reuse id 5 — should be rejected without forwarding.
                await ws.send_json({"id": 5, "type": "ping"})
                rej = await _read(ws)
                assert rej["id"] == 5
                assert rej["type"] == "result"
                assert rej["success"] is False
                assert rej["error"]["code"] == "id_reuse"

                # Lower id (3) — also rejected.
                await ws.send_json({"id": 3, "type": "ping"})
                rej2 = await _read(ws)
                assert rej2["error"]["code"] == "id_reuse"
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_registry_update_event_triggers_refetch_and_rescopes() -> None:
    """Live registry tracking: when HA emits an entity_registry_updated
    event mid-session, the proxy refetches the registry list. After the
    new list arrives, navigating to the newly-added device's page
    resolves correctly against the fresh index — proving the cache was
    not left stale.

    Scenario:
      * Mirror knows about ``sensor.new_outdoor`` from the start (it's a
        regular state mirror).
      * Initial registry has only ``dev1`` / ``dev2``; ``new_outdoor``
        has no device row yet, so the proxy can't scope to it on startup.
      * Mid-session, the registry gains ``dev3`` owning ``sensor.new_outdoor``
        and HA emits the corresponding ``*_registry_updated`` events.
      * Navigating to ``/config/devices/device/dev3`` should now scope to
        ``sensor.new_outdoor`` — emitting ``r: [light.kitchen]`` and
        ``a: {sensor.new_outdoor: ...}``. If the proxy hadn't refetched,
        the index would lack ``dev3``, ``_registry_scope`` would fall
        back to all-entities, and there would be no ``r:`` frame.
    """
    fake = FakeHA(
        mirror_add={
            **_DEFAULT_MIRROR_ADD,
            "sensor.new_outdoor": {
                "s": "62",
                "a": {},
                "c": "c4",
                "lc": 103.0,
            },
        }
    )
    fake_runner, proxy_runner, proxy_url, fake = await _setup(fake=fake)
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await ws.send_json({"id": 1, "type": "subscribe_entities"})
                await _read(ws)  # result
                snap = await _read(ws)
                assert "light.kitchen" in snap["event"]["a"]
                # Drain the propagated coalesced change frame.
                await _read(ws)

                # Mutate the registry: introduce a new device owning the
                # existing-in-mirror ``sensor.new_outdoor`` entity.
                fake.entities.append(
                    {
                        "entity_id": "sensor.new_outdoor",
                        "device_id": "dev3",
                        "platform": "mqtt",
                    }
                )
                fake.devices.append({"id": "dev3", "area_id": "outside"})

                # Fire synthetic registry-updated events. The proxy
                # buffers them through a 500 ms debounce window, then
                # either issues per-entity gets (incremental) or a
                # full list refetch (full mode).
                await emit_entity_registry_event(fake, "create", "sensor.new_outdoor")
                await emit_device_registry_event(fake, "create", "dev3")

                # Allow the debounce window plus refetch round trips.
                await asyncio.sleep(0.8)

                await ws.send_json(
                    {
                        "id": 2,
                        "type": "browser_mod/update",
                        "data": {"path": "/config/devices/device/dev3"},
                    }
                )

                removed = await _read(ws)
                assert removed["event"].get("r") == ["light.kitchen"], (
                    "expected light.kitchen removed; proxy may have fallen back "
                    "to all-entities, meaning the refetch didn't apply"
                )
                added = await _read(ws)
                added_keys = set(added["event"].get("a", {}).keys())
                assert "sensor.new_outdoor" in added_keys
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_startup_failure_disconnects_session() -> None:
    """If a startup-sequence request fails (here: entity_registry/list
    returns success=false), the proxy disconnects rather than falling
    back to a silently-permissive default — the rule that would have
    made the original id_reuse bug audibly fatal.
    """
    fake = FakeHA(fail_kinds={"config/entity_registry/list"})
    fake_runner, proxy_runner, proxy_url, fake = await _setup(fake=fake)
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                # The proxy should tear down the session shortly after the
                # entity_registry/list failure response arrives. Drain
                # incoming frames until we see a close-type frame; any
                # non-close frame is fine (e.g. lingering writes during
                # cleanup) but a close frame must arrive within the timeout.
                deadline = asyncio.get_event_loop().time() + 3.0
                closed = False
                while asyncio.get_event_loop().time() < deadline:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3.0)
                    if msg.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        closed = True
                        break
                assert closed, "proxy did not disconnect after startup failure"
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


# ---- Registry mode and debounce tests -------------------------------------


async def _settle(seconds: float = 0.8) -> None:
    """Wait long enough for the registry debounce window (~500 ms) and
    any follow-up round trips to land.
    """
    await asyncio.sleep(seconds)


@pytest.mark.asyncio
async def test_incremental_mode_entity_remove_is_local_only() -> None:
    """An entity_registry_updated event with action=remove must mutate
    the proxy's cache locally and NOT issue any follow-up HA request.
    Verifies the no-round-trip path for remove events.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental"
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                # Baseline counts after startup.
                base_list = fake.entity_list_count
                base_get = fake.entity_get_count

                await emit_entity_registry_event(fake, "remove", "light.kitchen")
                await _settle()

                # Zero new HA round trips for a pure remove burst.
                assert fake.entity_list_count == base_list
                assert fake.entity_get_count == base_get
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_incremental_mode_entity_create_uses_per_entity_get() -> None:
    """A single entity_registry_updated create event should trigger
    exactly one config/entity_registry/get — not a full list refetch.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental"
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_list = fake.entity_list_count
                base_get = fake.entity_get_count

                fake.entities.append(
                    {
                        "entity_id": "sensor.brand_new",
                        "device_id": "dev1",
                        "platform": "hue",
                    }
                )
                await emit_entity_registry_event(fake, "create", "sensor.brand_new")
                await _settle()

                assert fake.entity_list_count == base_list  # no full refetch
                assert fake.entity_get_count == base_get + 1  # exactly one get
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_incremental_mode_burst_promotes_to_full_refetch() -> None:
    """When a burst of create/update events exceeds the configured
    threshold, the incremental mode promotes to one full list refetch
    instead of issuing N per-entity gets.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental",
        registry_burst_threshold=3,
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_list = fake.entity_list_count
                base_get = fake.entity_get_count

                # Five creates in one debounce window — over the threshold of 3.
                for i in range(5):
                    fake.entities.append(
                        {
                            "entity_id": f"sensor.x{i}",
                            "device_id": "dev1",
                            "platform": "hue",
                        }
                    )
                    await emit_entity_registry_event(fake, "create", f"sensor.x{i}")
                await _settle()

                # Promoted to one full refetch, zero per-entity gets.
                assert fake.entity_list_count == base_list + 1
                assert fake.entity_get_count == base_get
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_full_mode_refetches_on_event() -> None:
    """Mode `full` always refetches the entire list on any event,
    matching HA frontend's pattern. No per-entity gets are used.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(registry_mode="full")
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_list = fake.entity_list_count
                base_get = fake.entity_get_count

                fake.entities.append(
                    {
                        "entity_id": "sensor.new1",
                        "device_id": "dev1",
                        "platform": "hue",
                    }
                )
                await emit_entity_registry_event(fake, "create", "sensor.new1")
                await _settle()

                assert fake.entity_list_count == base_list + 1
                assert fake.entity_get_count == base_get  # never gets in full mode
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_full_mode_debounces_burst_into_one_refetch() -> None:
    """A burst of events in full mode coalesces into a single refetch
    via the 500 ms trailing-edge debounce. Five rapid events → one
    refetch, not five.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(registry_mode="full")
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_list = fake.entity_list_count

                # Five back-to-back events well inside one debounce window.
                for i in range(5):
                    await emit_entity_registry_event(fake, "update", f"light.{i}")
                await _settle()

                # Exactly one refetch fired by the trailing edge.
                assert fake.entity_list_count == base_list + 1
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_incremental_mode_device_remove_is_local_only() -> None:
    """A device_registry_updated event with action=remove must mutate
    the cached device list locally without any HA round-trip — HA has
    no per-device get, but for `remove` we don't need one.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental"
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_list = fake.device_list_count

                await emit_device_registry_event(fake, "remove", "dev1")
                await _settle()

                assert fake.device_list_count == base_list
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_incremental_mode_device_create_refetches_device_list() -> None:
    """HA exposes no config/device_registry/get, so an incremental-mode
    device create/update event has to refetch the full device list.
    The entity list is NOT refetched (devices and entities are
    independent).
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental"
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_entity_list = fake.entity_list_count
                base_device_list = fake.device_list_count

                fake.devices.append({"id": "dev3", "area_id": "outside"})
                await emit_device_registry_event(fake, "create", "dev3")
                await _settle()

                # Device list refetched; entity list untouched.
                assert fake.device_list_count == base_device_list + 1
                assert fake.entity_list_count == base_entity_list
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_client_lovelace_config_resolves_scope() -> None:
    """Regression test for the bootstrap path used by every browser/kiosk
    that loads a non-default dashboard.

    When a client sends its own ``lovelace/config`` request (because the
    user navigated to a specific dashboard URL), the proxy must intercept
    the response and feed it to ``_resolve_scope`` — otherwise the scope
    stays at whatever the proxy's eager startup fetch resolved (typically
    the default dashboard) and the client gets the wrong entity set.

    This case was missed in the initial test suite: every existing
    navigation test drives scope changes via ``browser_mod/update``,
    which is path-based and does not exercise client-initiated config
    fetches. A real browser tab loading ``/my-dashboard`` sends
    ``lovelace/config?url_path=my-dashboard`` and expects scope to
    reflect that dashboard's contents.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup()
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)

                # Client fetches its dashboard's lovelace/config — the
                # bootstrap path every browser hits on page load.
                await ws.send_json(
                    {
                        "id": 1,
                        "type": "lovelace/config",
                        "url_path": "my-dashboard",
                    }
                )
                cfg = await _read(ws)
                assert cfg["id"] == 1
                assert cfg["type"] == "result"
                assert cfg["success"]
                # The fake's my-dashboard config references sensor.temp.

                # Now subscribe. The proxy must have used the client's
                # lovelace/config response to resolve scope to
                # my-dashboard's entities, NOT widened-to-all.
                await ws.send_json({"id": 2, "type": "subscribe_entities"})
                await _read(ws)  # result
                snap = await _read(ws)
                add = snap["event"]["a"]
                # In scope: sensor.temp (from my-dashboard).
                assert "sensor.temp" in add, (
                    "client-side lovelace/config response did not feed "
                    "scope resolution; bootstrap-time dashboards stay "
                    "widened to all entities"
                )
                # Out of scope: light.kitchen.
                assert "light.kitchen" not in add
                # Always in scope: calendar.workday (ALWAYS_IN_SCOPE_DOMAINS).
                assert "calendar.workday" in add
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()


@pytest.mark.asyncio
async def test_periodic_refetch_runs_on_interval() -> None:
    """The periodic refetch timer issues both list refetches every
    `registry_refetch_interval` seconds as a safety net for missed
    events. Configure a short interval and wait.
    """
    fake_runner, proxy_runner, proxy_url, fake = await _setup(
        registry_mode="incremental",
        registry_refetch_interval=0.3,
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await _auth(ws)
                await asyncio.wait_for(fake.on_lovelace_config.wait(), timeout=2.0)
                base_entity_list = fake.entity_list_count
                base_device_list = fake.device_list_count

                # Wait for ~2 intervals; expect at least 2 refetches of each.
                await asyncio.sleep(0.75)

                assert fake.entity_list_count >= base_entity_list + 2
                assert fake.device_list_count >= base_device_list + 2
    finally:
        await fake_runner.cleanup()
        await proxy_runner.cleanup()
