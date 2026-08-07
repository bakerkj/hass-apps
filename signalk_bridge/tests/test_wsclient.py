# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the Signal K delta websocket subscriber."""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
import websockets
from signalk_bridge.wsclient import WSSubscriber, _ws_url

DeltaSender = Callable[[dict[str, Any]], Awaitable[None]]


class _Server:
    """Minimal in-process SK WS server for tests.

    Accepts one client at a time; the test coroutine drives it by calling
    :meth:`send` to push a delta, and :meth:`drop` to disconnect the client.
    """

    def __init__(self) -> None:
        self._client: websockets.WebSocketServerProtocol | None = None
        self._connected = asyncio.Event()
        self.headers_seen: list[dict[str, str]] = []

    async def _handler(self, ws: websockets.WebSocketServerProtocol) -> None:
        self.headers_seen.append(dict(ws.request.headers))
        self._client = ws
        self._connected.set()
        try:
            async for _ in ws:
                pass  # subscription is via URL param; client isn't expected to send
        except websockets.exceptions.WebSocketException:
            pass

    async def wait_connected(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout=timeout)

    async def send(self, msg: dict[str, Any]) -> None:
        assert self._client is not None, "no client connected"
        await self._client.send(json.dumps(msg))

    async def drop(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._connected.clear()
            self._client = None


@pytest_asyncio.fixture
async def sk_ws() -> AsyncIterator[tuple[str, _Server]]:
    """Yields ``(base_url, server)`` where ``base_url`` is HTTP-shaped
    (as the caller would supply); ``_ws_url`` translates it to ``ws://``."""
    server = _Server()
    async with websockets.serve(server._handler, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", server


@pytest.mark.asyncio
async def test_ws_url_translation() -> None:
    assert _ws_url("http://sk:3000").startswith("ws://sk:3000/signalk/v1/stream")
    assert _ws_url("https://sk:3000").startswith("wss://sk:3000/signalk/v1/stream")
    assert "subscribe=self" in _ws_url("http://sk:3000")


@pytest.mark.asyncio
async def test_delta_updates_tree(sk_ws: tuple[str, _Server]) -> None:
    base, server = sk_ws
    sub = WSSubscriber(base)
    await sub.start()
    try:
        await server.wait_connected()
        await server.send(
            {
                "context": "vessels.self",
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {
                                "path": "electrical.batteries.house.voltage",
                                "value": 12.75,
                            }
                        ],
                    }
                ],
            }
        )
        # Poll snapshot until the delta lands (WS is async; give the reader
        # a beat to apply).
        for _ in range(50):
            snap = await sub.snapshot()
            leaf = (
                snap.get("electrical", {})
                .get("batteries", {})
                .get("house", {})
                .get("voltage")
            )
            if leaf is not None:
                break
            await asyncio.sleep(0.02)
        assert leaf is not None
        assert leaf["value"] == 12.75
        assert leaf["$source"] == "n2k-can0.c0"
        assert leaf["timestamp"] == "2026-08-05T04:00:00.000Z"
        # values{} fanout leaf is populated for multi-source disambiguation
        assert leaf["values"]["n2k-can0.c0"]["value"] == 12.75
        # dirty set carries the path once, cleared on read
        dirty = await sub.take_dirty()
        assert "electrical.batteries.house.voltage" in dirty
        assert await sub.take_dirty() == set()
    finally:
        await sub.stop()


@pytest.mark.asyncio
async def test_bootstrap_preserved_and_deltas_patch_in_place(
    sk_ws: tuple[str, _Server],
) -> None:
    base, server = sk_ws
    sub = WSSubscriber(base)
    sub.bootstrap(
        {
            "electrical": {
                "batteries": {
                    "house": {
                        "voltage": {
                            "value": 12.5,
                            "meta": {"units": "V"},
                            "timestamp": "2026-08-05T03:59:00.000Z",
                        }
                    }
                }
            },
            "navigation": {"speedOverGround": {"value": 0.0}},
        }
    )
    await sub.start()
    try:
        await server.wait_connected()
        await server.send(
            {
                "context": "vessels.self",
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {
                                "path": "electrical.batteries.house.voltage",
                                "value": 12.9,
                            }
                        ],
                    }
                ],
            }
        )
        for _ in range(50):
            snap = await sub.snapshot()
            v = snap["electrical"]["batteries"]["house"]["voltage"]
            if v.get("value") == 12.9:
                break
            await asyncio.sleep(0.02)
        # Value updated in place; meta preserved from bootstrap.
        assert v["value"] == 12.9
        assert v["meta"] == {"units": "V"}
        # Unrelated bootstrap paths still there.
        assert snap["navigation"]["speedOverGround"]["value"] == 0.0
    finally:
        await sub.stop()


@pytest.mark.asyncio
async def test_bearer_token_sent(sk_ws: tuple[str, _Server]) -> None:
    base, server = sk_ws
    sub = WSSubscriber(base, token="secret-token")
    await sub.start()
    try:
        await server.wait_connected()
        # HTTP header keys are case-insensitive; websockets lowercases them.
        assert server.headers_seen[-1].get("authorization") == "Bearer secret-token"
    finally:
        await sub.stop()


@pytest.mark.asyncio
async def test_reconnect_on_drop(sk_ws: tuple[str, _Server]) -> None:
    base, server = sk_ws
    sub = WSSubscriber(base)
    await sub.start()
    try:
        await server.wait_connected()
        await server.drop()
        # Reconnect happens on the internal backoff schedule (starts at 1s).
        # Wait past that; a fresh handshake sets ``_connected`` again.
        await server.wait_connected(timeout=8.0)
        # Prove the fresh session works.
        await server.send(
            {
                "context": "vessels.self",
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:01:00.000Z",
                        "values": [{"path": "navigation.headingTrue", "value": 1.23}],
                    }
                ],
            }
        )
        for _ in range(50):
            snap = await sub.snapshot()
            leaf = snap.get("navigation", {}).get("headingTrue")
            if leaf is not None:
                break
            await asyncio.sleep(0.02)
        assert leaf is not None and leaf["value"] == 1.23
    finally:
        await sub.stop()


@pytest.mark.asyncio
async def test_ais_delta_populates_ais_snapshot_not_self(
    sk_ws: tuple[str, _Server],
) -> None:
    base, server = sk_ws
    # include_ais=True widens the subscription and enables the AIS store.
    sub = WSSubscriber(base, include_ais=True)
    await sub.start()
    try:
        await server.wait_connected()
        # AIS position for MMSI 367674550
        await server.send(
            {
                "context": "vessels.urn:mrn:imo:mmsi:367674550",
                "updates": [
                    {
                        "$source": "ais",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {
                                "path": "navigation.position",
                                "value": {"latitude": 42.0, "longitude": -71.0},
                            }
                        ],
                    }
                ],
            }
        )
        # And a self delta so we can confirm they don't cross.
        await server.send(
            {
                "context": "vessels.self",
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {"path": "navigation.speedOverGround", "value": 3.0}
                        ],
                    }
                ],
            }
        )
        for _ in range(50):
            ais = await sub.ais_snapshot()
            snap = await sub.snapshot()
            if ais and snap.get("navigation", {}).get("speedOverGround") is not None:
                break
            await asyncio.sleep(0.02)
        assert "367674550" in ais
        target = ais["367674550"]
        assert target["navigation"]["position"]["value"] == {
            "latitude": 42.0,
            "longitude": -71.0,
        }
        # Self tree unaffected by the AIS delta.
        assert snap["navigation"]["speedOverGround"]["value"] == 3.0
        assert snap.get("navigation", {}).get("position") is None
        # forget_ais drops the entry.
        sub.forget_ais("367674550")
        assert "367674550" not in await sub.ais_snapshot()
    finally:
        await sub.stop()


@pytest.mark.asyncio
async def test_non_self_context_is_ignored(sk_ws: tuple[str, _Server]) -> None:
    base, server = sk_ws
    sub = WSSubscriber(base)
    await sub.start()
    try:
        await server.wait_connected()
        # AIS context: must NOT leak into vessels/self mirror.
        await server.send(
            {
                "context": "vessels.urn:mrn:imo:mmsi:367674550",
                "updates": [
                    {
                        "$source": "ais",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {
                                "path": "navigation.position",
                                "value": {"latitude": 42.0, "longitude": -71.0},
                            }
                        ],
                    }
                ],
            }
        )
        # And a self one that SHOULD land.
        await server.send(
            {
                "context": "vessels.self",
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {"path": "navigation.speedOverGround", "value": 3.0}
                        ],
                    }
                ],
            }
        )
        for _ in range(50):
            snap = await sub.snapshot()
            if snap.get("navigation", {}).get("speedOverGround") is not None:
                break
            await asyncio.sleep(0.02)
        assert snap["navigation"]["speedOverGround"]["value"] == 3.0
        # The AIS delta must not have landed at navigation.position.
        assert snap.get("navigation", {}).get("position") is None
    finally:
        with contextlib.suppress(Exception):
            await sub.stop()


@pytest.mark.asyncio
async def test_self_context_via_urn_from_hello(sk_ws: tuple[str, _Server]) -> None:
    """SK delivers ``?subscribe=self`` deltas with ``context`` set to the
    vessel's canonical URN (``vessels.urn:mrn:signalk:uuid:<uuid>``), not the
    ``vessels.self`` alias. The router must learn the URN from the WS hello
    frame and treat it as an equivalent self context -- otherwise every delta
    is dropped and the mirror stays frozen at the bootstrap snapshot.
    """
    base, server = sk_ws
    self_urn = "vessels.urn:mrn:signalk:uuid:e7686c68-c2f1-42b1-8cdc-516a3972e162"
    sub = WSSubscriber(base)
    await sub.start()
    try:
        await server.wait_connected()
        # SK's first frame is the server hello, which declares the self URN.
        await server.send(
            {
                "name": "signalk-server",
                "version": "2.30.0",
                "self": self_urn,
            }
        )
        # A subsequent delta uses the URN as context, not "vessels.self".
        await server.send(
            {
                "context": self_urn,
                "updates": [
                    {
                        "$source": "n2k-can0.c0",
                        "timestamp": "2026-08-05T04:00:00.000Z",
                        "values": [
                            {
                                "path": "electrical.batteries.house.voltage",
                                "value": 12.75,
                            }
                        ],
                    }
                ],
            }
        )
        snap: dict[str, Any] = {}
        for _ in range(50):
            snap = await sub.snapshot()
            leaf = (
                snap.get("electrical", {})
                .get("batteries", {})
                .get("house", {})
                .get("voltage")
            )
            if leaf is not None:
                break
            await asyncio.sleep(0.02)
        assert snap["electrical"]["batteries"]["house"]["voltage"]["value"] == 12.75
    finally:
        with contextlib.suppress(Exception):
            await sub.stop()
