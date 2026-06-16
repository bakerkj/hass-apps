# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the reverse-proxy paths in ``proxy.py``: header filtering,
HTTP forwarding, WebSocket tunneling, dial failures, and the
``TunnelConnection`` accounting that backs the status UI.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import aiohttp
from aiohttp import web
from multidict import CIMultiDict, CIMultiDictProxy

from dashboard_entity_proxy import proxy
from dashboard_entity_proxy.customization import Customization
from dashboard_entity_proxy.session import SessionRegistry


# ---- helpers ---------------------------------------------------------------


from _aiohttp_helpers import _free_port, _start_app


# ---- header / URL helpers (pure functions) ---------------------------------


def test_http_to_ws_swaps_scheme() -> None:
    assert proxy._http_to_ws("http://homeassistant:8123") == "ws://homeassistant:8123"
    assert proxy._http_to_ws("https://ha.example.com") == "wss://ha.example.com"
    assert (
        proxy._http_to_ws("http://homeassistant:8123/path/")
        == "ws://homeassistant:8123/path"
    )


def test_client_addr_prefers_x_real_ip() -> None:
    """``X-Real-IP`` is the most direct nginx-supplied "this is the
    actual client" hint and wins over both ``X-Forwarded-For`` and the
    aiohttp peer address.
    """
    from types import SimpleNamespace

    req = SimpleNamespace(
        headers={"X-Real-IP": "192.168.13.110", "X-Forwarded-For": "1.2.3.4"},
        remote="127.0.0.1",
    )
    assert proxy._client_addr(req) == "192.168.13.110"  # type: ignore[arg-type]


def test_client_addr_falls_back_to_x_forwarded_for_leftmost() -> None:
    """When only ``X-Forwarded-For`` is set, the leftmost entry is the
    original client.
    """
    from types import SimpleNamespace

    req = SimpleNamespace(
        headers={"X-Forwarded-For": "192.168.13.110, 10.0.0.1, 172.30.32.2"},
        remote="127.0.0.1",
    )
    assert proxy._client_addr(req) == "192.168.13.110"  # type: ignore[arg-type]


def test_client_addr_falls_back_to_remote_when_no_headers() -> None:
    """Direct connection without the nginx headers (e.g. a unit test or
    a misconfigured deployment) gets the aiohttp peer address.
    """
    from types import SimpleNamespace

    req = SimpleNamespace(headers={}, remote="10.0.0.5")
    assert proxy._client_addr(req) == "10.0.0.5"  # type: ignore[arg-type]


def test_filtered_response_headers_preserves_multiple_set_cookie() -> None:
    """HA's login flow rotates multiple ``Set-Cookie`` headers on a single
    response; collapsing them to one (via ``dict[str, str]``) breaks auth.
    The filter must return a ``CIMultiDict`` that keeps every value.
    """
    raw: CIMultiDict[str] = CIMultiDict()
    raw.add("Set-Cookie", "session=abc; Path=/; HttpOnly")
    raw.add("Set-Cookie", "csrf=xyz; Path=/; Secure")
    raw.add("Content-Type", "text/html")
    # Hop-by-hop, must be dropped.
    raw.add("Connection", "close")
    upstream = SimpleNamespace(headers=CIMultiDictProxy(raw))
    request = SimpleNamespace(scheme="https")

    out = proxy._filtered_response_headers(upstream, request)  # type: ignore[arg-type]

    assert isinstance(out, CIMultiDict)
    assert out.getall("Set-Cookie") == [
        "session=abc; Path=/; HttpOnly",
        "csrf=xyz; Path=/; Secure",
    ]
    assert out.get("Content-Type") == "text/html"
    assert "Connection" not in out
    # The result type must be accepted by web.StreamResponse(headers=...).
    web.StreamResponse(headers=out)


def test_filtered_response_headers_strips_hsts_on_plain_http_downstream() -> None:
    """If the downstream leg is plain HTTP, leaking ``Strict-Transport-Security``
    from an HTTPS upstream would pin the host in the browser's HSTS cache
    and break later plain-HTTP access. The filter must strip HSTS (and the
    deprecated ``Public-Key-Pins``) in that case, but preserve them when the
    downstream is itself HTTPS.
    """
    raw: CIMultiDict[str] = CIMultiDict()
    raw.add("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    raw.add("Public-Key-Pins", 'pin-sha256="abc"; max-age=5184000')
    raw.add("Content-Type", "text/html")
    upstream = SimpleNamespace(headers=CIMultiDictProxy(raw))

    # Plain-HTTP downstream: both headers stripped.
    out_http = proxy._filtered_response_headers(
        upstream,  # type: ignore[arg-type]
        SimpleNamespace(scheme="http"),  # type: ignore[arg-type]
    )
    assert "Strict-Transport-Security" not in out_http
    assert "Public-Key-Pins" not in out_http
    assert out_http.get("Content-Type") == "text/html"

    # HTTPS downstream: HSTS preserved (safe to advertise).
    out_https = proxy._filtered_response_headers(
        upstream,  # type: ignore[arg-type]
        SimpleNamespace(scheme="https"),  # type: ignore[arg-type]
    )
    assert (
        out_https.get("Strict-Transport-Security")
        == "max-age=31536000; includeSubDomains"
    )
    assert out_https.get("Public-Key-Pins") == 'pin-sha256="abc"; max-age=5184000'


def test_filtered_request_headers_preserves_multiple_values() -> None:
    """A multi-valued custom request header must survive the filter so
    that ``ClientSession.request(headers=...)`` receives both values.
    """
    raw: CIMultiDict[str] = CIMultiDict()
    raw.add("X-Custom", "one")
    raw.add("X-Custom", "two")
    raw.add("Authorization", "Bearer t")
    # Hop-by-hop, must be dropped.
    raw.add("Connection", "keep-alive")
    request = SimpleNamespace(headers=CIMultiDictProxy(raw), remote="1.2.3.4")

    out = proxy._filtered_request_headers(request, transparent=True)  # type: ignore[arg-type]

    assert isinstance(out, CIMultiDict)
    assert out.getall("X-Custom") == ["one", "two"]
    assert out.get("Authorization") == "Bearer t"
    assert "Connection" not in out
    # Transparent mode does not inject X-Forwarded-For.
    assert "X-Forwarded-For" not in out


def test_filtered_request_headers_keeps_nginx_xff_in_non_transparent_mode() -> None:
    """nginx already built ``X-Forwarded-For`` from the real client IP via
    ``$proxy_add_x_forwarded_for`` before forwarding to the loopback Python
    proxy. The proxy must NOT overwrite that with ``request.remote``
    (which is always ``127.0.0.1`` because the Python proxy binds
    loopback-only) — that would silently feed HA's ``trusted_proxies``
    handling the wrong client address.
    """
    raw: CIMultiDict[str] = CIMultiDict()
    raw.add("X-Forwarded-For", "203.0.113.42")
    raw.add("X-Forwarded-Proto", "https")
    request = SimpleNamespace(headers=CIMultiDictProxy(raw), remote="127.0.0.1")

    out = proxy._filtered_request_headers(request, transparent=False)  # type: ignore[arg-type]

    # The real client address survives unchanged.
    assert out.get("X-Forwarded-For") == "203.0.113.42"
    assert out.get("X-Forwarded-Proto") == "https"


def test_tunnel_connection_status_counts() -> None:
    conn = proxy.TunnelConnection(
        remote_addr="1.2.3.4", target_path="/api/websocket", passthrough=True
    )
    conn.msgs_client_to_ha = 5
    conn.msgs_ha_to_client = 7
    snap = conn.status()
    assert snap["kind"] == "passthrough"
    assert snap["remote_addr"] == "1.2.3.4"
    assert snap["target_path"] == "/api/websocket"
    assert snap["messages_client_to_ha"] == 5
    assert snap["messages_ha_to_client"] == 7
    assert "connected_at" in snap


def test_tunnel_connection_kind_is_tunnel_for_non_passthrough() -> None:
    conn = proxy.TunnelConnection(
        remote_addr="1.2.3.4", target_path="/custom/ws", passthrough=False
    )
    assert conn.status()["kind"] == "tunnel"


def test_tunnel_connection_surfaces_subprotocol_and_addon_slug() -> None:
    """Subprotocol and addon-slug both flow into the snapshot so the UI
    can show e.g. ``esphome-events`` and ``esphome_dist_server`` next
    to the connection.
    """
    conn = proxy.TunnelConnection(
        remote_addr="1.2.3.4",
        target_path="/api/hassio_ingress/abc/events",
        passthrough=False,
        subprotocol="esphome-events",
        addon_slug="esphome_dist_server",
    )
    snap = conn.status()
    assert snap["subprotocol"] == "esphome-events"
    assert snap["addon_slug"] == "esphome_dist_server"
    # Defaults: no frames yet, no close meta.
    assert snap["last_msg_at"] is None
    assert snap["close_code"] is None
    assert snap["close_reason"] == ""


def test_tunnel_connection_mark_frame_sets_last_msg_at() -> None:
    """``mark_frame`` records the most-recent activity timestamp so
    the UI can show ``Xs ago`` for tunnels that connected cleanly but
    then went silent.
    """
    conn = proxy.TunnelConnection(
        remote_addr="1.2.3.4", target_path="/custom/ws", passthrough=False
    )
    assert conn.status()["last_msg_at"] is None
    conn.mark_frame()
    assert conn.status()["last_msg_at"] is not None


def test_tunnel_connection_mark_closed_surfaces_close_meta() -> None:
    """``mark_closed`` records the disconnect code / reason so the
    retained card during the 60-s window shows why the tunnel ended.
    """
    conn = proxy.TunnelConnection(
        remote_addr="1.2.3.4", target_path="/custom/ws", passthrough=False
    )
    conn.mark_closed(1011, "internal error")
    snap = conn.status()
    assert snap["close_code"] == 1011
    assert snap["close_reason"] == "internal error"


def test_tunnel_connection_decode_addon_slug() -> None:
    """The panel-style Referer URL carries the addon slug after the
    8-hex install-hash prefix; non-panel URLs (lovelace, ws upgrade
    referrers) return empty.
    """
    decode = proxy.TunnelConnection.decode_addon_slug
    assert (
        decode("http://hass:8126/8455f921_esphome_dist_server") == "esphome_dist_server"
    )
    assert (
        decode("http://hass:8126/8455f921_esphome_dist_server/")
        == "esphome_dist_server"
    )
    assert (
        decode("http://hass:8126/8455f921_esphome_dist_server/builds")
        == "esphome_dist_server"
    )
    assert decode("http://hass:8126/lighting-test/0") == ""
    assert decode("") == ""
    # Hash must be 8 hex chars exactly; near-misses don't match.
    assert decode("http://hass:8126/8455F921_too_uppercase") == ""
    assert decode("http://hass:8126/8455f9_too_short") == ""


def _opts(target_url: str, **kw: Any) -> proxy.Options:
    return proxy.Options(
        target_url=target_url,
        registry=kw.get("registry"),
        transparent=kw.get("transparent", True),
        passthrough_all=kw.get("passthrough_all", False),
        customization=Customization(),
    )


# ---- _reverse_http end-to-end ----------------------------------------------


async def _fake_http_app(captured: dict[str, Any]) -> web.Application:
    """A fake HA HTTP server that records the request it received and
    returns a deterministic response so the test can assert what passed
    through.
    """

    async def handler(request: web.Request) -> web.Response:
        captured["method"] = request.method
        captured["path"] = request.path
        captured["headers"] = dict(request.headers)
        captured["body"] = await request.read()
        return web.Response(
            status=201,
            body=b'{"ok":true}',
            headers={
                "Content-Type": "application/json",
                "X-Upstream-Echo": "hello",
                # Hop-by-hop response header that must be stripped.
                "Connection": "close",
                # ``identity`` is the no-op encoding; the proxy
                # passes the body through untouched
                # (``auto_decompress=False``) so this header is
                # preserved on the way back to the client.
                "Content-Encoding": "identity",
            },
        )

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    return app


async def test_reverse_http_forwards_method_path_body_and_strips_hop_by_hop() -> None:
    captured: dict[str, Any] = {}
    ha_runner, ha_url = await _start_app(await _fake_http_app(captured))
    proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.post(
                f"{proxy_url}/api/states/light.kitchen",
                data=b'{"state":"on"}',
                headers={
                    "Authorization": "Bearer token123",
                    # Hop-by-hop request headers that must be stripped.
                    "Connection": "keep-alive",
                    "Keep-Alive": "timeout=5",
                },
            ) as resp:
                body = await resp.read()
                assert resp.status == 201
                assert body == b'{"ok":true}'
                # Hop-by-hop stripped from response; Content-Encoding
                # passes through (the proxy forwards bytes verbatim
                # rather than re-encoding).
                assert "Connection" not in resp.headers
                assert resp.headers.get("Content-Encoding") == "identity"
                assert resp.headers["X-Upstream-Echo"] == "hello"

        # Upstream received only the surviving headers / body / path.
        assert captured["method"] == "POST"
        assert captured["path"] == "/api/states/light.kitchen"
        assert captured["body"] == b'{"state":"on"}'
        assert captured["headers"].get("Authorization") == "Bearer token123"
        assert "Connection" not in captured["headers"]
        assert "Keep-Alive" not in captured["headers"]
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_reverse_http_strips_x_forwarded_in_transparent_mode() -> None:
    captured: dict[str, Any] = {}
    ha_runner, ha_url = await _start_app(await _fake_http_app(captured))
    proxy_runner, proxy_url = await _start_app(
        proxy.create_app(_opts(ha_url, transparent=True))
    )
    try:
        async with aiohttp.ClientSession() as cs:
            await cs.get(
                f"{proxy_url}/api/",
                headers={
                    "X-Forwarded-For": "10.0.0.1",
                    "X-Forwarded-Proto": "https",
                    "Forwarded": "for=10.0.0.1",
                },
            )
        assert "X-Forwarded-For" not in captured["headers"]
        assert "X-Forwarded-Proto" not in captured["headers"]
        assert "Forwarded" not in captured["headers"]
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_reverse_http_preserves_incoming_xff_when_not_transparent() -> None:
    """In production the Python proxy sits behind nginx on loopback, so
    ``request.remote`` is always ``127.0.0.1`` and the real client IP
    arrives via nginx's ``X-Forwarded-For``. The proxy must preserve
    nginx's value verbatim — synthesizing one from ``request.remote``
    would clobber it with the loopback address.
    """
    captured: dict[str, Any] = {}
    ha_runner, ha_url = await _start_app(await _fake_http_app(captured))
    proxy_runner, proxy_url = await _start_app(
        proxy.create_app(_opts(ha_url, transparent=False))
    )
    try:
        async with aiohttp.ClientSession() as cs:
            await cs.get(
                f"{proxy_url}/api/",
                headers={"X-Forwarded-For": "203.0.113.42"},
            )
        # The upstream sees nginx's value unchanged, not a loopback synthesis.
        assert captured["headers"].get("X-Forwarded-For") == "203.0.113.42"
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_reverse_http_returns_502_on_upstream_failure() -> None:
    # Point the proxy at a closed port. ``create_app`` builds the ClientSession
    # at app startup, so we don't need a running HA to test the failure path.
    closed_port = _free_port()
    ha_url = f"http://127.0.0.1:{closed_port}"
    proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(f"{proxy_url}/anything") as resp:
                assert resp.status == 502
                assert (await resp.text()) == "Bad Gateway"
    finally:
        await proxy_runner.cleanup()


# ---- _tunnel_ws end-to-end -------------------------------------------------


async def _fake_ws_echo_app() -> web.Application:
    """A fake HA WebSocket endpoint that echoes whatever it receives,
    so the tunnel test can assert message accounting and shape preservation.
    """

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await ws.send_str("echo:" + msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await ws.send_bytes(b"echo:" + msg.data)
            else:
                break
        return ws

    app = web.Application()
    app.router.add_route("GET", "/{tail:.*}", ws_handler)
    return app


async def test_tunnel_ws_passes_text_and_binary_and_counts() -> None:
    registry = SessionRegistry()
    ha_runner, ha_url = await _start_app(await _fake_ws_echo_app())
    proxy_runner, proxy_url = await _start_app(
        proxy.create_app(_opts(ha_url, registry=registry))
    )
    try:
        async with aiohttp.ClientSession() as cs:
            # /custom/ws is NOT /api/websocket so this goes through _tunnel_ws,
            # not _intercept.
            async with cs.ws_connect(proxy_url + "/custom/ws") as ws:
                await ws.send_str("hello")
                msg = await ws.receive(timeout=2.0)
                assert msg.type == aiohttp.WSMsgType.TEXT
                assert msg.data == "echo:hello"

                await ws.send_bytes(b"world")
                msg = await ws.receive(timeout=2.0)
                assert msg.type == aiohttp.WSMsgType.BINARY
                assert msg.data == b"echo:world"

                # Verify the TunnelConnection is registered while open.
                snap = registry.snapshot()
                assert len(snap) == 1
                assert snap[0]["kind"] == "tunnel"
                assert snap[0]["target_path"] == "/custom/ws"

                await ws.close()

        # After close the connection is removed from the live set; it
        # lingers in the recent set for the retention window.
        await asyncio.sleep(0.05)
        recents = [s for s in registry.snapshot() if "disconnected_at" in s]
        assert len(recents) == 1
        # The two echoes — one text + one binary — got counted in both
        # directions before close.
        assert recents[0]["messages_client_to_ha"] == 2
        assert recents[0]["messages_ha_to_client"] == 2
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_tunnel_ws_handles_dial_failure() -> None:
    # HA isn't running on this port. The proxy completes the client
    # handshake first (it has to, to send WS frames in response), then
    # dials HA, fails, logs, and closes the client. The test client sees
    # a successful upgrade followed by an immediate close — no raise.
    closed_port = _free_port()
    ha_url = f"http://127.0.0.1:{closed_port}"
    proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/custom/ws") as ws:
                msg = await ws.receive(timeout=2.0)
                assert msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                )
    finally:
        await proxy_runner.cleanup()


# ---- passthrough_all dispatch ---------------------------------------------


async def test_ws_dial_forwards_custom_headers_and_strips_handshake() -> None:
    """The WS dial must reuse the HTTP header policy: a custom client
    header (e.g. ``X-Test-Header``) survives, while the
    ``Sec-WebSocket-*`` handshake headers and ``Upgrade``/``Connection``
    are stripped (aiohttp regenerates those on the upstream socket).
    """
    captured: dict[str, Any] = {}

    class _FakeWS:
        async def close(self, *, code: int = 1000, message: bytes = b"") -> None:
            return None

        def __aiter__(self) -> Any:
            return self

        async def __anext__(self) -> Any:
            raise StopAsyncIteration

    async def fake_ws_connect(url: str, **kwargs: Any) -> Any:
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        return _FakeWS()

    proxy_app = proxy.create_app(_opts("http://127.0.0.1:1"))
    proxy_runner, proxy_url = await _start_app(proxy_app)
    # Swap in a fake upstream client whose ws_connect records its kwargs.
    real_client = proxy_app[proxy.CLIENT_KEY]
    proxy_app[proxy.CLIENT_KEY] = SimpleNamespace(  # type: ignore[misc]
        ws_connect=fake_ws_connect, close=real_client.close
    )
    try:
        async with aiohttp.ClientSession() as cs:
            try:
                async with cs.ws_connect(
                    proxy_url + "/custom/ws",
                    headers={"X-Test-Header": "foo"},
                ) as ws:
                    # Server-side _tunnel_ws will return immediately because
                    # the fake upstream iterator is empty; client sees a close.
                    await ws.receive(timeout=2.0)
            except aiohttp.ClientError:
                # The fake upstream short-circuits; the client may observe
                # a close mid-handshake. We only care about what headers
                # the proxy passed to ws_connect.
                pass
    finally:
        await proxy_runner.cleanup()

    assert captured.get("url") is not None
    headers = captured["headers"]
    # Custom header forwarded.
    assert headers.get("X-Test-Header") == "foo"
    # WS-handshake headers stripped — aiohttp regenerates them on the dial.
    lower = {k.lower() for k in headers}
    assert "upgrade" not in lower
    assert "connection" not in lower
    assert "sec-websocket-key" not in lower
    assert "sec-websocket-version" not in lower
    assert "sec-websocket-extensions" not in lower


async def test_tunnel_ws_propagates_close_code() -> None:
    """When HA closes the upstream socket with a non-default code/reason,
    the proxy must forward those values to the client — not the default
    1000/"OK" that a bare ``ws.close()`` would emit.
    """

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        # Wait for one frame so the client has actually established the
        # tunnel end-to-end, then close with a distinctive code/reason.
        msg = await ws.receive(timeout=2.0)
        assert msg.type == aiohttp.WSMsgType.TEXT
        await ws.close(code=1011, message=b"internal")
        return ws

    ha_app = web.Application()
    ha_app.router.add_route("GET", "/{tail:.*}", ws_handler)
    ha_runner, ha_url = await _start_app(ha_app)
    proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/custom/ws") as ws:
                await ws.send_str("hi")
                # Drain until we observe the CLOSE frame; aiohttp surfaces
                # the upstream close code/reason via ``msg.data`` /
                # ``msg.extra`` on the WSMessage of type CLOSE.
                close_code: int | None = None
                close_reason: str | None = None
                for _ in range(4):
                    msg = await ws.receive(timeout=2.0)
                    if msg.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        if isinstance(msg.data, int):
                            close_code = msg.data
                        if isinstance(msg.extra, str):
                            close_reason = msg.extra
                        break
                assert close_code == 1011
                assert close_reason == "internal"
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_tunnel_ws_propagates_close_code_from_client_to_ha() -> None:
    """When the *client* closes the tunnel with a non-default code/reason,
    the proxy must forward those values to HA. This exercises the path
    where the HA-side pump is the survivor — it must be cancelled and
    awaited so the close-code propagation runs on consistent state, not
    on a half-finished pump.
    """
    seen_close: dict[str, Any] = {}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        # Drain whatever the client sends. We expect the client to send
        # a frame and then close with code 4000/"client-quit".
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                continue
        # The async-for loop on aiohttp's WS surfaces close metadata
        # via the ws object after the loop exits.
        seen_close["code"] = ws.close_code
        return ws

    ha_app = web.Application()
    ha_app.router.add_route("GET", "/{tail:.*}", ws_handler)
    ha_runner, ha_url = await _start_app(ha_app)
    proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/custom/ws") as ws:
                await ws.send_str("hi")
                await ws.close(code=4000, message=b"client-quit")
        # Give the proxy a moment to propagate the close.
        for _ in range(20):
            if "code" in seen_close:
                break
            await asyncio.sleep(0.05)
        assert seen_close.get("code") == 4000
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()


async def test_http_forward_returns_504_on_upstream_timeout() -> None:
    """A slow upstream that exceeds the configured sock_read / total
    budget must surface as a clean 504, not a 502 or a hang.
    """
    from dashboard_entity_proxy import const

    async def slow_handler(request: web.Request) -> web.Response:
        # Sleep longer than the test's sock_read budget so the proxy's
        # ClientSession trips its timeout before we ever respond.
        await asyncio.sleep(5.0)
        return web.Response(text="too late")

    ha_app = web.Application()
    ha_app.router.add_route("*", "/{tail:.*}", slow_handler)
    ha_runner, ha_url = await _start_app(ha_app)

    # Patch the timeout constants down so the test runs in well under a
    # second — the production values (60s / 30s) would make this test
    # too slow. Restored in the finally.
    orig_total = const.HTTP_FORWARD_TIMEOUT_TOTAL
    orig_connect = const.HTTP_FORWARD_TIMEOUT_CONNECT
    orig_sock_read = const.HTTP_FORWARD_TIMEOUT_SOCK_READ
    const.HTTP_FORWARD_TIMEOUT_TOTAL = 0.3
    const.HTTP_FORWARD_TIMEOUT_CONNECT = 0.3
    const.HTTP_FORWARD_TIMEOUT_SOCK_READ = 0.3
    # ``proxy.py`` imports the names directly; rebind those too so the
    # ClientSession we build in ``_on_startup`` reads the patched values.
    proxy.HTTP_FORWARD_TIMEOUT_TOTAL = 0.3
    proxy.HTTP_FORWARD_TIMEOUT_CONNECT = 0.3
    proxy.HTTP_FORWARD_TIMEOUT_SOCK_READ = 0.3

    try:
        proxy_runner, proxy_url = await _start_app(proxy.create_app(_opts(ha_url)))
        try:
            async with aiohttp.ClientSession() as cs:
                async with cs.get(f"{proxy_url}/api/slow") as resp:
                    assert resp.status == 504
                    assert (await resp.text()) == "Gateway Timeout"
        finally:
            await proxy_runner.cleanup()
    finally:
        const.HTTP_FORWARD_TIMEOUT_TOTAL = orig_total
        const.HTTP_FORWARD_TIMEOUT_CONNECT = orig_connect
        const.HTTP_FORWARD_TIMEOUT_SOCK_READ = orig_sock_read
        proxy.HTTP_FORWARD_TIMEOUT_TOTAL = orig_total
        proxy.HTTP_FORWARD_TIMEOUT_CONNECT = orig_connect
        proxy.HTTP_FORWARD_TIMEOUT_SOCK_READ = orig_sock_read
        await ha_runner.cleanup()


async def test_passthrough_all_routes_api_websocket_through_tunnel() -> None:
    """With ``passthrough_all=True`` the proxy must NOT intercept
    ``/api/websocket`` — the request goes through ``_tunnel_ws`` and the
    connection shows up as kind ``passthrough`` in the registry.
    """
    registry = SessionRegistry()
    ha_runner, ha_url = await _start_app(await _fake_ws_echo_app())
    proxy_runner, proxy_url = await _start_app(
        proxy.create_app(_opts(ha_url, registry=registry, passthrough_all=True))
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.ws_connect(proxy_url + "/api/websocket") as ws:
                await ws.send_str("ping")
                msg = await ws.receive(timeout=2.0)
                assert msg.data == "echo:ping"
                snap = registry.snapshot()
                assert len(snap) == 1
                assert snap[0]["kind"] == "passthrough"
                await ws.close()
    finally:
        await proxy_runner.cleanup()
        await ha_runner.cleanup()
