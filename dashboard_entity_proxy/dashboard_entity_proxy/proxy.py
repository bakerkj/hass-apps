# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""aiohttp reverse proxy: transparently forwards HTTP, tunnels WebSockets for
non-API paths, and intercepts /api/websocket into a Session.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from aiohttp import WSMsgType, web
from multidict import CIMultiDict

from .const import (
    HA_WS_PATH,
    HEARTBEAT,
    HTTP_FORWARD_TIMEOUT_CONNECT,
    HTTP_FORWARD_TIMEOUT_SOCK_READ,
    HTTP_FORWARD_TIMEOUT_TOTAL,
    WS_MAX_MSG_SIZE,
)
from .session import Session


def _client_addr(request: web.Request) -> str:
    """Real client IP for status-UI display, honoring nginx's
    ``X-Real-IP`` / ``X-Forwarded-For`` headers. nginx sets these on the
    WS path so the Python proxy can attribute connections to the actual
    browser rather than to nginx's loopback peer (127.0.0.1). For the
    rare case where neither header is present (e.g. a direct connection
    to the Python proxy port that bypasses nginx), fall back to the WS
    peer address aiohttp reports.
    """
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        # X-Forwarded-For is comma-separated; the leftmost entry is the
        # original client. Trust nginx here: only nginx is allowed to
        # write this header (the Python proxy listens on 127.0.0.1 only,
        # nginx is the only thing that reaches it).
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    return request.remote or ""


class TunnelConnection:
    """A raw WebSocket pass-through, tracked in the status UI so users can see
    every connection the proxy is handling — not just intercepted ones.
    """

    def __init__(self, remote_addr: str, target_path: str, passthrough: bool):
        """``passthrough`` is True when the tunnelled WebSocket is on the
        HA WS path but the addon is configured to forward it unchanged
        (passthrough_all mode); otherwise it's some non-API WS path the
        proxy is tunnelling.
        """
        self._remote = remote_addr
        self._target = target_path
        self._passthrough = passthrough
        self._connected_at = datetime.now(timezone.utc)
        self.msgs_client_to_ha = 0
        self.msgs_ha_to_client = 0
        # Byte counters parallel to msgs_*. rx is client→HA payload
        # bytes; tx is HA→client payload bytes. Tracks WS payload sizes
        # only — frame overhead isn't counted.
        self.bytes_client_to_ha = 0
        self.bytes_ha_to_client = 0

    def status(self) -> dict[str, Any]:
        """Snapshot dict consumed by ``SessionRegistry`` / the Ingress
        status UI. Distinct ``kind`` value from intercepted ``Session``s so
        the UI can render the two differently.
        """
        return {
            "kind": "passthrough" if self._passthrough else "tunnel",
            "remote_addr": self._remote,
            "connected_at": self._connected_at.isoformat(),
            "target_path": self._target,
            "messages_client_to_ha": self.msgs_client_to_ha,
            "messages_ha_to_client": self.msgs_ha_to_client,
            "rx_bytes": self.bytes_client_to_ha,
            "tx_bytes": self.bytes_ha_to_client,
        }


# Headers we never forward between client and upstream. Most are hop-by-hop
# per RFC 7230 §6.1 — they describe the transport-level connection between
# two adjacent peers and confuse the upstream's connection state with the
# client's (e.g. forwarding ``Connection: close`` from the client would
# make us hang up on HA mid-response). ``host`` is excluded because aiohttp
# rewrites it for us against the upstream URL, and forwarding the client's
# value would point HA at the wrong vhost. ``content-length`` is excluded
# because we re-stream the body and let aiohttp recompute the framing
# (otherwise a stale length from the inbound request would mis-frame the
# outbound one).
_HEADERS_NOT_FORWARDED = frozenset(
    {
        # Hop-by-hop per RFC 7230 §6.1.
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",  # codespell:ignore te
        "trailer",
        "transfer-encoding",
        "upgrade",
        # Rewritten by aiohttp against the upstream URL.
        "host",
        # Rewritten because we re-stream the body.
        "content-length",
    }
)


# Shared with ``session.Options`` — see ``options.py``. Re-exported
# here so existing ``from dashboard_entity_proxy.proxy import Options``
# callers keep working without churn.
from .options import Options  # noqa: E402  (post-imports re-export)


CLIENT_KEY: web.AppKey[aiohttp.ClientSession] = web.AppKey(
    "client", aiohttp.ClientSession
)


def create_app(opts: Options) -> web.Application:
    """Build the aiohttp app that fronts Home Assistant. One catch-all route
    inspects each request and dispatches to one of three handlers:

      * ``_intercept`` — WebSocket on ``/api/websocket`` (the entity-filter
        path; only when ``passthrough_all`` is off).
      * ``_tunnel_ws`` — any other WebSocket (or ``/api/websocket`` in
        passthrough mode); forwarded unchanged.
      * ``_reverse_http`` — every plain HTTP request.

    A single ``aiohttp.ClientSession`` is created on app startup and shared
    across requests for connection pooling.
    """
    log = opts.logger or logging.getLogger("dashboard_entity_proxy.proxy")
    ws_base = _http_to_ws(opts.target_url)
    http_base = opts.target_url.rstrip("/")

    app = web.Application()

    async def _on_startup(app: web.Application) -> None:
        """aiohttp startup hook: create the shared upstream client session.

        The explicit timeout caps how long any single forwarded HTTP
        request can occupy the session if HA stalls — aiohttp's default
        is a 5-minute total, far too long for a reverse proxy. Hitting
        the cap surfaces as ``asyncio.TimeoutError`` in ``_reverse_http``
        and is translated to a clean 504.
        """
        timeout = aiohttp.ClientTimeout(
            total=HTTP_FORWARD_TIMEOUT_TOTAL,
            connect=HTTP_FORWARD_TIMEOUT_CONNECT,
            sock_read=HTTP_FORWARD_TIMEOUT_SOCK_READ,
        )
        app[CLIENT_KEY] = aiohttp.ClientSession(timeout=timeout)

    async def _on_cleanup(app: web.Application) -> None:
        """aiohttp cleanup hook: close the shared upstream client session."""
        await app[CLIENT_KEY].close()

    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)

    async def handler(request: web.Request) -> web.StreamResponse:
        """Catch-all dispatcher — picks one of the three forwarding modes
        per request, based on Upgrade header and path.
        """
        client = request.app[CLIENT_KEY]
        is_ws = request.headers.get("Upgrade", "").lower() == "websocket"

        if is_ws and request.path == HA_WS_PATH and not opts.passthrough_all:
            return await _intercept(request, client, ws_base, opts, log)
        if is_ws:
            return await _tunnel_ws(request, client, ws_base, opts, log)
        return await _reverse_http(request, client, http_base, opts.transparent, log)

    app.router.add_route("*", "/{tail:.*}", handler)
    return app


async def _intercept(
    request: web.Request,
    client: aiohttp.ClientSession,
    ws_base: str,
    opts: Options,
    log: logging.Logger,
) -> web.WebSocketResponse:
    """Handle a client WebSocket bound for ``/api/websocket``: accept the
    client connection, dial HA, hand the paired sockets to a ``Session``
    that does the auth-handshake relay + entity-filter logic. Returns when
    the session exits (either side disconnected).
    """
    ws_client = web.WebSocketResponse(heartbeat=HEARTBEAT, max_msg_size=WS_MAX_MSG_SIZE)
    await ws_client.prepare(request)
    try:
        ws_ha = await client.ws_connect(
            ws_base + request.path_qs,
            headers=_ws_dial_headers(request, opts.transparent),
            heartbeat=HEARTBEAT,
            max_msg_size=WS_MAX_MSG_SIZE,
        )
    except (aiohttp.ClientError, OSError) as exc:
        log.error("dial HA websocket failed: %s", exc)
        await ws_client.close()
        return ws_client

    sess = Session(
        ws_client,
        ws_ha,
        _client_addr(request),
        opts,
        log,
    )
    await sess.run()
    return ws_client


async def _tunnel_ws(
    request: web.Request,
    client: aiohttp.ClientSession,
    ws_base: str,
    opts: Options,
    log: logging.Logger,
) -> web.WebSocketResponse:
    """Forward a non-API WebSocket (or ``/api/websocket`` in passthrough
    mode) byte-for-byte between client and HA. Runs two pump tasks until
    either side closes, then tears both connections down. Registers a
    ``TunnelConnection`` with the SessionRegistry so the status UI lists
    it alongside intercepted sessions.
    """
    ws_client = web.WebSocketResponse(heartbeat=HEARTBEAT, max_msg_size=WS_MAX_MSG_SIZE)
    await ws_client.prepare(request)
    try:
        ws_ha = await client.ws_connect(
            ws_base + request.path_qs,
            headers=_ws_dial_headers(request, opts.transparent),
            max_msg_size=WS_MAX_MSG_SIZE,
        )
    except (aiohttp.ClientError, OSError) as exc:
        log.error("dial HA websocket (tunnel) failed: %s", exc)
        await ws_client.close()
        return ws_client

    conn = TunnelConnection(
        remote_addr=_client_addr(request),
        target_path=request.path_qs,
        passthrough=(request.path == HA_WS_PATH),
    )
    if opts.registry is not None:
        opts.registry.add(conn)

    def _account_c_to_h(n: int) -> None:
        conn.msgs_client_to_ha += 1
        conn.bytes_client_to_ha += n

    def _account_h_to_c(n: int) -> None:
        conn.msgs_ha_to_client += 1
        conn.bytes_ha_to_client += n

    c_to_h = asyncio.create_task(_pump_tunnel(ws_client, ws_ha, _account_c_to_h))
    h_to_c = asyncio.create_task(_pump_tunnel(ws_ha, ws_client, _account_h_to_c))
    tasks = [c_to_h, h_to_c]
    try:
        _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        # The surviving pump is still blocked in ``receive()``. Closing
        # one side of the tunnel below will wake it with a CLOSED/CLOSE
        # frame; cancel and await it so it either records that frame's
        # code or unwinds cleanly, before we read the captured values.
        for t in pending:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
    finally:
        if opts.registry is not None:
            opts.registry.remove(conn)
        # Each pump's task result is its captured close meta (or None for
        # a clean CLOSED without an explicit CLOSE frame). Cancelled
        # tasks report no close meta. Forward each side's observed close
        # code/reason to its peer so both halves of the tunnel see the
        # same disconnect cause.
        close_from_client = _close_meta_from_task(c_to_h)
        close_from_ha = _close_meta_from_task(h_to_c)
        ha_code, ha_reason = close_from_ha if close_from_ha is not None else (1000, "")
        client_code, client_reason = (
            close_from_client if close_from_client is not None else (1000, "")
        )
        await ws_client.close(code=ha_code, message=ha_reason.encode("utf-8"))
        await ws_ha.close(code=client_code, message=client_reason.encode("utf-8"))
    return ws_client


async def _pump_tunnel(
    ws_in: Any,
    ws_out: Any,
    on_frame: Callable[[int], None],
) -> tuple[int, str] | None:
    """Forward TEXT/BINARY frames verbatim from ``ws_in`` to ``ws_out``
    until a close frame arrives. Calls ``on_frame(len(data))`` for each
    forwarded frame so the caller can account messages + bytes.

    Returns the captured ``(code, reason)`` on CLOSE/CLOSING/ERROR;
    ``None`` on CLOSED (a clean disconnect without an explicit CLOSE
    frame) or any other terminal type. The caller propagates the
    captured meta to the *other* side of the tunnel so both halves see
    the same disconnect cause.

    Uses ``receive()`` directly rather than ``async for`` because the
    iterator protocol filters out CLOSE/CLOSING/CLOSED messages before
    the body sees them, which would discard the close code/reason.
    """
    while True:
        msg = await ws_in.receive()
        if msg.type == WSMsgType.TEXT:
            await ws_out.send_str(msg.data)
            on_frame(len(msg.data))
        elif msg.type == WSMsgType.BINARY:
            await ws_out.send_bytes(msg.data)
            on_frame(len(msg.data))
        elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
            # aiohttp puts the peer's close code in ``msg.data`` and the
            # reason text in ``msg.extra``; on ERROR ``msg.data`` is the
            # exception, so we coerce defensively.
            code = msg.data if isinstance(msg.data, int) else 1000
            reason = msg.extra if isinstance(msg.extra, str) else ""
            return code, reason
        else:  # CLOSED or any other terminal type
            return None


def _close_meta_from_task(task: "asyncio.Task[Any]") -> tuple[int, str] | None:
    """Read a pump task's captured close meta. Returns ``None`` if the
    task was cancelled, hasn't completed, or finished cleanly without a
    close frame.
    """
    if not task.done() or task.cancelled():
        return None
    try:
        return task.result()
    except (asyncio.CancelledError, Exception):  # noqa: BLE001
        return None


async def _reverse_http(
    request: web.Request,
    client: aiohttp.ClientSession,
    http_base: str,
    transparent: bool,
    log: logging.Logger,
) -> web.StreamResponse:
    """Forward a plain HTTP request to the upstream HA and stream the
    response back unchanged. Strips hop-by-hop headers in both
    directions and returns 502 on any upstream failure.

    ``auto_decompress=False`` keeps the upstream body opaque to the
    proxy: HA's compressed responses (brotli, gzip) pass through as-is
    with the original ``Content-Encoding`` preserved, and the browser
    decompresses natively. This avoids paying for a Python-level
    decompress on every asset and removes the need for the optional
    ``Brotli`` package in the runtime.
    """
    url = http_base + request.raw_path
    body = await request.read()
    headers = _filtered_request_headers(request, transparent)
    try:
        async with client.request(
            request.method,
            url,
            headers=headers,
            data=body,
            allow_redirects=False,
            auto_decompress=False,
        ) as upstream:
            response = web.StreamResponse(
                status=upstream.status,
                headers=_filtered_response_headers(upstream, request),
            )
            await response.prepare(request)
            async for chunk in upstream.content.iter_chunked(65536):
                await response.write(chunk)
            await response.write_eof()
            return response
    except asyncio.TimeoutError as exc:
        # The shared ClientSession is configured with bounded total /
        # connect / sock_read timeouts (see ``HTTP_FORWARD_TIMEOUT_*``).
        # An upstream stall surfaces here as ``asyncio.TimeoutError`` and
        # we translate it into a clean 504 — distinct from the generic
        # 502 we return for transport / protocol failures so operators
        # can tell the two failure modes apart in nginx access logs.
        log.warning(
            "reverse proxy timeout for %s %s: %s",
            request.method,
            request.path,
            exc,
        )
        return web.Response(status=504, text="Gateway Timeout")
    except Exception as exc:  # noqa: BLE001
        log.error(
            "reverse proxy error for %s %s: %s", request.method, request.path, exc
        )
        return web.Response(status=502, text="Bad Gateway")


# Headers aiohttp's ``ws_connect`` manages itself on the upstream socket;
# forwarding the client's values corrupts the new dial. ``Upgrade`` and
# ``Connection`` are part of the WS handshake aiohttp performs; the
# ``Sec-WebSocket-*`` family (Key, Version, Extensions, Protocol, Accept)
# is freshly negotiated between proxy↔HA, so the client's values must not
# leak through.
_WS_HOP_BY_HOP = frozenset(
    {
        "upgrade",
        "connection",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-extensions",
        "sec-websocket-protocol",
        "sec-websocket-accept",
    }
)


def _ws_dial_headers(request: web.Request, transparent: bool) -> CIMultiDict[str]:
    """Build the upstream header set for a WebSocket dial. Mirrors the
    HTTP-path policy in ``_filtered_request_headers`` (drop hop-by-hop,
    handle ``X-Forwarded-*`` per ``transparent``) and additionally strips
    the WS-handshake headers aiohttp regenerates on ``ws_connect``.
    Everything else — Cookie, Authorization, User-Agent, custom headers
    — is forwarded so middleware/forks that depend on them keep working.
    """
    out = _filtered_request_headers(request, transparent)
    for name in {n for n in out if n.lower() in _WS_HOP_BY_HOP}:
        while name in out:
            del out[name]
    return out


def _filtered_request_headers(
    request: web.Request, transparent: bool
) -> CIMultiDict[str]:
    """Build the upstream request header set: drop hop-by-hop headers, and
    in ``transparent`` mode also drop ``X-Forwarded-*`` / ``Forwarded`` so
    HA treats the proxy as a direct client (no ``trusted_proxies`` config
    needed). Otherwise injects ``X-Forwarded-For`` with the real remote.

    Returns a ``CIMultiDict`` so multi-valued headers (e.g. multiple
    ``Cache-Control`` directives) survive the filter; a plain ``dict``
    would collapse duplicates to the last value.
    """
    out: CIMultiDict[str] = CIMultiDict()
    for name, value in request.headers.items():
        lower = name.lower()
        if lower in _HEADERS_NOT_FORWARDED:
            continue
        if transparent and (lower.startswith("x-forwarded-") or lower == "forwarded"):
            continue
        out.add(name, value)
    if not transparent and request.remote:
        out["X-Forwarded-For"] = request.remote
    return out


def _filtered_response_headers(
    upstream: aiohttp.ClientResponse,
    request: web.Request,
) -> CIMultiDict[str]:
    """Strip hop-by-hop headers from the upstream response.

    ``Content-Length`` is in ``_HEADERS_NOT_FORWARDED`` and therefore
    dropped, so aiohttp's ``StreamResponse`` re-emits the body with
    chunked transfer encoding even when the upstream sent a fixed-length
    body. ``Content-Encoding`` (brotli / gzip) is preserved unchanged
    because the upstream request used ``auto_decompress=False`` — the
    body bytes pass through opaquely and the browser decompresses
    natively.

    Additionally, when the downstream leg is plain HTTP (e.g. the addon
    is reached via Supervisor Ingress, or the addon is fronted by a
    non-TLS listener) but the upstream HA is HTTPS, we strip
    ``Strict-Transport-Security`` (and the deprecated
    ``Public-Key-Pins``) before they reach the browser. Leaking HSTS
    onto a plain-HTTP origin pins that host in Chrome's HSTS cache and
    makes it refuse to load over HTTP again — a surprising,
    near-permanent failure for users who later reach the proxy via a
    different (non-TLS) front-end. ``request.scheme`` reflects the
    scheme of the inbound leg as aiohttp resolved it (honouring
    ``X-Forwarded-Proto`` when a trusted upstream proxy is configured),
    which is the only signal we have for what the browser sees.

    Returns a ``CIMultiDict`` so multi-valued headers (notably
    ``Set-Cookie`` on HA's login flow, plus ``Vary`` / ``Link``) survive
    the filter; a plain ``dict`` would collapse duplicates to the last
    value and break auth.
    """
    downstream_is_plain_http = request.scheme == "http"
    out: CIMultiDict[str] = CIMultiDict()
    for name, value in upstream.headers.items():
        lower = name.lower()
        if lower in _HEADERS_NOT_FORWARDED:
            continue
        if downstream_is_plain_http and lower in (
            "strict-transport-security",
            "public-key-pins",
        ):
            continue
        out.add(name, value)
    return out


def _http_to_ws(target: str) -> str:
    """Convert ``http(s)://host:port/path`` to ``ws(s)://host:port/path``
    by swapping the scheme. Used once to derive the WebSocket base URL
    from the HA HTTP URL.
    """
    parts = urlsplit(target)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, parts.path.rstrip("/"), "", ""))
