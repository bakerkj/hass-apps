# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the Signal K REST client: endpoints, auth header, and the
401/403 -> SignalKAuthError mapping, against a real (in-process) HTTP server."""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, cast

import pytest

from signalk_bridge import client


@pytest.fixture
def sk_server() -> Iterator[
    tuple[str, dict[str, tuple[int, Any]], list[tuple[str, str | None]]]
]:
    """An in-process Signal K stub.

    Yields ``(base_url, routes, captured)``: mutate ``routes`` to map a path
    prefix to ``(status, body)``; ``captured`` records ``(path, Authorization)``
    for each request so auth-header behaviour can be asserted.
    """
    routes: dict[str, tuple[int, Any]] = {}
    captured: list[tuple[str, str | None]] = []

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            captured.append((self.path, self.headers.get("Authorization")))
            status, body = 404, None
            for prefix, (s, b) in routes.items():
                if self.path.startswith(prefix):
                    status, body = s, b
                    break
            data = json.dumps(body).encode() if body is not None else b""
            self.send_response(status)
            if data:
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def log_message(self, *_a: Any) -> None:
            pass

    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    host, port = cast("tuple[str, int]", srv.server_address)
    try:
        yield f"http://{host}:{port}", routes, captured
    finally:
        srv.shutdown()
        thread.join(timeout=5)


def test_get_self_returns_tree(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/vessels/self"] = (
        200,
        {"navigation": {"x": {"value": 1.0}}},
    )
    assert client.get_self(base, "tok")["navigation"]["x"]["value"] == 1.0


def test_get_self_401_raises_auth(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/vessels/self"] = (401, {"error": "no"})
    with pytest.raises(client.SignalKAuthError):
        client.get_self(base, None)


def test_get_self_403_raises_auth(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/vessels/self"] = (403, None)
    with pytest.raises(client.SignalKAuthError):
        client.get_self(base, None)


def test_get_self_500_is_generic_not_auth(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/vessels/self"] = (500, None)
    with pytest.raises(client.SignalKError) as ei:
        client.get_self(base, None)
    assert not isinstance(ei.value, client.SignalKAuthError)


def test_get_self_non_object_raises(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/vessels/self"] = (200, [1, 2, 3])
    with pytest.raises(client.SignalKError):
        client.get_self(base, None)


def test_get_self_unreachable_raises() -> None:
    with pytest.raises(client.SignalKError):
        client.get_self("http://127.0.0.1:1", None, timeout=0.5)


def test_get_sources_ok_then_nonobject(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk/v1/api/sources"] = (200, {"n2k-can0": {}})
    assert client.get_sources(base, "t") == {"n2k-can0": {}}
    routes["/signalk/v1/api/sources"] = (200, [1])  # non-dict degrades to {}
    assert client.get_sources(base, "t") == {}


def test_get_server_info(sk_server: Any) -> None:
    base, routes, _ = sk_server
    routes["/signalk"] = (200, {"server": {"version": "9.9"}})
    assert client.get_server_info(base)["server"]["version"] == "9.9"


def test_bearer_token_sent(sk_server: Any) -> None:
    base, routes, captured = sk_server
    routes["/signalk/v1/api/vessels/self"] = (200, {})
    client.get_self(base, "secret-token")
    auths = [a for p, a in captured if p.startswith("/signalk/v1/api/vessels/self")]
    assert auths == ["Bearer secret-token"]


def test_server_info_sends_no_auth(sk_server: Any) -> None:
    base, routes, captured = sk_server
    routes["/signalk"] = (200, {})
    client.get_server_info(base)
    auths = [a for p, a in captured if p == "/signalk"]
    assert auths == [None]
