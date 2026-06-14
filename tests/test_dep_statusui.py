# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for ``statusui.py``: the Ingress status page route, the JSON
snapshot endpoint, and the ``show_client_paths`` redaction toggle.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiohttp

from dashboard_entity_proxy import statusui
from dashboard_entity_proxy.proxy import TunnelConnection
from dashboard_entity_proxy.session import SessionRegistry


from _aiohttp_helpers import _start_app


class _FakeIntercept:
    """Stand-in for ``Session`` — supplies the ``status()`` shape the UI
    consumes, including a ``current_path`` field so redaction can be tested.

    Mirrors the real ``Session.status()`` signature: ``detail='summary'``
    (default) returns ``scope_count`` and a ``scope_sample`` only;
    ``detail='full'`` adds the legacy ``scope_entities`` list.
    """

    def __init__(self, remote: str, current_path: str) -> None:
        self._remote = remote
        self._path = current_path

    def status(self, *, detail: str = "summary") -> dict[str, Any]:
        ids = ["light.kitchen"]
        out: dict[str, Any] = {
            "kind": "intercept",
            "remote_addr": self._remote,
            "connected_at": datetime.now(timezone.utc).isoformat(),
            "current_view": "(default dashboard)",
            "current_path": self._path,
            "scope_ready": True,
            "scope_all": False,
            "scope_count": len(ids),
            "scope_sample": list(ids),
            "mirror_entities": 1,
            "queue_depth": 0,
            "messages_sent": 0,
            "throttle_seconds": 0,
        }
        if detail == "full":
            out["scope_entities"] = list(ids)
        return out


# ---- HTML page route ------------------------------------------------------


async def test_get_root_returns_html_page() -> None:
    registry = SessionRegistry()
    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/") as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"].startswith("text/html")
                body = await resp.text()
                # Sanity-check that we're serving the bundled page, not a
                # placeholder — the index.html template contains the panel
                # title and the polling JS that hits /api/sessions.
                assert "Entity Proxy" in body or "Dashboard" in body
                assert "/api/sessions" in body
    finally:
        await runner.cleanup()


# ---- /api/sessions JSON endpoint -------------------------------------------


async def test_sessions_returns_live_and_recent_snapshots() -> None:
    registry = SessionRegistry()
    sess = _FakeIntercept("192.0.2.10", "/lovelace-foo/home")
    tunnel = TunnelConnection("192.0.2.11", "/custom/ws", passthrough=False)
    registry.add(sess)
    registry.add(tunnel)

    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions") as resp:
                assert resp.status == 200
                payload = await resp.json()
                assert "now" in payload
                snap = payload["sessions"]
                assert {s["remote_addr"] for s in snap} == {"192.0.2.10", "192.0.2.11"}
                by_kind = {s["kind"]: s for s in snap}
                assert by_kind["intercept"]["current_path"] == "/lovelace-foo/home"
                assert by_kind["tunnel"]["target_path"] == "/custom/ws"
    finally:
        await runner.cleanup()


async def test_sessions_redacts_paths_when_show_client_paths_is_false() -> None:
    registry = SessionRegistry()
    registry.add(_FakeIntercept("192.0.2.10", "/lovelace-foo/home"))
    registry.add(TunnelConnection("192.0.2.11", "/custom/ws", passthrough=False))

    runner, url = await _start_app(
        statusui.create_app(registry, show_client_paths=False)
    )
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions") as resp:
                payload = await resp.json()
                for entry in payload["sessions"]:
                    assert "current_path" not in entry
                    assert "target_path" not in entry
                # Non-path fields still present so the rest of the UI works.
                kinds = {s["kind"] for s in payload["sessions"]}
                assert kinds == {"intercept", "tunnel"}
    finally:
        await runner.cleanup()


async def test_sessions_keeps_paths_by_default() -> None:
    """The redaction is opt-in; with default ``show_client_paths=True`` the
    path fields must still be present in the JSON snapshot.
    """
    registry = SessionRegistry()
    registry.add(_FakeIntercept("192.0.2.10", "/lovelace-foo/home"))
    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions") as resp:
                payload = await resp.json()
                assert payload["sessions"][0]["current_path"] == "/lovelace-foo/home"
    finally:
        await runner.cleanup()


async def test_sessions_empty_when_no_connections() -> None:
    registry = SessionRegistry()
    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions") as resp:
                payload = await resp.json()
                assert payload["sessions"] == []
    finally:
        await runner.cleanup()


async def test_sessions_default_omits_full_scope_entities() -> None:
    """The default poll (no ``?detail=full``) returns the summary shape:
    ``scope_count`` and ``scope_sample`` present, ``scope_entities``
    absent.
    """
    registry = SessionRegistry()
    registry.add(_FakeIntercept("192.0.2.10", "/lovelace/home"))
    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions") as resp:
                payload = await resp.json()
                entry = payload["sessions"][0]
                assert "scope_entities" not in entry
                assert entry["scope_count"] == 1
                assert entry["scope_sample"] == ["light.kitchen"]
    finally:
        await runner.cleanup()


async def test_sessions_detail_full_includes_scope_entities() -> None:
    """``?detail=full`` asks for the legacy shape — full ``scope_entities``
    list is included alongside the summary fields.
    """
    registry = SessionRegistry()
    registry.add(_FakeIntercept("192.0.2.10", "/lovelace/home"))
    runner, url = await _start_app(statusui.create_app(registry))
    try:
        async with aiohttp.ClientSession() as cs:
            async with cs.get(url + "/api/sessions?detail=full") as resp:
                payload = await resp.json()
                entry = payload["sessions"][0]
                assert entry["scope_entities"] == ["light.kitchen"]
                assert entry["scope_count"] == 1
    finally:
        await runner.cleanup()
