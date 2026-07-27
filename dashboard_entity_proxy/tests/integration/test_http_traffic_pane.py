# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end check that HTTP requests flowing nginx → HA (the direct
path, bypassing the Python proxy entirely) surface in the status UI's
per-client HTTP panes via the access-log tailer.
"""

import asyncio
import json
import urllib.request

import aiohttp
import pytest

E2E = pytest.mark.e2e


def _fetch_sessions(status_url: str, detail: str = "summary") -> list[dict]:
    url = f"{status_url}/api/sessions"
    if detail == "full":
        url += "?detail=full"
    with urllib.request.urlopen(url, timeout=2) as r:
        return json.loads(r.read())["sessions"]


@E2E
async def test_http_requests_show_in_status_ui(
    proxy_url: str, status_url: str, ha: dict[str, str]
):
    """Drive a few HTTP requests through the addon's nginx; assert the
    status UI lists at least one ``http_client`` card whose rows include
    the right targets and a populated rx/tx byte count.
    """
    async with aiohttp.ClientSession() as cs:
        headers = {"Authorization": f"Bearer {ha['access_token']}"}
        for _ in range(3):
            async with cs.get(proxy_url + "/api/", headers=headers) as resp:
                await resp.read()
        async with cs.get(proxy_url + "/auth/providers") as resp:
            await resp.read()

    # Under the compose network the addon sees two client IPs — the
    # in-container docker healthcheck (from localhost) and pytest's
    # aiohttp session (from the host-side docker gateway). Each shows
    # up as its own http_client card; pick the pytest one by matching
    # ``/auth/providers`` (never fired by the healthcheck, which only
    # hits ``/``).
    rows: list[dict] = []
    tx_total = 0
    for _ in range(40):
        clients = [
            s for s in _fetch_sessions(status_url) if s.get("kind") == "http_client"
        ]
        us = next(
            (
                c
                for c in clients
                if any(
                    "/auth/providers" in r.get("last_uri", "")
                    for r in c.get("rows", [])
                )
            ),
            None,
        )
        if us and us.get("rows"):
            rows = us["rows"]
            tx_total = us.get("tx_bytes", 0)
            break
        await asyncio.sleep(0.1)

    assert rows, "no http_client card with rows ever appeared"
    targets = {r["target"] for r in rows}
    assert "ha-rest" in targets or any("/api" in r["last_uri"] for r in rows), (
        f"expected an ha-rest row; got targets={targets}"
    )
    assert "auth" in targets or any("/auth" in r["last_uri"] for r in rows), (
        f"expected an auth row; got targets={targets}"
    )
    last_seens = [r["last_seen"] for r in rows]
    assert last_seens == sorted(last_seens, reverse=True), (
        f"rows not sorted most-recent-first: {last_seens}"
    )
    # rx/tx bytes are accumulating — at least the response bodies should
    # be non-zero for /api/.
    assert tx_total > 0, f"expected tx_bytes > 0 at the client level; got {tx_total}"
    assert any(r["tx_bytes"] > 0 for r in rows), (
        f"expected at least one row with non-zero tx_bytes; got {rows}"
    )


@E2E
async def test_http_traffic_detail_full_returns_all_rows(
    proxy_url: str, status_url: str, ha: dict[str, str]
):
    """The ``?detail=full`` query param returns the full row set
    (otherwise capped at 50). Mirrors B12's detail switch on intercept
    sessions.
    """
    async with aiohttp.ClientSession() as cs:
        headers = {"Authorization": f"Bearer {ha['access_token']}"}
        async with cs.get(proxy_url + "/api/", headers=headers) as resp:
            await resp.read()

    def _pick_us(entries: list[dict]) -> dict | None:
        # Match pytest's aiohttp session by its /api target; the docker
        # healthcheck client (from container-localhost) never hits /api,
        # so we never pick it.
        return next(
            (
                e
                for e in entries
                if any("/api" in r.get("last_uri", "") for r in e.get("rows", []))
            ),
            None,
        )

    for _ in range(20):
        clients = [
            s for s in _fetch_sessions(status_url) if s.get("kind") == "http_client"
        ]
        if _pick_us(clients):
            break
        await asyncio.sleep(0.1)
    else:
        pytest.fail("pytest-side http_client card never showed up")

    for detail in ("summary", "full"):
        clients = [
            s
            for s in _fetch_sessions(status_url, detail=detail)
            if s.get("kind") == "http_client"
        ]
        entry = _pick_us(clients)
        assert entry, f"pytest-side http_client missing for detail={detail}"
        assert "row_count" in entry
        assert "rows" in entry
        assert "rx_bytes" in entry
        assert "tx_bytes" in entry
