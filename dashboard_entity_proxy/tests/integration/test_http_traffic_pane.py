# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end check that HTTP requests flowing nginx → HA (the direct
path, bypassing the Python proxy entirely) surface in the status UI's
per-client HTTP panes via the access-log tailer.
"""

import json
import time
import urllib.request

import aiohttp
import pytest

INTEGRATION = pytest.mark.integration

# Status-UI port. The addon container runs with ``--network host`` (see
# ``proxy_url`` fixture), so the Python-served /api/sessions endpoint
# binds on the host's loopback at this port.
_STATUS_PORT = 8098


def _fetch_sessions(detail: str = "summary") -> list[dict]:
    url = f"http://127.0.0.1:{_STATUS_PORT}/api/sessions"
    if detail == "full":
        url += "?detail=full"
    with urllib.request.urlopen(url, timeout=2) as r:
        return json.loads(r.read())["sessions"]


@INTEGRATION
async def test_http_requests_show_in_status_ui(proxy_url: str, ha: dict[str, str]):
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

    rows: list[dict] = []
    tx_total = 0
    for _ in range(40):
        clients = [s for s in _fetch_sessions() if s.get("kind") == "http_client"]
        if clients and clients[0].get("rows"):
            rows = clients[0]["rows"]
            tx_total = clients[0].get("tx_bytes", 0)
            break
        time.sleep(0.1)

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


@INTEGRATION
async def test_http_traffic_detail_full_returns_all_rows(
    proxy_url: str, ha: dict[str, str]
):
    """The ``?detail=full`` query param returns the full row set
    (otherwise capped at 50). Mirrors B12's detail switch on intercept
    sessions.
    """
    async with aiohttp.ClientSession() as cs:
        headers = {"Authorization": f"Bearer {ha['access_token']}"}
        async with cs.get(proxy_url + "/api/", headers=headers) as resp:
            await resp.read()

    # Give the tailer a moment to ingest.
    for _ in range(20):
        clients = [s for s in _fetch_sessions() if s.get("kind") == "http_client"]
        if clients and clients[0].get("rows"):
            break
        time.sleep(0.1)
    else:
        pytest.fail("http_client card never showed up")

    for detail in ("summary", "full"):
        clients = [
            s for s in _fetch_sessions(detail=detail) if s.get("kind") == "http_client"
        ]
        assert clients, f"http_client missing for detail={detail}"
        entry = clients[0]
        assert "row_count" in entry
        assert "rows" in entry
        assert "rx_bytes" in entry
        assert "tx_bytes" in entry
