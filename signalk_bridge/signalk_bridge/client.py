# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Minimal async Signal K REST client.

Polls the full ``vessels/self`` tree rather than subscribing to the delta
websocket. Polling is a deliberate simplification: a REST snapshot is
inherently consistent, and there is no reconnect/backfill state machine to
get wrong. The websocket stream is the upgrade path when sub-second latency
is needed.

Async because the bridge runs on asyncio: sync urllib in the poll loop would
block the loop and stall aiomqtt heartbeats, MQTT deliveries, and any
future concurrent subscription (e.g., the vessels.* delta stream).
"""

from typing import Any

import aiohttp

SELF_PATH = "/signalk/v1/api/vessels/self"
SOURCES_PATH = "/signalk/v1/api/sources"


class SignalKError(Exception):
    """Signal K could not be reached, or returned something unusable."""


class SignalKAuthError(SignalKError):
    """Signal K rejected the request for lack of (valid) credentials (401/403)."""


async def _request(
    session: aiohttp.ClientSession,
    url: str,
    token: str | None = None,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> Any:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.request(
            method, url, json=body, headers=headers, timeout=to
        ) as resp:
            if resp.status in (401, 403):
                raise SignalKAuthError(
                    f"{url} returned HTTP {resp.status} (unauthorized)"
                )
            if resp.status >= 400:
                raise SignalKError(f"{url} returned HTTP {resp.status}")
            raw = await resp.read()
            if not raw:
                return None
            try:
                return await resp.json(content_type=None)
            except (aiohttp.ContentTypeError, ValueError) as exc:
                raise SignalKError(f"{url} returned invalid JSON: {exc}") from exc
    except aiohttp.ClientError as exc:
        raise SignalKError(f"{url} unreachable: {exc}") from exc


async def get_self(
    session: aiohttp.ClientSession,
    base_url: str,
    token: str | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Fetch the vessel's own Signal K tree."""
    payload = await _request(
        session, base_url.rstrip("/") + SELF_PATH, token=token, timeout=timeout
    )
    if not isinstance(payload, dict):
        raise SignalKError(
            f"{SELF_PATH} returned {type(payload).__name__}, expected object"
        )
    return payload


async def get_sources(
    session: aiohttp.ClientSession,
    base_url: str,
    token: str | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Fetch the source (device) inventory -- one entry per bus device."""
    payload = await _request(
        session, base_url.rstrip("/") + SOURCES_PATH, token=token, timeout=timeout
    )
    return payload if isinstance(payload, dict) else {}


async def get_server_info(
    session: aiohttp.ClientSession, base_url: str, timeout: float = 10.0
) -> dict[str, Any]:
    """Fetch ``/signalk`` -- unauthenticated, so it doubles as a reachability probe."""
    payload = await _request(
        session, base_url.rstrip("/") + "/signalk", token=None, timeout=timeout
    )
    return payload if isinstance(payload, dict) else {}
