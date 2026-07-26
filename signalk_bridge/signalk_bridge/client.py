# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Minimal Signal K REST client.

Polls the full ``vessels/self`` tree rather than subscribing to the delta
websocket. Polling is a deliberate simplification: marine instrument data is
published to Home Assistant at a human timescale (seconds), a REST snapshot is
inherently consistent, and there is no reconnect/backfill state machine to get
wrong. If sub-second latency is ever needed, the websocket stream is the upgrade
path.
"""

import json
import urllib.error
import urllib.request
from typing import Any

SELF_PATH = "/signalk/v1/api/vessels/self"


class SignalKError(Exception):
    """Signal K could not be reached, or returned something unusable."""


class SignalKAuthError(SignalKError):
    """Signal K rejected the request for lack of (valid) credentials (401/403)."""


def _request(
    url: str,
    token: str | None = None,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise SignalKAuthError(
                f"{url} returned HTTP {exc.code} (unauthorized)"
            ) from exc
        raise SignalKError(f"{url} returned HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise SignalKError(f"{url} unreachable: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SignalKError(f"{url} returned invalid JSON: {exc}") from exc


def _get(url: str, token: str | None, timeout: float) -> Any:
    return _request(url, token=token, timeout=timeout)


def get_self(
    base_url: str, token: str | None = None, timeout: float = 15.0
) -> dict[str, Any]:
    """Fetch the vessel's own Signal K tree."""
    payload = _get(base_url.rstrip("/") + SELF_PATH, token, timeout)
    if not isinstance(payload, dict):
        raise SignalKError(
            f"{SELF_PATH} returned {type(payload).__name__}, expected object"
        )
    return payload


SOURCES_PATH = "/signalk/v1/api/sources"


def get_sources(
    base_url: str, token: str | None = None, timeout: float = 15.0
) -> dict[str, Any]:
    """Fetch the source (device) inventory -- one entry per bus device."""
    payload = _get(base_url.rstrip("/") + SOURCES_PATH, token, timeout)
    return payload if isinstance(payload, dict) else {}


def get_server_info(base_url: str, timeout: float = 10.0) -> dict[str, Any]:
    """Fetch ``/signalk`` -- unauthenticated, so it doubles as a reachability probe."""
    payload = _get(base_url.rstrip("/") + "/signalk", None, timeout)
    return payload if isinstance(payload, dict) else {}
