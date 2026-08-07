# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Signal K device access-request flow.

Signal K issues device tokens by request-and-approve: the client POSTs an access
request, an admin approves it in the Signal K UI, and the client polls until it
receives a token. The clientId is persisted so the same request is recognised
across restarts, and the granted token is persisted so approval is a one-time step.

The HTTP calls are async; the local-disk persistence stays sync -- filesystem
ops are microseconds and moving them off-loop would gain nothing but boilerplate.
"""

import json
import logging
import os
import uuid
from typing import Any

import aiohttp

from .client import SignalKError, _request

log = logging.getLogger(__name__)

_ACCESS_PATH = "/signalk/v1/access/requests"


def _auth_file(data_dir: str) -> str:
    return os.path.join(data_dir, "signalk_auth.json")


def _load(data_dir: str) -> dict[str, Any]:
    try:
        with open(_auth_file(data_dir), encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except OSError, json.JSONDecodeError:
        return {}


def _save(data_dir: str, data: dict[str, Any]) -> None:
    tmp = _auth_file(data_dir) + ".tmp"
    # The file holds a bearer token; create it 0600 rather than leaving it
    # world-readable. os.replace carries the mode onto the final path.
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, _auth_file(data_dir))


def client_id(data_dir: str) -> str:
    """Stable per-install client id; generated once and persisted."""
    a = _load(data_dir)
    if not a.get("clientId"):
        a["clientId"] = str(uuid.uuid4())
        _save(data_dir, a)
    return str(a["clientId"])


def saved_token(data_dir: str) -> str | None:
    tok = _load(data_dir).get("token")
    return str(tok) if tok else None


def save_token(data_dir: str, token: str) -> None:
    a = _load(data_dir)
    a["token"] = token
    _save(data_dir, a)


def clear_token(data_dir: str) -> None:
    a = _load(data_dir)
    a.pop("token", None)
    _save(data_dir, a)


async def request_access(
    session: aiohttp.ClientSession,
    base_url: str,
    cid: str,
    description: str,
    timeout: float = 10.0,
) -> str:
    """POST an access request; return the request href to poll."""
    r = await _request(
        session,
        base_url.rstrip("/") + _ACCESS_PATH,
        method="POST",
        body={"clientId": cid, "description": description},
        timeout=timeout,
    )
    if not isinstance(r, dict) or not r.get("href"):
        raise SignalKError(f"unexpected access-request response: {r}")
    return str(r["href"])


async def poll_request(
    session: aiohttp.ClientSession,
    base_url: str,
    href: str,
    timeout: float = 10.0,
) -> tuple[str, str | None]:
    """Poll a pending request. Returns (state, token) where state is one of
    PENDING / APPROVED / DENIED."""
    r = await _request(session, base_url.rstrip("/") + href, timeout=timeout)
    if not isinstance(r, dict):
        return ("PENDING", None)
    if r.get("state") == "COMPLETED":
        ar = r.get("accessRequest") or {}
        if ar.get("permission") == "APPROVED":
            return ("APPROVED", ar.get("token"))
        return ("DENIED", None)
    return ("PENDING", None)
