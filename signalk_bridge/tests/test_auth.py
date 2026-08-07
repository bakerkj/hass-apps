# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the Signal K access-request flow."""

from typing import Any
from unittest import mock

import pytest

from signalk_bridge import auth


def test_client_id_is_stable(tmp_path: Any) -> None:
    d = str(tmp_path)
    a = auth.client_id(d)
    b = auth.client_id(d)
    assert a == b and len(a) >= 8  # generated once, persisted


def test_token_save_load_clear(tmp_path: Any) -> None:
    d = str(tmp_path)
    assert auth.saved_token(d) is None
    auth.save_token(d, "tok-123")
    assert auth.saved_token(d) == "tok-123"
    auth.clear_token(d)
    assert auth.saved_token(d) is None
    # client id survives token clear
    assert auth.client_id(d)


def _async_return(value: Any) -> Any:
    """AsyncMock-style helper: a coroutine that returns ``value``."""

    async def _coro(*_a: Any, **_kw: Any) -> Any:
        return value

    return _coro


@pytest.mark.asyncio
async def test_request_access_returns_href() -> None:
    with mock.patch.object(
        auth,
        "_request",
        new=_async_return({"href": "/signalk/v1/requests/x", "state": "PENDING"}),
    ):
        assert (
            await auth.request_access(mock.MagicMock(), "http://sk:3000", "cid", "desc")
            == "/signalk/v1/requests/x"
        )


@pytest.mark.asyncio
async def test_poll_pending_approved_denied() -> None:
    session = mock.MagicMock()

    with mock.patch.object(auth, "_request", new=_async_return({"state": "PENDING"})):
        assert await auth.poll_request(session, "http://sk:3000", "/r") == (
            "PENDING",
            None,
        )

    with mock.patch.object(
        auth,
        "_request",
        new=_async_return(
            {
                "state": "COMPLETED",
                "accessRequest": {"permission": "APPROVED", "token": "T"},
            }
        ),
    ):
        assert await auth.poll_request(session, "http://sk:3000", "/r") == (
            "APPROVED",
            "T",
        )

    with mock.patch.object(
        auth,
        "_request",
        new=_async_return(
            {"state": "COMPLETED", "accessRequest": {"permission": "DENIED"}}
        ),
    ):
        assert await auth.poll_request(session, "http://sk:3000", "/r") == (
            "DENIED",
            None,
        )
