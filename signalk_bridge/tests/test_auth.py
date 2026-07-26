# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the Signal K access-request flow."""

from typing import Any
from unittest import mock

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


def test_request_access_returns_href() -> None:
    with mock.patch.object(
        auth,
        "_request",
        return_value={"href": "/signalk/v1/requests/x", "state": "PENDING"},
    ):
        assert (
            auth.request_access("http://sk:3000", "cid", "desc")
            == "/signalk/v1/requests/x"
        )


def test_poll_pending_approved_denied() -> None:
    with mock.patch.object(auth, "_request", return_value={"state": "PENDING"}):
        assert auth.poll_request("http://sk:3000", "/r") == ("PENDING", None)
    with mock.patch.object(
        auth,
        "_request",
        return_value={
            "state": "COMPLETED",
            "accessRequest": {"permission": "APPROVED", "token": "T"},
        },
    ):
        assert auth.poll_request("http://sk:3000", "/r") == ("APPROVED", "T")
    with mock.patch.object(
        auth,
        "_request",
        return_value={"state": "COMPLETED", "accessRequest": {"permission": "DENIED"}},
    ):
        assert auth.poll_request("http://sk:3000", "/r") == ("DENIED", None)
