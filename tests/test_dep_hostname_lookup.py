# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the reverse-DNS cache that backs the status UI's
``hostname`` field. We don't talk to a real resolver — the OS-level
``getnameinfo`` call is monkey-patched on the asyncio loop so the tests
exercise the cache, the TTL refresh, and the first-label truncation
without depending on /etc/hosts or a live DNS server.
"""

from __future__ import annotations

import asyncio
import socket
from typing import Any

import pytest

from dashboard_entity_proxy import hostname_lookup


@pytest.fixture(autouse=True)
def _reset() -> None:
    hostname_lookup._reset_for_tests()


async def _drain() -> None:
    # Two passes: one for the scheduled lookup task, one for any
    # follow-up callbacks the task creates before it returns.
    for _ in range(2):
        await asyncio.sleep(0)


def _patch_lookup(
    monkeypatch: pytest.MonkeyPatch, results: dict[str, str | Exception]
) -> list[str]:
    """Replace ``loop.getnameinfo`` with a stub backed by ``results``.
    Returns a list that records each IP looked up so tests can assert
    on hit-count / refresh behaviour.
    """
    called: list[str] = []

    async def fake_getnameinfo(
        sockaddr: tuple[Any, ...], _flags: int
    ) -> tuple[str, str]:
        ip = sockaddr[0]
        called.append(ip)
        outcome = results.get(ip, ip)  # default: PTR-less → IP echoed back
        if isinstance(outcome, Exception):
            raise outcome
        return (outcome, "0")

    loop = asyncio.get_event_loop()
    monkeypatch.setattr(loop, "getnameinfo", fake_getnameinfo)
    return called


@pytest.mark.asyncio
async def test_prime_resolves_and_caches_short_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FQDN from the resolver is truncated to its first label."""
    _patch_lookup(monkeypatch, {"192.168.1.50": "kiosk-1.lan.example.com"})
    hostname_lookup.prime("192.168.1.50")
    await _drain()
    assert hostname_lookup.cached_hostname("192.168.1.50") == "kiosk-1"


@pytest.mark.asyncio
async def test_cached_before_resolution_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cached_hostname`` never blocks: it returns "" until the
    background lookup completes.
    """
    _patch_lookup(monkeypatch, {"10.0.0.5": "tablet.lan"})
    hostname_lookup.prime("10.0.0.5")
    # No await between prime and the read → lookup task hasn't run yet.
    assert hostname_lookup.cached_hostname("10.0.0.5") == ""
    await _drain()
    assert hostname_lookup.cached_hostname("10.0.0.5") == "tablet"


@pytest.mark.asyncio
async def test_failed_lookup_caches_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolver error caches as empty so we don't retry on every poll
    (the TTL still re-tries later — that's covered separately).
    """
    called = _patch_lookup(monkeypatch, {"203.0.113.7": socket.gaierror("nope")})
    hostname_lookup.prime("203.0.113.7")
    await _drain()
    assert hostname_lookup.cached_hostname("203.0.113.7") == ""
    # Second prime inside TTL is a no-op — no extra resolver call.
    hostname_lookup.prime("203.0.113.7")
    await _drain()
    assert called == ["203.0.113.7"]


@pytest.mark.asyncio
async def test_ptr_returning_ip_treated_as_no_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``getnameinfo`` echoes the IP back when there's no PTR record;
    that's "no hostname", not a literal hostname of "192.168.1.99".
    """
    _patch_lookup(monkeypatch, {})  # default behaviour: echo IP
    hostname_lookup.prime("192.168.1.99")
    await _drain()
    assert hostname_lookup.cached_hostname("192.168.1.99") == ""


@pytest.mark.asyncio
async def test_ttl_triggers_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Past the TTL, ``prime`` re-resolves. We simulate the passage of
    time by reaching into the cache and ageing the timestamp.
    """
    results: dict[str, str | Exception] = {"10.1.2.3": "alpha.lan"}
    called = _patch_lookup(monkeypatch, results)
    hostname_lookup.prime("10.1.2.3")
    await _drain()
    assert hostname_lookup.cached_hostname("10.1.2.3") == "alpha"

    # Age the cache entry past the TTL and flip the resolver's answer.
    ts, val = hostname_lookup._cache["10.1.2.3"]
    hostname_lookup._cache["10.1.2.3"] = (
        ts - hostname_lookup._CACHE_TTL - 1.0,
        val,
    )
    results["10.1.2.3"] = "beta.lan"

    hostname_lookup.prime("10.1.2.3")
    await _drain()
    assert hostname_lookup.cached_hostname("10.1.2.3") == "beta"
    assert called == ["10.1.2.3", "10.1.2.3"]


@pytest.mark.asyncio
async def test_prime_within_ttl_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = _patch_lookup(monkeypatch, {"10.9.9.9": "x.example"})
    hostname_lookup.prime("10.9.9.9")
    await _drain()
    for _ in range(5):
        hostname_lookup.prime("10.9.9.9")
        await _drain()
    assert called == ["10.9.9.9"]


def test_prime_outside_event_loop_is_safe() -> None:
    """Called from sync construction without a running loop, prime() is
    a no-op rather than raising. The next status() call from inside the
    loop will pick it up.
    """
    hostname_lookup.prime("198.51.100.1")
    assert hostname_lookup.cached_hostname("198.51.100.1") == ""


def test_empty_ip_is_ignored() -> None:
    hostname_lookup.prime("")
    assert hostname_lookup.cached_hostname("") == ""
