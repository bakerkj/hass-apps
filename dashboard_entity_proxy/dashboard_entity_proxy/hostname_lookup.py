# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Best-effort reverse-DNS cache for status-UI display. Maps a client
IP to its short hostname (first DNS label only — not the FQDN) so the
Ingress status page can show ``kiosk-1`` next to ``192.168.1.50``.

Resolution is asynchronous and TTL-cached: ``prime()`` schedules a
background lookup when an IP is first seen, and again whenever the
cached entry has aged past :data:`_CACHE_TTL` — so DHCP-driven name
changes are picked up within a few minutes without forcing a restart.
``cached_hostname()`` returns whatever is in the cache without blocking,
even if it's stale, so the UI doesn't flicker while a refresh is in
flight. A failed PTR caches as the empty string and is retried on the
same TTL.
"""

import asyncio
import logging
import socket
import time

log = logging.getLogger(__name__)

# Bound on the reverse-DNS query itself. Resolvers usually answer in
# tens of milliseconds; anything past a couple of seconds is a broken
# nameserver and we'd rather show the IP than hang the lookup task.
_RESOLVE_TIMEOUT = 2.0

# How long a cached hostname (or cached "no hostname") is trusted before
# the next ``prime()`` call schedules a refresh. Long enough that we
# don't hammer the resolver from the 2-second UI poll, short enough
# that a DHCP reassignment surfaces within a normal session.
_CACHE_TTL = 900.0

# IP → (timestamp, short hostname). Missing key means "never looked
# up". An empty hostname means "looked up, no usable PTR" — it still
# gets refreshed on the same TTL so a host that gains a PTR later
# eventually shows up.
_cache: dict[str, tuple[float, str]] = {}
_in_flight: set[str] = set()


def cached_hostname(ip: str) -> str:
    """Return the cached short hostname for ``ip``, or empty string if
    the lookup hasn't completed (or didn't find anything). May return a
    stale value while a refresh is in flight — that's preferable to
    flickering empty in the UI mid-resolve.
    """
    entry = _cache.get(ip)
    return entry[1] if entry is not None else ""


def prime(ip: str) -> None:
    """Schedule a reverse-DNS lookup for ``ip`` if it isn't already in
    flight, and either has no cached entry yet or has one older than
    :data:`_CACHE_TTL`. Safe to call from sync code; a no-op outside a
    running event loop.
    """
    if not ip or ip in _in_flight:
        return
    entry = _cache.get(ip)
    if entry is not None and (time.monotonic() - entry[0]) < _CACHE_TTL:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _in_flight.add(ip)
    loop.create_task(_resolve(ip))


async def _resolve(ip: str) -> None:
    loop = asyncio.get_running_loop()
    try:
        try:
            # ``NI_NAMEREQD`` makes a missing PTR raise ``gaierror``
            # instead of echoing the IP back — the gaierror path below
            # is the single "no usable hostname" branch. The alternative
            # (string-compare the result against the input) is fragile
            # for IPv6, where the resolver may canonicalise the address
            # to a different textual form than we passed in.
            host, _ = await asyncio.wait_for(
                loop.getnameinfo((ip, 0), socket.NI_NAMEREQD),
                timeout=_RESOLVE_TIMEOUT,
            )
        except (asyncio.TimeoutError, socket.gaierror, OSError) as exc:
            log.debug("%s reverse DNS lookup failed: %r", ip, exc)
            _cache[ip] = (time.monotonic(), "")
            return
        short = host.split(".", 1)[0] if host else ""
        _cache[ip] = (time.monotonic(), short)
    finally:
        # Runs on success, failure, and ``CancelledError`` (loop
        # teardown). Without this, a cancelled lookup would leave the
        # IP wedged in ``_in_flight`` and silently block every future
        # ``prime()`` for it.
        _in_flight.discard(ip)


def _reset_for_tests() -> None:
    """Test hook: clear caches between unit-test cases."""
    _cache.clear()
    _in_flight.clear()
