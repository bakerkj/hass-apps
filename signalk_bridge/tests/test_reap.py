# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the cold-start AIS orphan reap in app._reap_ais_orphans.

The reap has a history of shipping bugs the type checker and unit tests
missed (an invalid MQTT wildcard silently matched nothing; SIGTERM was
delayed the full window when messages were sparse). This test file
exercises those regressions directly against a mock aiomqtt.Client so a
future refactor can't quietly break the same failure modes again.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest
from signalk_bridge.app import _AIS_CONFIG_TOPIC_PATTERN, _reap_ais_orphans


@dataclass
class _FakeMsg:
    """Shape of an aiomqtt message the reap actually reads."""

    topic: str
    retain: bool = True


class _FakeClient:
    """Minimal aiomqtt.Client stand-in.

    ``messages`` is an async iterator over a queue that ``feed`` pushes to.
    ``subscribe``/``unsubscribe`` calls are recorded so the tests can assert
    on the exact topic filter the reap uses (an earlier bug shipped an
    invalid ``ais_+`` wildcard for months without a test catching it).
    """

    def __init__(self) -> None:
        self._q: asyncio.Queue[_FakeMsg] = asyncio.Queue()
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append(topic)

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)

    @property
    def messages(self) -> _FakeClient:
        return self

    def __aiter__(self) -> _FakeClient:
        return self

    async def __anext__(self) -> _FakeMsg:
        return await self._q.get()

    def feed(self, topic: str, *, retain: bool = True) -> None:
        self._q.put_nowait(_FakeMsg(topic=topic, retain=retain))


@pytest.mark.asyncio
async def test_reap_subscribes_to_full_segment_wildcard() -> None:
    """The subscribe filter MUST use ``+`` as a whole level. ``ais_+`` was
    a shipped no-op for a month before an adversarial review caught it."""
    mq = _FakeClient()
    stop = asyncio.Event()

    async def run() -> set[str]:
        return await _reap_ais_orphans(mq, "homeassistant", 0.05, stop)

    result = await run()
    assert mq.subscribed == ["homeassistant/device_tracker/signalk/+/config"]
    # ``ais_<mmsi>`` cannot be a topic-level literal glued to ``+``; verify
    # the fix hasn't silently regressed to that form.
    assert not any("ais_+" in t for t in mq.subscribed)
    assert result == set()
    # Unsubscribed on the way out, same topic.
    assert mq.unsubscribed == ["homeassistant/device_tracker/signalk/+/config"]


@pytest.mark.asyncio
async def test_reap_collects_retained_ais_mmsis() -> None:
    mq = _FakeClient()
    mq.feed("homeassistant/device_tracker/signalk/ais_367674550/config")
    mq.feed("homeassistant/device_tracker/signalk/ais_338216333/config")
    stop = asyncio.Event()

    # Feed messages before starting; the reap drains them then hits the
    # window timeout.
    observed = await _reap_ais_orphans(mq, "homeassistant", 0.15, stop)
    assert observed == {"367674550", "338216333"}


@pytest.mark.asyncio
async def test_reap_ignores_non_retained_messages() -> None:
    """A fresh (non-retained) publish arriving mid-window isn't HA's
    persistent memory -- e.g. our own reap-window discovery publishes on
    other trackers. Don't count them."""
    mq = _FakeClient()
    mq.feed(
        "homeassistant/device_tracker/signalk/ais_111111111/config",
        retain=False,
    )
    mq.feed(
        "homeassistant/device_tracker/signalk/ais_222222222/config",
        retain=True,
    )
    stop = asyncio.Event()
    observed = await _reap_ais_orphans(mq, "homeassistant", 0.15, stop)
    assert observed == {"222222222"}


@pytest.mark.asyncio
async def test_reap_filters_inventory_and_non_ais_slugs() -> None:
    """The subscribe pattern is a broad wildcard on the discovery config
    space, so the client-side marker check has to drop entries that
    aren't real MMSI slugs -- inventory sensor, plus any hypothetical
    non-AIS device_tracker HA remembers."""
    mq = _FakeClient()
    mq.feed(f"homeassistant/{_AIS_CONFIG_TOPIC_PATTERN}367674550/config")
    mq.feed(f"homeassistant/{_AIS_CONFIG_TOPIC_PATTERN}inventory/config")
    mq.feed("homeassistant/device_tracker/signalk/vessel/config")
    mq.feed("homeassistant/device_tracker/other/foo/config")
    stop = asyncio.Event()
    observed = await _reap_ais_orphans(mq, "homeassistant", 0.15, stop)
    assert observed == {"367674550"}


@pytest.mark.asyncio
async def test_reap_stop_race_returns_promptly_on_empty_queue() -> None:
    """When no messages arrive, stop.set() must unblock the reap
    immediately rather than force it to wait the full window. This is
    the SIGTERM-latency regression the original ``async for`` loop
    shipped -- fixed by racing ``mq.messages.__anext__`` against
    ``stop.wait``. The window here is deliberately large; if the race is
    broken the test would wall-clock for that long."""
    mq = _FakeClient()
    stop = asyncio.Event()

    async def stop_after(delay: float) -> None:
        await asyncio.sleep(delay)
        stop.set()

    started = asyncio.get_running_loop().time()
    # 60s window, but stop fires after 100ms.
    await asyncio.gather(
        _reap_ais_orphans(mq, "homeassistant", 60.0, stop),
        stop_after(0.1),
    )
    elapsed = asyncio.get_running_loop().time() - started
    assert elapsed < 1.0, f"reap did not race stop; elapsed={elapsed:.2f}s"
    # And the unsubscribe still ran on the way out.
    assert mq.unsubscribed == ["homeassistant/device_tracker/signalk/+/config"]


@pytest.mark.asyncio
async def test_reap_unsubscribe_failure_is_non_fatal() -> None:
    """A broker that dies just as we're unsubscribing at end-of-window
    shouldn't propagate an exception -- the reap has already collected
    everything it's going to, and the caller only cares about the
    observed set."""

    class _FailUnsubClient(_FakeClient):
        async def unsubscribe(self, topic: str) -> None:
            self.unsubscribed.append(topic)
            raise ConnectionError("broker dropped")

    mq = _FailUnsubClient()
    mq.feed("homeassistant/device_tracker/signalk/ais_367674550/config")
    stop = asyncio.Event()
    observed = await _reap_ais_orphans(mq, "homeassistant", 0.1, stop)
    assert observed == {"367674550"}
