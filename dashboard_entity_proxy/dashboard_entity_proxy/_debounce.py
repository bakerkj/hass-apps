# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Trailing-edge debouncer for coalescing event bursts into one callback fire.

Used by the session's registry-update path so a burst of HA registry
events (entity_registry_updated, device_registry_updated) collapses into
a single rebuild/refetch instead of a per-event flurry.
"""

from __future__ import annotations

import asyncio
from typing import Any


class _TrailingDebouncer:
    """Coalesces a burst of events into a single callback fire.

    Each ``poke()`` (re)starts the timer; the callback fires once after
    ``interval`` seconds with no further pokes. Pure trailing-edge: the
    burst's total size is known at flush time, so the caller can decide
    between per-event handling and a bulk-promotion path (incremental
    mode's burst-threshold rule).
    """

    def __init__(self, interval: float, flush_cb: "Any") -> None:
        self._interval = interval
        self._flush = flush_cb
        self._handle: asyncio.TimerHandle | None = None

    def poke(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
        # ``get_running_loop`` requires a running loop, which is guaranteed
        # here (poke is invoked from async handlers). Python 3.14 plans to
        # raise on ``get_event_loop`` outside a running loop, so prefer the
        # explicit accessor.
        loop = asyncio.get_running_loop()
        self._handle = loop.call_later(self._interval, self._fire)

    def _fire(self) -> None:
        self._handle = None
        self._flush()

    def cancel(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
