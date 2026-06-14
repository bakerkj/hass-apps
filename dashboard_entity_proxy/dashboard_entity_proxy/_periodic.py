# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tick-every-N-seconds-until-done loop.

Races ``done`` against the wait so a session disconnecting partway
through an interval exits within milliseconds instead of waiting out
the full window. Keep the tick callable synchronous and short;
long-running work belongs in a dedicated task, not this loop.
"""

from __future__ import annotations

import asyncio
from typing import Callable


class PeriodicTask:
    """Calls ``tick()`` every ``interval`` seconds until ``done`` is set."""

    def __init__(
        self,
        *,
        done: asyncio.Event,
        interval: float,
        tick: Callable[[], object],
    ) -> None:
        self._done = done
        self._interval = interval
        self._tick = tick

    async def run(self) -> None:
        while not self._done.is_set():
            try:
                await asyncio.wait_for(self._done.wait(), timeout=self._interval)
                return
            except asyncio.TimeoutError:
                pass
            self._tick()
