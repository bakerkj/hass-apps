# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Safety-net coroutine that widens session scope to "all entities" if
the startup ``lovelace/config`` response doesn't land in time.

Dependencies are supplied to the constructor: an ``asyncio.Event`` to
abort on session close, a logger, a timeout, an ``is_scope_ready``
predicate, and a ``widen_to_all`` action.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable


class ScopeWatchdog:
    """If ``is_scope_ready()`` is still False after ``timeout`` seconds,
    invoke ``widen_to_all()``. Waits on ``done`` so a session that
    disconnects before the timeout exits cleanly with no fallback fired.
    """

    def __init__(
        self,
        *,
        done: asyncio.Event,
        timeout: float,
        log: logging.Logger,
        is_scope_ready: Callable[[], bool],
        widen_to_all: Callable[[], None],
    ) -> None:
        self._done = done
        self._timeout = timeout
        self._log = log
        self._is_scope_ready = is_scope_ready
        self._widen_to_all = widen_to_all

    async def run(self) -> None:
        try:
            await asyncio.wait_for(self._done.wait(), timeout=self._timeout)
            return
        except asyncio.TimeoutError:
            pass
        # The session could have closed during the same event-loop tick
        # the wait_for timed out on. Re-check before invoking the widen
        # action; sending frames to a closing peer raises on the writer.
        if self._done.is_set():
            return
        if not self._is_scope_ready():
            self._log.warning(
                "dashboard config not resolved in time; serving all entities"
            )
            self._widen_to_all()
