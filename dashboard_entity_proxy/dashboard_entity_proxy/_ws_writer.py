# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Single-writer-per-WebSocket queue drainer.

Drains one outbound queue, sending each ``(kind, payload)`` to the
bound WebSocket. Single-writer-per-WebSocket so producers can enqueue
with ``put_nowait`` without awaiting; this absorbs back-pressure.

Exits cleanly when ``done`` is set and the queue is drained, so the
cleanup path can ``wait_for`` each writer with a short deadline
instead of cancelling mid-send. Any write failure invokes
``on_write_failure`` and returns.

``queue.task_done()`` after each ``get()`` pairs the get for
correctness; cancellation paths skip it because the item was never
observed.
"""

import asyncio
from collections.abc import Callable
from typing import Any


class WsWriter:
    def __init__(
        self,
        ws: Any,
        queue: asyncio.Queue[tuple[str, Any]],
        done: asyncio.Event,
        on_write_failure: Callable[[], None],
    ) -> None:
        self._ws = ws
        self._queue = queue
        self._done = done
        self._on_write_failure = on_write_failure

    async def run(self) -> None:
        done_wait = asyncio.create_task(self._done.wait())
        try:
            while True:
                if self._done.is_set() and self._queue.empty():
                    return
                getter = asyncio.create_task(self._queue.get())
                try:
                    finished, _ = await asyncio.wait(
                        {getter, done_wait}, return_when=asyncio.FIRST_COMPLETED
                    )
                except asyncio.CancelledError:
                    getter.cancel()
                    raise
                if getter not in finished:
                    # ``done`` fired with nothing pending: drain whatever
                    # already landed (put_nowait paths queue synchronously)
                    # then exit.
                    getter.cancel()
                    try:
                        await getter
                    except asyncio.CancelledError, Exception:  # noqa: BLE001, S110 - best-effort drain after cancel
                        pass
                    continue
                kind, payload = getter.result()
                try:
                    if kind == "text":
                        await self._ws.send_str(payload)
                    else:
                        await self._ws.send_bytes(payload)
                except Exception:  # noqa: BLE001 - write failure ends the session
                    self._queue.task_done()
                    self._on_write_failure()
                    return
                self._queue.task_done()
        finally:
            done_wait.cancel()
