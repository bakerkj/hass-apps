# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for WsWriter.

Constructs the writer against a fake WS (recording each send) and a
real ``asyncio.Queue`` + ``asyncio.Event``, exercises the race between
inbound queue items and the done signal, and verifies the
write-failure → on_write_failure → exit path.
"""

from __future__ import annotations

import asyncio

import pytest

from dashboard_entity_proxy._ws_writer import WsWriter


class _FakeWS:
    def __init__(self, fail_on: str | None = None) -> None:
        self.sent_text: list[str] = []
        self.sent_bytes: list[bytes] = []
        self._fail_on = fail_on

    async def send_str(self, data: str) -> None:
        if self._fail_on == "text":
            raise RuntimeError("simulated send_str failure")
        self.sent_text.append(data)

    async def send_bytes(self, data: bytes) -> None:
        if self._fail_on == "bytes":
            raise RuntimeError("simulated send_bytes failure")
        self.sent_bytes.append(data)


async def test_writer_drains_queue_until_done():
    """Items enqueued before done.set() are all sent; the writer exits
    once done is set and the queue is empty.
    """
    ws = _FakeWS()
    queue: asyncio.Queue = asyncio.Queue()
    done = asyncio.Event()
    failures: list[None] = []
    writer = WsWriter(ws, queue, done, lambda: failures.append(None))

    queue.put_nowait(("text", "first"))
    queue.put_nowait(("text", "second"))
    queue.put_nowait(("bin", b"\x01\x02"))

    task = asyncio.create_task(writer.run())
    # Let the writer drain.
    await asyncio.sleep(0.05)
    done.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert ws.sent_text == ["first", "second"]
    assert ws.sent_bytes == [b"\x01\x02"]
    assert failures == []  # clean exit, no failure path


async def test_writer_exits_promptly_when_done_set_with_empty_queue():
    """done.set() with nothing pending exits within milliseconds rather
    than blocking on the empty queue.
    """
    import time

    ws = _FakeWS()
    queue: asyncio.Queue = asyncio.Queue()
    done = asyncio.Event()
    writer = WsWriter(ws, queue, done, lambda: None)

    async def fire_done() -> None:
        await asyncio.sleep(0.01)
        done.set()

    start = time.monotonic()
    await asyncio.gather(writer.run(), fire_done())
    elapsed = time.monotonic() - start
    assert elapsed < 0.5


async def test_writer_invokes_failure_callback_and_exits_on_write_error():
    """A failing ws.send_* invokes ``on_write_failure`` exactly once and
    causes the writer to exit. Subsequent enqueues don't trigger more
    failures because the writer is no longer draining.
    """
    ws = _FakeWS(fail_on="text")
    queue: asyncio.Queue = asyncio.Queue()
    done = asyncio.Event()
    failures: list[None] = []
    writer = WsWriter(ws, queue, done, lambda: failures.append(None))

    queue.put_nowait(("text", "doomed"))

    task = asyncio.create_task(writer.run())
    await asyncio.wait_for(task, timeout=1.0)

    assert failures == [None]
    assert ws.sent_text == []  # never landed


@pytest.fixture(autouse=True)
def _asyncio_mode():
    """Tests in this file are async; ``pytest-asyncio``'s auto mode is
    configured in pyproject.toml so individual tests don't need a marker.
    """
    yield
