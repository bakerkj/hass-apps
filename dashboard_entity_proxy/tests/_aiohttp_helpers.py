# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shared aiohttp test helpers: ephemeral port + app runner bootstrap.

Lives in its own module rather than ``conftest.py`` because importing
``from conftest import X`` is fragile when multiple ``conftest.py`` files
are loaded in the same pytest run — whichever was imported first wins
``sys.modules["conftest"]``.
"""

from __future__ import annotations

import socket

from aiohttp import web


def _free_port() -> int:
    """Return an ephemeral TCP port the OS isn't currently holding."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _start_app(app: web.Application) -> tuple[web.AppRunner, str]:
    """Start an aiohttp app on a free port. Returns ``(runner, base_url)``.

    Caller is responsible for ``await runner.cleanup()`` in its teardown.
    """
    port = _free_port()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, f"http://127.0.0.1:{port}"
