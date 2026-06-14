# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Test-only constructor for ``dashboard_entity_proxy.session.Session``.

Building a real Session via ``__init__`` (with placeholder WebSocket
objects, ``remote = "test"``, a default ``Options``, and a test logger)
gives unit tests a fully-initialised instance without ``object.__new__``
plus a scatter of hand-wired attributes. Lives outside the production
package so production code carries no test knowledge.

``asyncio.Queue`` and ``asyncio.Event`` construct fine outside a running
loop and only bind one on first await, so the returned Session is usable
from sync test bodies. Tests that need to drive specific behaviour
override individual attributes or stub methods on the returned instance.
"""

from __future__ import annotations

import logging
from typing import Any

from dashboard_entity_proxy.options import Options
from dashboard_entity_proxy.session import Session


def build_session_for_test(
    *,
    opts: Options | None = None,
    log: logging.Logger | None = None,
) -> Session:
    """Build a real ``Session`` for unit-test use. See module docstring."""
    ws_stub: Any = object()
    return Session(
        ws_client=ws_stub,
        ws_ha=ws_stub,
        remote="test",
        opts=opts if opts is not None else Options(),
        log=log if log is not None else logging.getLogger("test.session"),
    )
