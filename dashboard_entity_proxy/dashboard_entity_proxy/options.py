# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shared ``Options`` dataclass used by both ``proxy.py`` (one instance
per app construction) and ``session.py`` (the same instance threaded
into each per-connection ``Session``). Keeps the proxy's app-level
knobs and the session's per-connection knobs on a single object so
there's one source of truth for both.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .customization import Customization

if TYPE_CHECKING:
    from .session_registry import SessionRegistry


@dataclass
class Options:
    """Configuration for one ``create_app`` invocation and the
    per-connection ``Session``s spawned from it. Covers:

    * Upstream HA URL and the header policy the proxy uses when talking
      to it (``target_url``, ``transparent``, ``passthrough_all``).
    * The throttle window and scope include/exclude filters threaded
      into each session.
    * The cross-session ``SessionRegistry`` reference (for the status
      UI) and the loaded customization extensions.
    * Registry-update tracking knobs.
    """

    # Upstream HA.
    target_url: str = "http://homeassistant:8123"
    transparent: bool = True
    passthrough_all: bool = False

    # Steady-state knobs.
    throttle: float = 0.0
    extra_entities: list[str] = field(default_factory=list)
    include_globs: list[str] = field(default_factory=list)
    exclude_globs: list[str] = field(default_factory=list)

    # Shared infrastructure.
    registry: "SessionRegistry | None" = None
    logger: logging.Logger | None = None
    customization: Customization = field(default_factory=Customization)

    # Registry-update tracking. See ARCHITECTURE.md for the ``full`` vs
    # ``incremental`` trade-offs and the burst-threshold promotion rule.
    registry_mode: str = "incremental"
    registry_refetch_interval: float = 60.0
    registry_burst_threshold: int = 50
