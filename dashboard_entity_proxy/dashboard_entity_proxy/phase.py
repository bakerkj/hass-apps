# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Lifecycle phase of a proxy ``Session``.

A session transitions through four named phases::

    CONNECTING ── auth_ok ──▶ STARTUP ── scope-ready ──▶ READY
        │                       │                          │
        └──── _done.set() ──────┴──────────────────────────┴──▶ CLOSING

CONNECTING
    Pre-auth. Awaiting the ``auth_required`` / ``auth_ok`` handshake
    with HA.

STARTUP
    HA accepted auth. The proxy's startup commands have been sent
    (mirror subscription, entity/device registries, registry update
    subscriptions, lovelace_updated subscription, energy/get_prefs,
    initial lovelace/config). Waiting for the responses needed to
    resolve initial scope.

READY
    Initial scope has resolved (either via lovelace/config response or
    via the watchdog widening to "all entities" on timeout). Steady-
    state frame forwarding and subsequent navigations re-resolve scope
    in place.

CLOSING
    ``_done`` has been set; cleanup pending. No new frames should be
    emitted to the client, since emitting risks a
    ``ConnectionResetError`` on a closed writer.

Data-readiness flags like ``ScopeResolver.ready``,
``RegistryStore.have_entities``, and ``RegistryStore.have_devices`` are
orthogonal to the phase: they track readiness of specific data, not
the overall lifecycle.
"""

from __future__ import annotations

import enum


class Phase(enum.Enum):
    """Session lifecycle phase. See module docstring for transitions."""

    CONNECTING = "connecting"
    STARTUP = "startup"
    READY = "ready"
    CLOSING = "closing"
