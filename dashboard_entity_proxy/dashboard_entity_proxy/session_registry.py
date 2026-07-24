# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Registry of active and recently-disconnected proxy connections.

Backs the Ingress status UI's ``/api/sessions`` snapshot. Distinct from
the entity-index module (``entity_index.py``), which indexes HA's
entity and device registries for scope resolution: two unrelated
"registries" with separate concerns.
"""

import time
from datetime import UTC, datetime
from typing import Any

from .const import DEFAULT_DISCONNECT_RETENTION_SECONDS


class SessionRegistry:
    """Tracks active connections (intercepted sessions and raw tunnels) so the
    status UI can show everything the proxy is handling. Holds anything with a
    ``status() -> dict`` method.

    Recently-disconnected sessions linger as frozen status snapshots tagged
    with ``disconnected_at`` for ``retention_seconds`` after they end, so the
    UI can show "this tab was here a moment ago" rather than blinking them
    out instantly. Snapshots beyond the retention window are pruned on every
    ``snapshot()`` call.

    Concurrency: this class holds no locks. That is sound only because every
    mutation runs on the single asyncio event loop the proxy uses; any
    integration-test injector that runs on another thread MUST hop to the
    loop with ``loop.call_soon_threadsafe`` before touching the registry.
    """

    def __init__(
        self, retention_seconds: float = DEFAULT_DISCONNECT_RETENTION_SECONDS
    ) -> None:
        self._conns: set[Any] = set()
        # (monotonic-at-disconnect, raw connected_at datetime, frozen status
        # dict including disconnected_at). Storing the raw ``datetime``
        # alongside the snapshot lets ``snapshot()`` sort by a real
        # comparable value rather than the ISO string in the dict; see
        # the invariant comment in ``snapshot()``.
        self._recent: list[tuple[float, datetime, dict[str, Any]]] = []
        self._retention = retention_seconds

    def add(self, conn: Any) -> None:
        """Register a live connection so it shows in the status UI."""
        self._conns.add(conn)

    def remove(self, conn: Any) -> None:
        """De-register a connection on disconnect, capturing its final
        ``status()`` snapshot tagged with ``disconnected_at`` so the UI can
        linger it for the retention window instead of blinking it out.
        """
        if conn not in self._conns:
            return
        self._conns.discard(conn)
        snap = conn.status()
        snap["disconnected_at"] = datetime.now(UTC).isoformat()
        # ``_connected_at`` is a raw aware ``datetime`` on both Session and
        # TunnelConnection. Fall back to ``now`` if a test double omits it.
        raw_dt = getattr(conn, "_connected_at", datetime.now(UTC))
        self._recent.append((time.monotonic(), raw_dt, snap))

    def snapshot(self, *, detail: str = "summary") -> list[dict[str, Any]]:
        """Return live + recently-disconnected session statuses, sorted by
        connected-at. Prunes recents past the retention window in place.

        ``detail`` is forwarded to each live connection's ``status()`` when
        the method accepts the kwarg. Session uses it to switch between
        the small per-poll shape (default) and the full ``scope_entities``
        shape. Connections whose ``status()`` doesn't accept the kwarg
        (e.g. ``TunnelConnection``) are called bare for backward compat.

        Invariant: sort by the raw ``datetime`` (``_connected_at``), never by
        the ISO string in the snapshot dict. ISO sort works only as long as
        every writer uses the same timezone-aware format; a future writer
        that drops the tz suffix or truncates microseconds would silently
        break the ordering.
        """
        now_mono = time.monotonic()
        cutoff = now_mono - self._retention
        # Drop expired recents in place so memory stays bounded.
        self._recent = [(t, dt, s) for t, dt, s in self._recent if t > cutoff]
        live: list[tuple[datetime, dict[str, Any]]] = [
            (
                getattr(c, "_connected_at", datetime.now(UTC)),
                _call_status(c, detail),
            )
            for c in self._conns
        ]
        recent: list[tuple[datetime, dict[str, Any]]] = [
            (dt, s) for _, dt, s in self._recent
        ]
        combined = live + recent
        combined.sort(key=lambda pair: pair[0])
        return [s for _, s in combined]


def _call_status(conn: Any, detail: str) -> dict[str, Any]:
    """Call ``conn.status()`` forwarding ``detail`` when the signature
    accepts it. ``TunnelConnection.status()`` and any external test double
    are called bare, preserving backward compatibility.
    """
    try:
        return conn.status(detail=detail)
    except TypeError:
        return conn.status()
