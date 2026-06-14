# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""HTTP-traffic visibility: tails the nginx access log, splits requests
by source IP into per-client views, and surfaces them as one
``SessionRegistry`` entry per client. Plain-HTTP traffic goes nginx → HA
directly without touching the Python proxy, so this is the only way the
status UI sees these requests.

Per-target rows inside a client expire after the same retention window
as disconnected sessions (:data:`DEFAULT_DISCONNECT_RETENTION_SECONDS`),
and the whole client is removed from the registry when all of its rows
have expired.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .const import DEFAULT_DISCONNECT_RETENTION_SECONDS, HTTP_ACCESS_LOG_MAX_BYTES

log = logging.getLogger(__name__)


# Matches the ``log_format dep_traffic`` directive in nginx.conf.tmpl:
#   $remote_addr "$request_method $request_uri" $status $bytes_sent
#   $request_length $request_time "$http_referer"
# bytes_sent = response total bytes (headers + body); request_length
# = request total bytes (request line + headers + body). The tracker
# folds them into per-row rx/tx counters that surface in the status UI.
_LOG_RE = re.compile(
    r"^(?P<addr>\S+)\s+"
    r'"(?P<method>\S+)\s+(?P<uri>[^"]*)"\s+'
    r"(?P<status>\d+)\s+"
    r"(?P<bytes_sent>\d+)\s+"
    r"(?P<request_length>\d+)\s+"
    r"(?P<time>\S+)\s+"
    r'"(?P<referer>[^"]*)"'
)

# HA addon-panel paths look like ``/<8hexhash>_<addon-name>`` (e.g.
# ``/f1c878cb_adsb-multi-portal-feeder``); the panel slug carries the
# addon identity. Used to classify both the URI and the Referer when
# bucketing into the ``addon: …`` target group.
_PANEL_RE = re.compile(r"^/([a-f0-9]{8}_[A-Za-z0-9_-]+)(?:/|$)")


def classify_target(uri: str, referer: str = "") -> str:
    """Bucket a URI (with optional Referer hint) into a short human label.

    The status UI groups rows by this label, so the goal is "few, stable,
    obvious" buckets — not per-URL granularity. Ingress traffic to a
    specific addon is the most useful signal; the Referer is consulted
    because the ingress URL itself just carries an opaque token.
    """
    if uri.startswith("/api/hassio_ingress/"):
        # Referer is a full URL — pull the path component before matching
        # the panel pattern that's anchored at "/".
        referer_path = urlparse(referer).path if referer else ""
        m = _PANEL_RE.match(referer_path)
        if m:
            return f"addon: {m.group(1)}"
        return "addon-ingress"
    m = _PANEL_RE.match(uri)
    if m:
        return f"addon: {m.group(1)}"
    if uri.startswith("/api/"):
        return "ha-rest"
    if uri.startswith("/static/") or uri.startswith("/frontend_latest/"):
        return "ha-frontend"
    if uri.startswith("/local/"):
        return "local-assets"
    if uri.startswith("/auth/"):
        return "auth"
    if uri.startswith("/lovelace") or uri.startswith("/dashboard"):
        head = uri.lstrip("/").split("/", 1)[0]
        return f"dashboard: {head}" if head else "dashboard"
    if uri == "/" or uri == "":
        return "/"
    return uri.lstrip("/").split("/", 1)[0]


@dataclass
class _HttpRow:
    target: str
    first_seen: datetime
    last_seen: datetime
    count: int = 0
    last_status: int = 0
    last_method: str = ""
    last_uri: str = ""
    # rx/tx aggregate bytes across all requests in this row. rx_bytes is
    # total request bytes (client → server, $request_length in nginx);
    # tx_bytes is total response bytes (server → client, $bytes_sent).
    rx_bytes: int = 0
    tx_bytes: int = 0


class _HttpClient:
    """One status-UI entry summarising the HTTP requests from a single
    source IP. Holds per-target rows (one per ``classify_target`` bucket).
    Registered with :class:`session.SessionRegistry` like a Session or
    TunnelConnection so the existing snapshot machinery handles it
    uniformly.

    Rows expire after the configured retention window; the client itself
    is considered "empty" once all of its rows have expired, which is the
    tracker's signal to deregister it from the registry.
    """

    def __init__(
        self, source: str, retention_seconds: float, kind: str = "http_client"
    ) -> None:
        self.source = source
        self._retention = retention_seconds
        self._rows: dict[str, _HttpRow] = {}
        # Fixed at construction so SessionRegistry sort order is stable
        # for this card (a new client joins the bottom of the list and
        # stays put until it expires).
        self._connected_at = datetime.now(timezone.utc)
        # Most recent observed activity, retained independently of
        # ``_rows`` so ``status()`` can report a meaningful ``last_seen``
        # even if the status snapshot races with row expiry (rows pruned
        # before the tracker's ``expire()`` removes the empty client).
        self._last_activity_at = self._connected_at
        self._kind = kind

    def record(
        self,
        *,
        method: str,
        uri: str,
        status: int,
        bytes_sent: int = 0,
        request_length: int = 0,
        referer: str = "",
    ) -> None:
        target = classify_target(uri, referer)
        now = datetime.now(timezone.utc)
        row = self._rows.get(target)
        if row is None:
            row = _HttpRow(target=target, first_seen=now, last_seen=now)
            self._rows[target] = row
        row.last_seen = now
        row.count += 1
        row.last_status = status
        row.last_method = method
        row.last_uri = uri
        row.tx_bytes += bytes_sent
        row.rx_bytes += request_length
        self._last_activity_at = now

    def _prune(self) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - self._retention
        self._rows = {
            k: r for k, r in self._rows.items() if r.last_seen.timestamp() > cutoff
        }

    def is_empty(self) -> bool:
        """Whether the client has no live rows left — the tracker's cue
        to deregister this view from the SessionRegistry.
        """
        self._prune()
        return not self._rows

    def status(self, *, detail: str = "summary") -> dict[str, Any]:
        self._prune()
        rows = sorted(self._rows.values(), key=lambda r: r.last_seen, reverse=True)
        total_rx = sum(r.rx_bytes for r in rows)
        total_tx = sum(r.tx_bytes for r in rows)
        total_count = sum(r.count for r in rows)
        # When all rows have just expired (race with the tracker's
        # ``expire()`` sweep), ``rows`` is empty but ``_last_activity_at``
        # still names the most recent observed request — preferable to
        # collapsing ``last_seen`` to the client's connection timestamp.
        last_seen = max(
            (r.last_seen for r in rows),
            default=self._last_activity_at,
        )
        if detail != "full":
            rows = rows[:50]
        return {
            "kind": self._kind,
            "remote_addr": self.source,
            "connected_at": self._connected_at.isoformat(),
            "last_seen": last_seen.isoformat(),
            "row_count": len(self._rows),
            "request_count": total_count,
            "rx_bytes": total_rx,
            "tx_bytes": total_tx,
            "rows": [
                {
                    "target": r.target,
                    "count": r.count,
                    "last_status": r.last_status,
                    "last_method": r.last_method,
                    "last_uri": r.last_uri,
                    "rx_bytes": r.rx_bytes,
                    "tx_bytes": r.tx_bytes,
                    "first_seen": r.first_seen.isoformat(),
                    "last_seen": r.last_seen.isoformat(),
                }
                for r in rows
            ],
        }


class HttpTrafficTracker:
    """Manages per-source :class:`_HttpClient` views. Registers each new
    client with the :class:`session.SessionRegistry` on first request,
    and removes clients whose rows have all expired.
    """

    def __init__(
        self,
        registry: Any,
        retention_seconds: float = DEFAULT_DISCONNECT_RETENTION_SECONDS,
    ) -> None:
        self._registry = registry
        self._retention = retention_seconds
        self._clients: dict[str, _HttpClient] = {}

    def record(
        self,
        *,
        source: str,
        method: str,
        uri: str,
        status: int,
        bytes_sent: int = 0,
        request_length: int = 0,
        referer: str = "",
    ) -> None:
        """Fold one nginx access-log entry into the per-(source, target)
        aggregate. A previously-unseen source IP gets a fresh
        :class:`_HttpClient` registered with the SessionRegistry.
        """
        client = self._clients.get(source)
        if client is None:
            client = _HttpClient(source, self._retention)
            self._clients[source] = client
            self._registry.add(client)
        client.record(
            method=method,
            uri=uri,
            status=status,
            bytes_sent=bytes_sent,
            request_length=request_length,
            referer=referer,
        )

    def expire(self) -> None:
        """Remove client views whose rows have all expired. Called from
        the tailer loop so it runs at the same cadence the rows expire.
        """
        for source, client in list(self._clients.items()):
            if client.is_empty():
                self._registry.remove(client)
                del self._clients[source]


def parse_line(line: str) -> dict[str, str | int] | None:
    """Parse one nginx access-log line. Returns ``None`` on malformed
    input rather than raising — the tailer skips and logs at DEBUG.
    """
    m = _LOG_RE.match(line)
    if m is None:
        return None
    referer = m["referer"]
    return {
        "source": m["addr"],
        "method": m["method"],
        "uri": m["uri"],
        "status": int(m["status"]),
        "bytes_sent": int(m["bytes_sent"]),
        "request_length": int(m["request_length"]),
        "referer": "" if referer == "-" else referer,
    }


async def tail_access_log(
    path: Path,
    tracker: HttpTrafficTracker,
    *,
    poll_interval: float = 0.5,
    max_bytes: int = HTTP_ACCESS_LOG_MAX_BYTES,
    done: asyncio.Event | None = None,
) -> None:
    """Tail a growing nginx access log into ``tracker``. Truncates the
    file in place once it exceeds ``max_bytes`` so /dev/shm doesn't bloat;
    nginx opens its log with O_APPEND so the next write lands at offset 0
    after truncate. Resilient to the file disappearing (returns to a
    "wait for it" state) and to external rotation (detects size <
    position and resets).
    """
    pos = 0
    while True:
        if done is not None and done.is_set():
            return
        try:
            stat_size = path.stat().st_size
            if stat_size < pos:
                pos = 0  # truncated/rotated externally
            if pos < stat_size:
                with open(path) as f:
                    f.seek(pos)
                    for line in f:
                        parsed = parse_line(line.rstrip("\n"))
                        if parsed is None:
                            continue
                        tracker.record(**parsed)  # type: ignore[arg-type]
                    pos = f.tell()
            if stat_size > max_bytes:
                try:
                    os.truncate(path, 0)
                    pos = 0
                    log.info(
                        "truncated access log at %d bytes (was %d)",
                        max_bytes,
                        stat_size,
                    )
                except OSError as exc:
                    log.warning("could not truncate access log: %s", exc)
            tracker.expire()
        except FileNotFoundError:
            pos = 0
        except Exception as exc:  # noqa: BLE001 - tailer must not die
            log.warning("access-log tailer error: %r", exc, exc_info=True)
        try:
            await asyncio.wait_for(
                done.wait() if done is not None else asyncio.Event().wait(),
                timeout=poll_interval,
            )
            return
        except asyncio.TimeoutError:
            continue
