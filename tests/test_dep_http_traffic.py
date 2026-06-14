# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for the HTTP-traffic tracker that powers the status-UI's
per-client HTTP panes. Two concerns: parsing nginx log lines, and
aggregating them into per-source clients with their per-target rows.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dashboard_entity_proxy.http_traffic import (
    HttpTrafficTracker,
    classify_target,
    parse_line,
    tail_access_log,
)


class _FakeRegistry:
    """Minimal SessionRegistry stand-in: tracks add/remove calls so tests
    can assert tracker lifecycle without booting the real registry.
    """

    def __init__(self) -> None:
        self.conns: list = []

    def add(self, conn) -> None:
        self.conns.append(conn)

    def remove(self, conn) -> None:
        if conn in self.conns:
            self.conns.remove(conn)


# ---- parse_line ----------------------------------------------------------


def test_parse_line_typical_get():
    line = (
        '192.168.1.10 "GET /api/states/sensor.foo" 200 1234 567 '
        '0.012 "http://hass:8126/lovelace/0"'
    )
    parsed = parse_line(line)
    assert parsed == {
        "source": "192.168.1.10",
        "method": "GET",
        "uri": "/api/states/sensor.foo",
        "status": 200,
        "bytes_sent": 1234,
        "request_length": 567,
        "referer": "http://hass:8126/lovelace/0",
    }


def test_parse_line_dash_referer_becomes_empty():
    line = '10.0.0.1 "GET /local/img.png" 200 500 100 0.001 "-"'
    parsed = parse_line(line)
    assert parsed is not None
    assert parsed["referer"] == ""
    assert parsed["bytes_sent"] == 500
    assert parsed["request_length"] == 100


def test_parse_line_malformed_returns_none():
    assert parse_line("garbage") is None
    assert parse_line("") is None


# ---- classify_target -----------------------------------------------------


@pytest.mark.parametrize(
    "uri,referer,want",
    [
        (
            "/f1c878cb_adsb-multi-portal-feeder/",
            "",
            "addon: f1c878cb_adsb-multi-portal-feeder",
        ),
        (
            "/api/hassio_ingress/abc/index.html",
            "http://hass:8126/f1c878cb_adsb-multi-portal-feeder/",
            "addon: f1c878cb_adsb-multi-portal-feeder",
        ),
        ("/api/hassio_ingress/abc/anything", "", "addon-ingress"),
        ("/api/states/sensor.foo", "", "ha-rest"),
        ("/frontend_latest/app.js", "", "ha-frontend"),
        ("/static/icons/foo.png", "", "ha-frontend"),
        ("/local/community/foo.js", "", "local-assets"),
        ("/auth/token", "", "auth"),
        ("/lovelace-skylights/0", "", "dashboard: lovelace-skylights"),
        ("/dashboard-database/1", "", "dashboard: dashboard-database"),
        ("/", "", "/"),
    ],
)
def test_classify_target_buckets(uri, referer, want):
    assert classify_target(uri, referer) == want


# ---- HttpTrafficTracker --------------------------------------------------


def test_tracker_registers_one_client_per_source():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(source="1.2.3.4", method="GET", uri="/api/x", status=200)
    t.record(source="1.2.3.4", method="GET", uri="/api/y", status=200)
    t.record(source="1.2.3.5", method="GET", uri="/api/x", status=200)
    # Two distinct sources → two registry entries.
    assert len(reg.conns) == 2
    sources = {c.source for c in reg.conns}
    assert sources == {"1.2.3.4", "1.2.3.5"}


def test_client_groups_requests_by_target():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(source="1.2.3.4", method="GET", uri="/api/states", status=200)
    t.record(source="1.2.3.4", method="GET", uri="/api/services", status=200)
    t.record(source="1.2.3.4", method="GET", uri="/local/img.png", status=200)
    client = reg.conns[0]
    snap = client.status()
    targets = {r["target"] for r in snap["rows"]}
    # /api/states + /api/services both classify to "ha-rest", local-assets is separate.
    assert targets == {"ha-rest", "local-assets"}


def test_client_status_sorts_rows_most_recent_first():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(source="1.2.3.4", method="GET", uri="/api/a", status=200)
    t.record(source="1.2.3.4", method="GET", uri="/local/img.png", status=200)
    t.record(source="1.2.3.4", method="GET", uri="/api/b", status=200)  # latest
    client = reg.conns[0]
    snap = client.status()
    assert snap["rows"][0]["target"] == "ha-rest"
    assert snap["rows"][0]["last_seen"] >= snap["rows"][-1]["last_seen"]


def test_client_aggregates_rx_tx_bytes():
    """Per-target row rx/tx bytes accumulate; client-level totals
    aggregate across all rows.
    """
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(
        source="1.2.3.4",
        method="GET",
        uri="/api/states",
        status=200,
        bytes_sent=1000,
        request_length=300,
    )
    t.record(
        source="1.2.3.4",
        method="POST",
        uri="/api/services/foo",
        status=200,
        bytes_sent=200,
        request_length=400,
    )
    t.record(
        source="1.2.3.4",
        method="GET",
        uri="/local/img.png",
        status=200,
        bytes_sent=5000,
        request_length=100,
    )
    snap = reg.conns[0].status()
    # Client totals.
    assert snap["rx_bytes"] == 800
    assert snap["tx_bytes"] == 6200
    assert snap["request_count"] == 3
    # Row-level totals.
    by_target = {r["target"]: r for r in snap["rows"]}
    assert by_target["ha-rest"]["rx_bytes"] == 700
    assert by_target["ha-rest"]["tx_bytes"] == 1200
    assert by_target["local-assets"]["rx_bytes"] == 100
    assert by_target["local-assets"]["tx_bytes"] == 5000


def test_client_status_last_seen_uses_last_activity_after_row_expiry():
    """When the status snapshot races with row-retention expiry (status() called
    after _prune() has dropped every row but before tracker.expire() removes
    the client), last_seen should reflect the most recent observed activity,
    not the client's connection timestamp.
    """
    from datetime import timedelta

    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(source="1.2.3.4", method="GET", uri="/api/states", status=200)
    client = reg.conns[0]
    # Backdate the row's last_seen so _prune() drops it on the next status() call.
    row = next(iter(client._rows.values()))
    row.last_seen = row.last_seen - timedelta(seconds=120)
    snap = client.status()
    assert snap["row_count"] == 0
    # last_seen reflects the recorded activity, not _connected_at.
    assert snap["last_seen"] == client._last_activity_at.isoformat()
    assert snap["last_seen"] != snap["connected_at"]


def test_expire_removes_client_when_all_rows_silent():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=0.001)
    t.record(source="1.2.3.4", method="GET", uri="/api/x", status=200)
    assert len(reg.conns) == 1
    # Age the only row past retention.
    client = reg.conns[0]
    row = next(iter(client._rows.values()))
    row.last_seen = datetime.now(timezone.utc) - timedelta(seconds=10)
    t.expire()
    assert reg.conns == []


def test_client_status_summary_caps_rows_at_50():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    for i in range(75):
        # Different uri prefixes so each gets its own target bucket.
        t.record(source="1.2.3.4", method="GET", uri=f"/bucket-{i}/x", status=200)
    snap = reg.conns[0].status(detail="summary")
    assert snap["row_count"] == 75
    assert len(snap["rows"]) == 50
    full = reg.conns[0].status(detail="full")
    assert len(full["rows"]) == 75


def test_client_classifies_ingress_via_referer():
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    t.record(
        source="1.2.3.4",
        method="GET",
        uri="/api/hassio_ingress/tok123/asset.js",
        status=200,
        referer="http://hass:8126/f1c878cb_adsb-multi-portal-feeder/",
    )
    snap = reg.conns[0].status()
    assert snap["rows"][0]["target"] == "addon: f1c878cb_adsb-multi-portal-feeder"


# ---- tail_access_log -----------------------------------------------------


@pytest.mark.asyncio
async def test_tail_reads_appended_lines(tmp_path: Path):
    log_path = tmp_path / "access.log"
    log_path.write_text("")
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    done = asyncio.Event()
    task = asyncio.create_task(
        tail_access_log(log_path, t, poll_interval=0.05, done=done)
    )
    try:
        await asyncio.sleep(0.1)
        with log_path.open("a") as f:
            f.write('1.2.3.4 "GET /api/states" 200 800 250 0.01 "-"\n')
            f.write('1.2.3.4 "GET /api/x" 500 50 200 0.02 "-"\n')
        for _ in range(20):
            if reg.conns and reg.conns[0]._rows:
                break
            await asyncio.sleep(0.05)
    finally:
        done.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert len(reg.conns) == 1
    snap = reg.conns[0].status()
    # Both lines classified to "ha-rest" — one row, count=2, tx=850, rx=450.
    row = snap["rows"][0]
    assert row["count"] == 2
    assert row["tx_bytes"] == 850
    assert row["rx_bytes"] == 450


@pytest.mark.asyncio
async def test_tail_reads_lines_written_after_truncate(tmp_path: Path):
    """After the file shrinks (rotation or auto-truncate), the tailer must
    pick up the next-written line from offset 0 — otherwise auto-truncate
    would silently lose every subsequent request.
    """
    log_path = tmp_path / "access.log"
    log_path.write_text("")
    reg = _FakeRegistry()
    t = HttpTrafficTracker(reg, retention_seconds=60.0)
    done = asyncio.Event()
    task = asyncio.create_task(
        tail_access_log(log_path, t, poll_interval=0.02, done=done)
    )
    try:
        with log_path.open("a") as f:
            f.write('1.1.1.1 "GET /api/x" 200 100 50 0.01 "-"\n')
        for _ in range(50):
            if any(c.source == "1.1.1.1" for c in reg.conns):
                break
            await asyncio.sleep(0.02)
        assert any(c.source == "1.1.1.1" for c in reg.conns), "first line not read"

        os.truncate(log_path, 0)
        await asyncio.sleep(0.05)

        with log_path.open("a") as f:
            f.write('2.2.2.2 "GET /api/y" 200 100 50 0.01 "-"\n')
        for _ in range(100):
            if any(c.source == "2.2.2.2" for c in reg.conns):
                break
            await asyncio.sleep(0.02)
    finally:
        done.set()
        await asyncio.wait_for(task, timeout=2.0)

    sources = {c.source for c in reg.conns}
    assert "2.2.2.2" in sources, (
        f"post-truncate line not picked up; saw sources {sources}"
    )
