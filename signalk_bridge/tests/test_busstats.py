# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for NMEA 2000 bus-health entities."""

from datetime import UTC, datetime, timedelta

from signalk_bridge.busstats import BusStats

from signalk_bridge import busstats


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def test_count_devices_total_and_active() -> None:
    now = datetime.now(UTC)
    fresh = _iso(now - timedelta(seconds=5))
    stale = _iso(now - timedelta(seconds=600))
    sources = {
        "defaults": {},  # not an n2k provider
        "n2k-can0": {
            "0": {"n2k": {"pgns": {"127508": fresh}}},  # active
            "35": {"n2k": {"pgns": {"129025": stale}}},  # present, inactive
            "kip": {},  # non-numeric addr, ignored
        },
    }
    total, active = busstats._count_devices(sources)
    assert total == 2
    assert active == 1


def test_count_devices_detects_by_structure_not_label() -> None:
    # A provider named for its interface (not "n2k...") still has its N2K
    # devices counted, identified by the n2k substructure. A non-N2K provider
    # (no n2k substructure) contributes nothing.
    now = datetime.now(UTC)
    fresh = _iso(now - timedelta(seconds=5))
    sources = {
        "can0": {"3": {"n2k": {"pgns": {"127488": fresh}}}},
        "gps.serial": {"7": {}},  # no n2k substructure -> not a bus device
    }
    total, active = busstats._count_devices(sources)
    assert total == 1
    assert active == 1


def test_sample_reports_frame_rate_from_counter_delta(monkeypatch) -> None:
    # Fake sysfs + a controllable clock so we can drive an exact delta.
    counters = {"rx_packets": 1000, "rx_errors": 2, "rx_dropped": 0}
    clock = {"t": 100.0}
    monkeypatch.setattr(busstats, "_read_int", lambda p: counters[p.rsplit("/", 1)[-1]])
    monkeypatch.setattr(busstats, "_read_str", lambda p: "up")
    monkeypatch.setattr(busstats.time, "monotonic", lambda: clock["t"])

    bus = BusStats()
    first = bus.sample(None)
    # No prior sample yet -> no rate on the first pass.
    assert "n2k_bus_frame_rate" not in first
    assert first["n2k_bus_status"]["state"] == "ON"

    counters["rx_packets"] = 1250  # +250 frames
    clock["t"] = 110.0  # over 10 s -> 25 frame/s
    second = bus.sample(None)
    assert second["n2k_bus_frame_rate"]["state"] == "25.0"
    assert second["n2k_bus_frame_rate"]["unit"] == "frame/s"
    assert second["n2k_bus_rx_errors"]["state"] == "2"


def test_sample_status_off_when_link_down(monkeypatch) -> None:
    monkeypatch.setattr(busstats, "_read_int", lambda p: 0)
    monkeypatch.setattr(busstats, "_read_str", lambda p: "down")
    monkeypatch.setattr(busstats.time, "monotonic", lambda: 5.0)
    out = BusStats().sample(None)
    assert out["n2k_bus_status"]["state"] == "OFF"
    assert out["n2k_bus_status"]["component"] == "binary_sensor"


def test_devices_entities_present_when_sources_given(monkeypatch) -> None:
    monkeypatch.setattr(busstats, "_read_int", lambda p: 100)
    monkeypatch.setattr(busstats, "_read_str", lambda p: "up")
    monkeypatch.setattr(busstats.time, "monotonic", lambda: 1.0)
    sources = {"n2k-can0": {"0": {"n2k": {"pgns": {}}}}}
    out = BusStats().sample(sources)
    assert out["n2k_bus_devices"]["state"] == "1"
    assert "n2k_bus_active_devices" in out
