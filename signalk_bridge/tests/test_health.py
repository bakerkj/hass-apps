# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for per-device N2K health entities."""

from datetime import UTC, datetime

from signalk_bridge.health import health_entities


def _now() -> datetime:
    return datetime(2026, 8, 5, 4, 0, 0, tzinfo=UTC)


def _sources_with_devices() -> dict:
    return {
        "n2k-can0": {
            "86": {
                "n2k": {
                    "canName": "c032abcd12345678",
                    "modelId": "YDNG-03",
                    "manufacturerCode": "Yacht Devices",
                    "deviceClass": "Internetwork device",
                    "pgns": {
                        "60928": "2026-08-05T03:59:52Z",
                        "126996": "2026-08-05T03:59:37Z",
                    },
                },
            },
            "43": {
                "n2k": {
                    "canName": "c064a0009999aaaa",
                    "modelId": "E32158",
                    "manufacturerCode": "Raymarine",
                    "deviceClass": "Communication",
                    "pgns": {
                        "60928": "2026-08-04T21:24:49Z",  # long-dead
                        "129038": "2026-08-04T21:24:49Z",
                    },
                },
            },
        },
    }


def test_online_device_reports_on() -> None:
    ents = health_entities(_sources_with_devices(), _now(), stale_after_seconds=90)
    key = "n2k_dev_c032abcd12345678"
    assert key in ents
    e = ents[key]
    assert e["state"] == "ON"
    assert e["component"] == "binary_sensor"
    assert e["device_class"] == "connectivity"
    assert e["group_id"] == "n2k.fleet"
    assert e["attributes"]["model"] == "YDNG-03"
    assert e["attributes"]["manufacturer"] == "Yacht Devices"
    assert e["attributes"]["address"] == "86"
    assert "last_seen_age_seconds" in e["attributes"]


def test_stale_device_reports_off() -> None:
    ents = health_entities(_sources_with_devices(), _now(), stale_after_seconds=90)
    key = "n2k_dev_c064a0009999aaaa"
    assert key in ents
    e = ents[key]
    assert e["state"] == "OFF"
    assert e["attributes"]["last_seen_age_seconds"] > 90


def test_device_without_canname_skipped() -> None:
    sources = {"n2k-can0": {"5": {"n2k": {"pgns": {}}}}}  # no canName
    assert health_entities(sources, _now(), stale_after_seconds=90) == {}


def test_non_n2k_entries_skipped() -> None:
    sources = {"n2k-can0": {"label": "N2K Bus", "type": "canbus", "5": "not-a-dict"}}
    assert health_entities(sources, _now(), stale_after_seconds=90) == {}


def test_empty_sources_produces_no_entities() -> None:
    assert health_entities({}, _now(), stale_after_seconds=90) == {}


def test_display_name_prefers_model() -> None:
    ents = health_entities(_sources_with_devices(), _now(), stale_after_seconds=90)
    names = {e["name"] for e in ents.values()}
    assert "Yacht Devices YDNG-03" in names
    assert "Raymarine E32158" in names


def test_display_name_fallback_when_no_model() -> None:
    sources = {
        "n2k-can0": {
            "5": {
                "n2k": {
                    "canName": "c0aabbccddeeff11",
                    "manufacturerCode": "Actisense",
                    "pgns": {"60928": "2026-08-05T03:59:00Z"},
                }
            }
        }
    }
    ents = health_entities(sources, _now(), stale_after_seconds=90)
    e = next(iter(ents.values()))
    assert e["name"].startswith("Actisense device")


def test_device_with_no_pgns_reports_off() -> None:
    """A device that announced its address but never emitted a PGN reads as
    offline -- there's no positive evidence it's actually alive."""
    sources = {
        "n2k-can0": {
            "7": {"n2k": {"canName": "c0deadbeefabcdef", "pgns": {}}},
        }
    }
    ents = health_entities(sources, _now(), stale_after_seconds=90)
    e = next(iter(ents.values()))
    assert e["state"] == "OFF"
