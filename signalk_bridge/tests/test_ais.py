# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the AIS target registry."""

from typing import Any

from signalk_bridge.ais import AISRegistry


def _sk_target(
    lat: float | None = None,
    lon: float | None = None,
    name: str | None = None,
    sog: float | None = None,
    ship_type: Any = None,
) -> dict[str, Any]:
    """Build a Signal K-shaped per-context tree for one AIS target."""
    tree: dict[str, Any] = {}
    if lat is not None and lon is not None:
        tree["navigation"] = {
            "position": {
                "value": {"latitude": lat, "longitude": lon},
                "timestamp": "2026-08-05T04:00:00.000Z",
            }
        }
    if sog is not None:
        tree.setdefault("navigation", {})["speedOverGround"] = {"value": sog}
    if name is not None:
        tree["name"] = {"value": name}
    if ship_type is not None:
        tree["design"] = {"aisShipType": {"value": {"id": 60, "name": ship_type}}}
    return tree


def test_ingest_produces_device_tracker_entity() -> None:
    reg = AISRegistry(expire_seconds=900)
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0, name="TESTVSL")},
        now_monotonic=100.0,
    )
    ents = reg.entities()
    assert "ais_367674550" in ents
    e = ents["ais_367674550"]
    assert e["component"] == "device_tracker"
    assert e["name"] == "TESTVSL"
    assert e["attributes"]["latitude"] == 42.0
    assert e["attributes"]["longitude"] == -71.0
    assert e["attributes"]["mmsi"] == "367674550"


def test_target_without_position_is_not_rendered() -> None:
    reg = AISRegistry(expire_seconds=900)
    # Static-only delta (rare in the wild, but SK static PGN arrives before
    # the first position PGN sometimes).
    reg.ingest(
        {"367674550": _sk_target(name="STATICONLY")},
        now_monotonic=100.0,
    )
    assert reg.entities() == {}
    # But the registry knows about it.
    assert "367674550" in reg.mmsis()


def test_expire_drops_targets_past_window() -> None:
    reg = AISRegistry(expire_seconds=60)
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0)},
        now_monotonic=100.0,
    )
    # 30s later, still alive
    assert reg.expire(now_monotonic=130.0) == []
    assert "367674550" in reg.mmsis()
    # 90s later, expired
    dropped = reg.expire(now_monotonic=200.0)
    assert dropped == ["367674550"]
    assert "367674550" not in reg.mmsis()


def test_always_retain_survives_expiry() -> None:
    reg = AISRegistry(expire_seconds=60, always_retain=frozenset({"367674550"}))
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0)},
        now_monotonic=100.0,
    )
    # Well past window; sticky targets never expire.
    dropped = reg.expire(now_monotonic=999999.0)
    assert dropped == []
    assert "367674550" in reg.mmsis()
    # The entity still renders with the last-known position.
    assert "ais_367674550" in reg.entities()


def test_max_targets_drops_oldest_non_sticky() -> None:
    reg = AISRegistry(
        expire_seconds=99999,
        max_targets=2,
        always_retain=frozenset({"AAA"}),
    )
    reg.ingest({"AAA": _sk_target(lat=1, lon=1)}, now_monotonic=100.0)
    reg.ingest({"OLDEST": _sk_target(lat=2, lon=2)}, now_monotonic=100.0)
    reg.ingest({"MIDDLE": _sk_target(lat=3, lon=3)}, now_monotonic=200.0)
    reg.ingest({"NEWEST": _sk_target(lat=4, lon=4)}, now_monotonic=300.0)
    dropped = reg.expire(now_monotonic=300.0)
    # AAA is sticky (kept), plus one slot left for freshest non-sticky (NEWEST).
    kept = set(reg.mmsis())
    assert "AAA" in kept
    assert "NEWEST" in kept
    assert "OLDEST" in dropped
    assert "MIDDLE" in dropped


def test_max_targets_ties_break_deterministically_by_insertion_order() -> None:
    """A batched WS message applies multiple deltas at the same monotonic
    time; when the sort key ties, the surviving vs dropped set must still
    be well-defined so cap enforcement isn't order-random."""
    reg = AISRegistry(expire_seconds=99999, max_targets=1)
    # All three ingested at the same monotonic time -- Python's sort is
    # stable, and dict insertion order preserves the ingest sequence, so
    # the pre-sort iterator gives us FIRST, SECOND, THIRD in that order.
    # sorted(..., reverse=True) with a tied key preserves that order, so
    # the first slot goes to FIRST and the other two get dropped.
    reg.ingest({"FIRST": _sk_target(lat=1, lon=1)}, now_monotonic=100.0)
    reg.ingest({"SECOND": _sk_target(lat=2, lon=2)}, now_monotonic=100.0)
    reg.ingest({"THIRD": _sk_target(lat=3, lon=3)}, now_monotonic=100.0)
    dropped = reg.expire(now_monotonic=100.0)
    kept = set(reg.mmsis())
    assert kept == {"FIRST"}
    assert set(dropped) == {"SECOND", "THIRD"}


def test_inventory_entity_reports_count_and_summaries() -> None:
    reg = AISRegistry(expire_seconds=900)
    reg.ingest(
        {
            "367674550": _sk_target(lat=42.0, lon=-71.0, name="ALPHA", sog=3.0),
            "338216333": _sk_target(lat=42.1, lon=-71.1, name="BRAVO", sog=5.0),
        },
        now_monotonic=100.0,
    )
    inv = reg.inventory_entity()
    assert inv["component"] == "sensor"
    assert inv["state"] == "2"
    mmsis_in_attrs = {row["mmsi"] for row in inv["attributes"]["targets"]}
    assert mmsis_in_attrs == {"367674550", "338216333"}
    assert inv["attributes"]["truncated"] is False
    assert inv["attributes"]["total_tracked"] == 2


def test_inventory_truncates_when_over_budget() -> None:
    reg = AISRegistry(expire_seconds=900)
    # Enough targets with long names to exceed a small byte budget.
    reg.ingest(
        {
            str(600000000 + i): _sk_target(
                lat=42.0, lon=-71.0, name="VESSEL_WITH_LONG_NAME_" * 3
            )
            for i in range(200)
        },
        now_monotonic=100.0,
    )
    inv = reg.inventory_entity(max_attr_bytes=2000)
    assert inv["state"] == "200"  # count is exact
    assert inv["attributes"]["truncated"] is True
    # Row count is bounded by the byte budget.
    assert len(inv["attributes"]["targets"]) < 200
    assert inv["attributes"]["total_tracked"] == 200


def test_reingesting_a_target_refreshes_last_seen() -> None:
    reg = AISRegistry(expire_seconds=60)
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0)},
        now_monotonic=100.0,
    )
    # Would be expired at 200s...
    reg.ingest(
        {"367674550": _sk_target(lat=42.1, lon=-71.1)},
        now_monotonic=180.0,
    )
    # ...but the second ingest reset the clock.
    assert reg.expire(now_monotonic=200.0) == []
    # And the position updated to the latest.
    e = reg.entities()["ais_367674550"]
    assert e["attributes"]["latitude"] == 42.1


def test_silent_target_expires_when_dirty_set_omits_it() -> None:
    """The production callsite in app.py always passes the full accumulated
    ais_snapshot every tick (that's the only thing WSSubscriber exposes),
    but only MMSIs in ``dirty`` actually saw a delta. Without threading the
    dirty set through, a silent target's clock would be re-zeroed every
    tick just by being in the snapshot -- and expire() would never fire.
    """
    reg = AISRegistry(expire_seconds=60)
    # Tick 1: fresh delta arrives; MMSI is in dirty set.
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0)},
        now_monotonic=100.0,
        dirty={"367674550"},
    )
    # Tick 2: target went silent; snapshot still contains it (WSSubscriber
    # never prunes on its own) but the dirty set is empty. Clock must NOT
    # advance for this MMSI.
    reg.ingest(
        {"367674550": _sk_target(lat=42.0, lon=-71.0)},
        now_monotonic=130.0,
        dirty=set(),
    )
    # Past the 60s window -- should expire even though ingest kept seeing
    # the target every tick.
    dropped = reg.expire(now_monotonic=200.0)
    assert dropped == ["367674550"]


def test_attribute_names_are_spelled_out_with_converted_units() -> None:
    """Attribute keys drop the ``sog``/``cog``/``hdg`` marine abbreviations,
    and the values are converted at the edge -- knots for speed, degrees for
    bearings -- so a downstream template can read the bearing without
    remembering it's Signal K radians."""
    import math

    reg = AISRegistry(expire_seconds=900)
    reg.ingest(
        {
            "367674550": _sk_target(
                lat=42.0,
                lon=-71.0,
                # 5.0 m/s -> 9.72 kn; pi/2 rad -> 90.0 deg
                sog=5.0,
            )
        },
        now_monotonic=100.0,
    )
    # Splice in course + heading via a follow-up ingest since the helper
    # doesn't expose them directly.
    reg._targets["367674550"].course_over_ground_rad = math.pi / 2
    reg._targets["367674550"].heading_rad = math.pi
    attrs = reg.entities()["ais_367674550"]["attributes"]
    assert attrs["speed_over_ground"] == round(5.0 * 1.94384, 2)
    assert attrs["course_over_ground"] == 90.0
    assert attrs["heading"] == 180.0
    # Inventory summary follows the same convention.
    inv = reg.inventory_entity()
    row = inv["attributes"]["targets"][0]
    assert "speed_over_ground" in row
    assert "course_over_ground" in row
    assert row["heading"] == 180.0
    assert "latitude" in row and "longitude" in row


def test_sticky_flag_reflected_in_attributes() -> None:
    reg = AISRegistry(expire_seconds=900, always_retain=frozenset({"367674550"}))
    reg.ingest(
        {
            "367674550": _sk_target(lat=42.0, lon=-71.0),
            "338216333": _sk_target(lat=42.1, lon=-71.1),
        },
        now_monotonic=100.0,
    )
    ents = reg.entities()
    assert ents["ais_367674550"]["attributes"]["sticky"] is True
    assert ents["ais_338216333"]["attributes"]["sticky"] is False
