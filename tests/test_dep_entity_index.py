# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

from dashboard_entity_proxy import entity_index


def test_build_and_lookups():
    entities = [
        {
            "entity_id": "light.kitchen",
            "device_id": "dev1",
            "platform": "hue",
            "area_id": "kitchen",
        },
        {
            "entity_id": "sensor.kitchen_temp",
            "device_id": "dev1",
            "platform": "hue",
        },  # area via device
        {"entity_id": "switch.fan", "device_id": "dev2", "platform": "mqtt"},
        {"entity_id": "sensor.cloud", "platform": "weather"},  # no device, no area
        {"entity_id": ""},  # ignored
    ]
    devices = [
        {"id": "dev1", "area_id": "kitchen"},
        {"id": "dev2", "area_id": "garage"},
    ]
    idx = entity_index.build(entities, devices)

    assert idx.device("dev1") == ["light.kitchen", "sensor.kitchen_temp"]
    assert idx.integration("hue") == ["light.kitchen", "sensor.kitchen_temp"]
    assert idx.integration("mqtt") == ["switch.fan"]
    assert idx.area("kitchen") == ["light.kitchen", "sensor.kitchen_temp"]
    assert idx.area("garage") == ["switch.fan"]
    assert idx.device("unknown") == []
    assert idx.platforms(["hue", "mqtt"]) == [
        "light.kitchen",
        "sensor.kitchen_temp",
        "switch.fan",
    ]


def test_platforms_dedupes_and_handles_unknown_domains():
    # Two domains sharing an entity id (rare but legal: same entity_id
    # could theoretically appear under multiple platforms in a degenerate
    # registry) must not duplicate in the output, and unknown domains
    # must be silently skipped.
    idx = entity_index.EntityIndex()
    idx.by_platform["a"] = ["x.1", "x.3"]
    idx.by_platform["b"] = ["x.2", "x.3"]
    assert idx.platforms(["a", "b", "missing"]) == ["x.1", "x.2", "x.3"]
    assert idx.platforms([]) == []
    assert idx.platforms(["missing"]) == []


def test_platforms_does_not_resort_on_every_call():
    # The sibling lookups (device/integration/area) return ids sorted at
    # build time; platforms() should do the same — i.e. its output must
    # be order-stable regardless of the order of input domains.
    idx = entity_index.EntityIndex()
    idx.by_platform["p1"] = ["e.a", "e.c"]
    idx.by_platform["p2"] = ["e.b", "e.d"]
    assert idx.platforms(["p1", "p2"]) == ["e.a", "e.b", "e.c", "e.d"]
    assert idx.platforms(["p2", "p1"]) == ["e.a", "e.b", "e.c", "e.d"]
