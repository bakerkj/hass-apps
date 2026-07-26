# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: run the real signalk_bridge add-on container against a stub
Signal K server and a real Mosquitto broker, and assert the discovery configs and
converted state values it publishes on the wire. This exercises the shipped
artifact -- the image, its apk paho, run.sh -- not a stand-in process.
"""

import json
from collections.abc import Callable
from typing import Any

import pytest

pytestmark = pytest.mark.e2e


def test_bridge_container_publishes_to_a_real_broker(
    bridge_container: Any,
    subscriber: Any,
    wait_for: Callable[..., bool],
) -> None:
    cfg = "homeassistant/sensor/signalk/navigation_speedoverground/config"
    sog = "signalk/navigation_speedoverground/state"
    temp = "signalk/environment_water_temperature/state"

    arrived = wait_for(
        lambda: (
            subscriber.get("signalk/availability") == "online"
            and subscriber.get(cfg) is not None
            and subscriber.get(sog) is not None
            and subscriber.get(temp) is not None
        ),
        40,
    )
    if not arrived:
        pytest.fail(
            "expected messages never arrived; "
            f"saw {sorted(subscriber.snapshot())}\n"
            f"container running={bridge_container.running()}; logs:\n"
            f"{bridge_container.logs()[-3000:]}"
        )

    # discovery payload is well-formed
    disc_raw = subscriber.get(cfg)
    assert disc_raw is not None
    disc = json.loads(disc_raw)
    assert disc["unit_of_measurement"] == "m/s"
    assert disc["device_class"] == "speed"
    assert disc["device"]["identifiers"] == ["signalk_navigation"]
    assert disc["state_topic"] == sog

    # converted values reached the wire
    assert subscriber.get(sog) == "3.086"
    assert subscriber.get(temp) == "15"
    assert (
        subscriber.get("signalk/electrical_batteries_house_stateofcharge/state") == "87"
    )
    cog_raw = subscriber.get("signalk/navigation_courseovergroundtrue/state")
    assert cog_raw is not None
    assert abs(float(cog_raw) - 90.0) < 0.01

    # position -> device_tracker: lat/lon in attributes, no state topic (GPS mode)
    pos_cfg = "homeassistant/device_tracker/signalk/navigation_position/config"
    pos_raw = subscriber.get(pos_cfg)
    assert pos_raw is not None
    pos = json.loads(pos_raw)
    assert pos["source_type"] == "gps"
    assert "state_topic" not in pos
    attrs_raw = subscriber.get("signalk/navigation_position/attributes")
    assert attrs_raw is not None
    attrs = json.loads(attrs_raw)
    assert attrs["latitude"] == 40.0
    assert attrs["longitude"] == -70.0

    # a graceful container stop flips availability to "offline" (SIGTERM path)
    bridge_container.graceful_stop()
    assert wait_for(lambda: subscriber.get("signalk/availability") == "offline", 15), (
        "availability did not go offline on graceful container stop"
    )
