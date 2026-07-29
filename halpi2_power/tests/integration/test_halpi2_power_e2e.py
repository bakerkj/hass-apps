# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: run the real halpi2_power add-on container (s6 + shipped
python/paho, with a fake halpid serving the /values socket) against a real
Mosquitto broker, and assert the discovery configs and converted state values
the publisher puts on the wire. Exercises the shipped artifact, not a stand-in
process -- only halpid itself (hardware-bound) is faked.
"""

import json
from collections.abc import Callable
from typing import Any

import pytest

pytestmark = pytest.mark.e2e

# device_id from the fake halpid's /values sample; discovery configs key off it.
DEVICE_ID = "0011223344556677"


def test_container_publishes_power_telemetry_to_a_real_broker(
    addon_container: Any,
    subscriber: Any,
    wait_for: Callable[..., bool],
) -> None:
    cfg = f"homeassistant/sensor/{DEVICE_ID}/dcin_voltage/config"
    dcin = "halpi2_power/dcin_voltage/state"
    mcu = "halpi2_power/mcu_temperature/state"
    pstate = "halpi2_power/power_state/state"

    arrived = wait_for(
        lambda: (
            subscriber.get("halpi2_power/availability") == "online"
            and subscriber.get(cfg) is not None
            and subscriber.get(dcin) is not None
            and subscriber.get(pstate) is not None
        ),
        60,
    )
    if not arrived:
        pytest.fail(
            "expected messages never arrived; "
            f"saw {sorted(subscriber.snapshot())}\n"
            f"container running={addon_container.running()}; logs:\n"
            f"{addon_container.logs()[-3000:]}"
        )

    # discovery payload is well-formed and points at the right device + state
    disc = json.loads(subscriber.get(cfg))
    assert disc["device_class"] == "voltage"
    assert disc["unit_of_measurement"] == "V"
    assert disc["state_topic"] == dcin
    assert disc["device"]["identifiers"] == [DEVICE_ID]

    # raw pass-through: V_in reaches the wire unconverted (4 dp)
    assert subscriber.get(dcin) == "13.8135"

    # power_state distinguishes Co-op from Solo -- the point of the add-on -- and
    # comes straight from halpid's `state` field. It is a plain text sensor, NOT
    # device_class=enum: an enum's fixed options list would blank the entity when
    # halpid reports a power-outage or startup/shutdown state.
    assert subscriber.get(pstate) == "OperationalCoOp"
    pstate_cfg = json.loads(
        subscriber.get(f"homeassistant/sensor/{DEVICE_ID}/power_state/config")
    )
    assert "device_class" not in pstate_cfg
    assert "options" not in pstate_cfg

    # kelvin -> celsius conversion happened on the way out (324.54 K -> ~51.39 C)
    mcu_raw = subscriber.get(mcu)
    assert mcu_raw is not None
    assert abs(float(mcu_raw) - 51.386) < 0.01

    # a graceful container stop flips availability to "offline" (SIGTERM path
    # through s6 -> the publisher's finally-block offline publish)
    addon_container.graceful_stop()
    assert wait_for(
        lambda: subscriber.get("halpi2_power/availability") == "offline", 15
    ), "availability did not go offline on graceful container stop"
