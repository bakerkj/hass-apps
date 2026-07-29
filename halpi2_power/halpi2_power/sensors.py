# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Sensor definitions for the HALPI2 power controller.

Each entry maps an entity key (stable -- it appears in MQTT topics and
``unique_id``) to the field halpid actually returns from ``/values``.

The upstream README documents field names like ``dcin_voltage`` and
``power_state``. The daemon does not use those. Verified against halpid 5.1.1
on real hardware, ``/values`` returns::

    {"5v_output_enabled": true, "I_in": 0.246, "T_mcu": 324.53, "T_pcb": 311.77,
     "V_cap": 10.35, "V_in": 13.81, "daemon_version": "5.1.1",
     "device_id": "0011223344556677", "firmware_version": "3.3.1",
     "hardware_version": "N/A", "num_leds": 5, "state": "OperationalSolo",
     "watchdog_elapsed": 0.0, "watchdog_enabled": false, "watchdog_timeout": 0.0}

Note the temperatures are in **kelvin**, not celsius.
"""

from typing import Any

# Absolute zero offset, for the kelvin -> celsius conversion below.
KELVIN_OFFSET = 273.15


def kelvin_to_celsius(value: float) -> float:
    return value - KELVIN_OFFSET


SENSOR_DEFS: dict[str, dict[str, Any]] = {
    "dcin_voltage": {
        "source": "V_in",
        "name": "DC input voltage",
        "unit": "V",
        "device_class": "voltage",
        "state_class": "measurement",
        "suggested_display_precision": 2,
        "icon": "mdi:current-dc",
    },
    "supercap_voltage": {
        "source": "V_cap",
        "name": "Supercapacitor voltage",
        "unit": "V",
        "device_class": "voltage",
        "state_class": "measurement",
        "suggested_display_precision": 2,
        "icon": "mdi:battery-charging-high",
    },
    "input_current": {
        "source": "I_in",
        "name": "Input current",
        "unit": "A",
        "device_class": "current",
        "state_class": "measurement",
        "suggested_display_precision": 3,
        "icon": "mdi:current-ac",
    },
    "mcu_temperature": {
        "source": "T_mcu",
        "transform": kelvin_to_celsius,
        "name": "MCU temperature",
        "unit": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "suggested_display_precision": 1,
    },
    "pcb_temperature": {
        "source": "T_pcb",
        "transform": kelvin_to_celsius,
        "name": "PCB temperature",
        "unit": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "suggested_display_precision": 1,
    },
}

# The controller's operating mode, from the `state` field.
#
# This is the entity to watch. OperationalCoOp means the daemon is coordinating
# with the controller and a power outage will produce a clean shutdown.
# OperationalSolo means it is not -- the controller will hold the machine up on
# the supercaps and then cut power abruptly.
#
# Co-op is entered only when the hardware watchdog is armed; with
# watchdog_timeout_ms set to 0 the controller stays in Solo permanently.
#
# Deliberately a plain text sensor, NOT device_class=enum. halpid emits a wide
# set of state strings -- steady operating states, power-outage states, and
# startup/shutdown states -- and a fixed `options` list would mark any unlisted
# value invalid, blanking the entity exactly when an outage changes the state. A
# text sensor accepts whatever the daemon reports, this and future versions alike.
POWER_STATE_KEY = "power_state"
POWER_STATE_SOURCE = "state"

POWER_STATE_DEF: dict[str, Any] = {
    "source": POWER_STATE_SOURCE,
    "name": "Power state",
    "icon": "mdi:power-plug",
}

# Reported as device metadata rather than as entities.
DEVICE_INFO_KEYS = ("device_id", "firmware_version", "hardware_version")


# Derived entities: not fields in /values, but computed by the publisher from
# the observed state plus the configured power outage thresholds.
#
# halpid tracks its own power outage timer internally and does not publish it, so
# these are the publisher's independent measurement of the same thing. They will
# agree closely but are not the daemon's authoritative countdown.
POWER_OUTAGE_ELAPSED_KEY = "power_outage_elapsed"
SHUTDOWN_IN_KEY = "shutdown_in"

DERIVED_DEFS: dict[str, dict[str, Any]] = {
    POWER_OUTAGE_ELAPSED_KEY: {
        "name": "Power outage elapsed",
        "unit": "s",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": "mdi:timer-sand",
    },
    SHUTDOWN_IN_KEY: {
        "name": "Shutdown in",
        "unit": "s",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": "mdi:timer-alert-outline",
    },
}


def extract(values: dict[str, Any], definition: dict[str, Any]) -> Any:
    """Pull a definition's value out of a /values payload, applying any transform.

    Returns None when the source field is absent or null, so callers can skip
    publishing rather than emitting a bogus state.
    """
    raw = values.get(definition["source"])
    if raw is None:
        return None
    transform = definition.get("transform")
    if transform is not None:
        try:
            return transform(float(raw))
        except TypeError, ValueError:
            return None
    return raw
