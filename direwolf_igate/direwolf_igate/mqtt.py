# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health tracking, discovery payloads, sensor definitions."""

from typing import Any, NamedTuple


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0
        # Watchdog-only mirror: published ages stay wall-clock for HA, but a
        # duration-based decision must survive an NTP step.
        self.last_disconnect_monotonic: float = 0.0


def heartbeat_payload(
    *,
    now: float,
    health: MqttHealth,
    last_output: float,
) -> dict[str, Any]:
    """Diagnostic heartbeat. Ages are wall-clock; ``None`` means "not yet",
    which is distinct from an age of zero."""

    # Clamped: callers may sample `now` before publishing, so a stamp can land
    # marginally later and the age go negative.
    def age(stamp: float) -> float | None:
        return max(0.0, round(now - stamp, 1)) if stamp else None

    return {
        "ts_ms": int(now * 1000),
        "connected": health.connected,
        "last_output_age_s": age(last_output),
        "state_publish_age_s": age(health.last_state_publish_ok),
    }


class SensorMeta(NamedTuple):
    name: str
    icon: str
    unit: str | None = None
    device_class: str | None = None
    state_class: str | None = None
    enabled_by_default: bool = True
    # "diagnostic" collapses an entity out of the device page, auto-generated
    # dashboards and Assist. Nothing here wants that; the hook is for anything
    # later that describes the add-on rather than the gate.
    entity_category: str | None = None


# The counters direwolf reports are cumulative and reset when the add-on
# restarts, so they are total_increasing rather than total -- that is the state
# class that tells HA a drop is a restart and not a negative delta.
SENSORS: dict[str, SensorMeta] = {
    "packets_uploaded": SensorMeta(
        "Packets gated to APRS-IS",
        "mdi:upload-network",
        unit="packets",
        state_class="total_increasing",
    ),
    "packets_downloaded": SensorMeta(
        "Packets from APRS-IS",
        "mdi:download-network",
        unit="packets",
        state_class="total_increasing",
    ),
    "rf_packets_received": SensorMeta(
        "RF packets received",
        "mdi:radio-tower",
        unit="packets",
        state_class="total_increasing",
    ),
    # The totals restart with the add-on; these say what the gate is doing now,
    # which is what a dashboard wants and what an automation can threshold on.
    "uploaded_rate": SensorMeta(
        "Packets gated to APRS-IS rate",
        "mdi:upload-network",
        unit="packets/min",
        state_class="measurement",
    ),
    "downloaded_rate": SensorMeta(
        "Packets from APRS-IS rate",
        "mdi:download-network",
        unit="packets/min",
        state_class="measurement",
    ),
    "rf_rate": SensorMeta(
        "RF packets received rate",
        "mdi:radio-tower",
        unit="packets/min",
        state_class="measurement",
    ),
    "stations_heard": SensorMeta(
        "Stations heard (RF)",
        "mdi:account-multiple",
        unit="stations",
        state_class="measurement",
    ),
    "stations_heard_direct": SensorMeta(
        "Stations heard direct",
        "mdi:access-point",
        unit="stations",
        state_class="measurement",
    ),
    "stations_seen_total": SensorMeta(
        "Unique stations seen",
        "mdi:counter",
        unit="stations",
        state_class="total_increasing",
        enabled_by_default=False,
    ),
    "audio_level": SensorMeta(
        "Audio level",
        "mdi:volume-high",
        state_class="measurement",
        enabled_by_default=False,
    ),
    "last_heard": SensorMeta(
        "Last packet heard",
        "mdi:clock-outline",
        device_class="timestamp",
    ),
}

BINARY_SENSORS: dict[str, SensorMeta] = {
    "igate_connected": SensorMeta(
        "IGate connected",
        "mdi:lan-connect",
        device_class="connectivity",
    ),
}


def build_discovery_payloads(
    discovery_prefix: str,
    device_id: str,
    device_name: str,
    base_topic: str,
    availability_topic: str,
    expire_after_s: int,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "Dire Wolf",
        "model": "APRS IGate (receive-only)",
    }

    for component, table in (("sensor", SENSORS), ("binary_sensor", BINARY_SENSORS)):
        for key, meta in table.items():
            payload: dict[str, Any] = {
                "name": meta.name,
                "unique_id": f"{device_id}_{key}",
                "state_topic": f"{base_topic}/{key}/state",
                "icon": meta.icon,
                "device": device,
                "availability_topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline",
            }
            if meta.unit is not None:
                payload["unit_of_measurement"] = meta.unit
            if meta.device_class is not None:
                payload["device_class"] = meta.device_class
            if meta.state_class is not None:
                payload["state_class"] = meta.state_class
            if meta.entity_category is not None:
                payload["entity_category"] = meta.entity_category
            if not meta.enabled_by_default:
                payload["enabled_by_default"] = False
            if component == "binary_sensor":
                payload["payload_on"] = "ON"
                payload["payload_off"] = "OFF"
                # Only the link state expires; RF sensors go quiet on a dead band.
                payload["expire_after"] = expire_after_s

            out[f"{discovery_prefix}/{component}/{device_id}/{key}/config"] = payload

    return out
