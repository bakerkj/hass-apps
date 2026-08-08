# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health tracking and discovery payloads."""

from typing import Any

from .metadata import DIAGNOSTIC_COLS, friendly_name, guess_meta


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0


def build_discovery_payloads(
    discovery_prefix: str,
    device_id: str,
    device_name: str,
    base_topic: str,
    availability_topic: str,
    cols: dict[str, str],
    expire_after_s: int,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "turbostat",
        "model": "turbostat summary",
    }

    expire_after = expire_after_s

    for original_col, json_key in cols.items():
        name = friendly_name(original_col)
        unit, device_class, icon, sdp = guess_meta(original_col)

        payload: dict[str, Any] = {
            "name": name,
            "unique_id": f"{device_id}_{json_key}",
            "state_topic": f"{base_topic}/{json_key}/state",
            "icon": icon,
            "device": device,
            "entity_category": "diagnostic",
            "state_class": "measurement",
            "suggested_display_precision": int(sdp),
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "expire_after": expire_after,
        }

        if unit is not None:
            payload["unit_of_measurement"] = unit
        if device_class is not None:
            payload["device_class"] = device_class
        if original_col in DIAGNOSTIC_COLS:
            payload["enabled_by_default"] = False

        disc_topic = f"{discovery_prefix}/sensor/{device_id}/{json_key}/config"
        out[disc_topic] = payload

    return out
