# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health state + HA discovery payload construction."""

from typing import Any


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0


def discovery_payloads(
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    device_name: str,
    metrics: dict[str, dict[str, Any]],
    expire_after_s: int,
) -> dict[str, dict[str, Any]]:
    """Build the HA discovery configs as {config_topic: payload}.

    Pure: publishing belongs to the publisher, so the payload shape stays
    testable without a broker.
    """
    out: dict[str, dict[str, Any]] = {}

    device = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "Intel",
        "model": "intel_gpu_top",
    }

    availability_topic = f"{base_topic}/availability"

    engine_icons = {
        "Render/3D": "mdi:cube-outline",
        "Video": "mdi:video-outline",
        "VideoEnhance": "mdi:video-plus-outline",
        "Blitter": "mdi:image-move",
    }

    for key, m in metrics.items():
        payload: dict[str, Any] = {
            "name": m["name"],
            "unique_id": f"{device_id}_{key}",
            "default_entity_id": f"sensor.intel_gpu_{key}",
            "state_topic": f"{base_topic}/{key}/state",
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "expire_after": expire_after_s,
            "unit_of_measurement": m["unit"],
            "device": device,
        }

        # Suggested display precision in the Home Assistant UI.
        if m["unit"] in ("W", "%"):
            payload["suggested_display_precision"] = 1
        elif m["unit"] == "MHz":
            payload["suggested_display_precision"] = 0

        if m["unit"] == "W":
            payload["device_class"] = "power"
            payload["icon"] = "mdi:flash-outline"
        elif key.startswith("freq_"):
            payload["icon"] = "mdi:speedometer"
        elif key.startswith("rc6_"):
            payload["icon"] = "mdi:sleep"

        # Icon per engine type.
        engine = m.get("attrs", {}).get("engine")
        if engine in engine_icons:
            payload["icon"] = engine_icons[engine]

        out[f"{discovery_prefix}/sensor/{device_id}/{key}/config"] = payload

    return out
