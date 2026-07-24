# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT helpers: health state + HA discovery publisher."""

import json
import logging
from typing import Any

import paho.mqtt.client as mqtt


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0


def publish_discovery(
    client: mqtt.Client,
    discovery_prefix: str,
    base_topic: str,
    device_id: str,
    device_name: str,
    metrics: dict[str, dict[str, Any]],
    expire_after_s: int,
    log: logging.Logger,
) -> None:
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

    expire_after = expire_after_s

    for key, m in metrics.items():
        state_topic = f"{base_topic}/{key}/state"
        payload: dict[str, Any] = {
            "name": m["name"],
            "unique_id": f"{device_id}_{key}",
            "default_entity_id": f"sensor.intel_gpu_{key}",
            "state_topic": state_topic,
            "availability_topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "expire_after": expire_after,
            "unit_of_measurement": m["unit"],
            "device": device,
        }

        # Suggested display precision in Home Assistant UI
        if m["unit"] == "W" or m["unit"] == "%":
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

        # Icon per engine type
        engine = m.get("attrs", {}).get("engine")
        if engine in engine_icons:
            payload["icon"] = engine_icons[engine]

        config_topic = f"{discovery_prefix}/sensor/{device_id}/{key}/config"
        info = client.publish(config_topic, json.dumps(payload), qos=1, retain=False)
        log.debug(
            "MQTT discovery publish %s mid=%s rc=%s", config_topic, info.mid, info.rc
        )
