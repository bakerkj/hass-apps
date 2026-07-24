# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""MQTT client helpers: health tracking, publish wrapper, discovery payloads."""

import time
from typing import Any

import paho.mqtt.client as mqtt

from .metadata import DIAGNOSTIC_COLS, friendly_name, guess_meta
from .util import log


class MqttHealth:
    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0


def mqtt_publish(
    client: mqtt.Client,
    topic: str,
    payload: str,
    *,
    qos: int,
    retain: bool,
    log_level: str,
    health: MqttHealth,
    mark_state: bool = False,
) -> bool:
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            if mark_state:
                health.last_state_publish_ok = time.time()
            return True
        log("WARNING", f"MQTT publish rc={info.rc} topic={topic}", log_level)
    except Exception as e:  # noqa: BLE001 publish wrapper must not crash caller
        log("WARNING", f"MQTT publish failed topic={topic}: {e}", log_level)
    return False


def connect_mqtt_with_retry(
    client: mqtt.Client,
    mqtt_host: str,
    mqtt_port: int,
    log_level: str,
) -> None:
    delay = 5
    while True:
        try:
            client.connect(mqtt_host, mqtt_port, keepalive=60)
            return
        except Exception as e:  # noqa: BLE001 retry loop must not crash
            log(
                "WARNING",
                f"Cannot connect to MQTT broker {mqtt_host}:{mqtt_port}: {e} — retrying in {delay}s",
                log_level,
            )
            time.sleep(delay)
            delay = min(delay * 2, 60)


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
