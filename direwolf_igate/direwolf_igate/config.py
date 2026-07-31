# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Add-on options: parsing, clamping, and the derived topic names."""

import json
import os
from dataclasses import dataclass

DEVICE_NAME = "Direwolf APRS IGate"


@dataclass(frozen=True)
class Options:
    """Validated options.

    The numeric bounds mirror config.json's schema, upper limits included, so a
    value that reached us without Supervisor validation (a hand-edited
    options.json, a schema change) cannot produce a nonsensical interval.
    """

    mqtt_enabled: bool
    log_level: str
    interval: int
    discovery_prefix: str
    base_topic: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    client_id: str
    disconnect_timeout: int
    expire_after_s: int

    @property
    def device_id(self) -> str:
        return self.client_id.replace("-", "_")

    @property
    def availability_topic(self) -> str:
        return f"{self.base_topic}/availability"

    @property
    def heartbeat_topic(self) -> str:
        return f"{self.base_topic}/heartbeat"

    @property
    def stall_timeout(self) -> int:
        """A connected IGate is never silent this long, even on a dead band."""
        return self.disconnect_timeout * 2

    def summary(self, mycall: str) -> str:
        return "\n".join(
            [
                "Configuration:",
                f"  base_topic:         {self.base_topic}",
                f"  client_id:          {self.client_id}",
                f"  disconnect_timeout: {self.disconnect_timeout}s",
                f"  discovery_prefix:   {self.discovery_prefix}",
                f"  interval:           {self.interval}s",
                f"  log_level:          {self.log_level}",
                f"  mqtt_host:          {self.mqtt_host}:{self.mqtt_port}",
                f"  mqtt_username:      {self.mqtt_username or '(none)'}",
                f"  mycall:             {mycall}",
                f"  expire_after:       {self.expire_after_s}s",
            ]
        )


def _as_bool(value: object) -> bool:
    """The schema makes Supervisor deliver a real bool, but a hand-edited
    options.json can hold a string -- where bool("false") is True."""
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _clamp(value: str | float, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def _resolved(opts: dict, key: str, env: str, default: str = "") -> str:
    """An explicit option wins; otherwise use what run.sh resolved from the
    Supervisor's mqtt service, so a broker add-on needs no hand-provisioning."""
    return str(opts.get(key) or os.environ.get(env) or default)


def from_mapping(opts: dict) -> Options:
    # Bounds from config.json: interval_seconds int(5,300),
    # expire_after_multiplier int(2,10), disconnect int(5,600).
    interval = _clamp(opts.get("interval_seconds", 30), 5, 300)
    multiplier = _clamp(opts.get("expire_after_multiplier", 4), 2, 10)
    return Options(
        mqtt_enabled=_as_bool(opts.get("mqtt_enabled", False)),
        log_level=(opts.get("log_level") or "INFO").upper(),
        interval=interval,
        discovery_prefix=opts.get("mqtt_discovery_prefix") or "homeassistant",
        base_topic=(opts.get("mqtt_base_topic") or "direwolf_igate").rstrip("/"),
        # `or`, not a get default: an explicit null would otherwise give None,
        # and connect(None, ...) retries forever with no useful error.
        mqtt_host=_resolved(opts, "mqtt_host", "SVC_MQTT_HOST", "core-mosquitto"),
        mqtt_port=int(_resolved(opts, "mqtt_port", "SVC_MQTT_PORT", "1883")),
        mqtt_username=_resolved(opts, "mqtt_username", "SVC_MQTT_USERNAME"),
        mqtt_password=_resolved(opts, "mqtt_password", "SVC_MQTT_PASSWORD"),
        client_id=opts.get("client_id") or "direwolf-igate",
        disconnect_timeout=_clamp(
            opts.get("mqtt_disconnect_timeout_seconds", 300), 5, 600
        ),
        expire_after_s=max(60, interval * multiplier),
    )


def read(path: str) -> dict:
    """Just the JSON. Interpreting it is separate, so an unparsable MQTT option
    cannot break the pass-through path that ignores those options entirely."""
    with open(path, "r", encoding="utf-8") as f:
        result: dict = json.load(f)
        return result


def enabled(opts: dict) -> bool:
    return _as_bool(opts.get("mqtt_enabled", False))
