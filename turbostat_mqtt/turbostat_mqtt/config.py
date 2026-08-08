# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Options: read, clamp, derive.

Split out of app.py so the clamping is reachable without a broker or a running
turbostat -- the schema bounds in config.json are the contract, and a
hand-edited options.json arrives without Supervisor validation.
"""

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Options:
    interval: float
    discovery_prefix: str
    base_topic: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    client_id: str
    log_level: str
    publish_raw: bool
    disconnect_timeout: int
    expire_after_s: int

    @property
    def raw_topic(self) -> str:
        return f"{self.base_topic}/raw_sample"

    @property
    def availability_topic(self) -> str:
        return f"{self.base_topic}/availability"

    @property
    def heartbeat_topic(self) -> str:
        return f"{self.base_topic}/heartbeat"

    @property
    def restart_grace_seconds(self) -> float:
        """Debounce between turbostat restarts.

        Half the expiry so a flapping turbostat cannot be restarted faster than
        HA notices the gap, capped at 30s so a long expiry does not leave a dead
        turbostat unattended for minutes.
        """
        return max(1.0, min(self.expire_after_s / 2.0, 30.0))

    def summary(self) -> str:
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
                f"  publish_raw:        {self.publish_raw}",
                f"  expire_after:       {self.expire_after_s}s",
            ]
        )


def read(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError("options file must contain a JSON object")
    return data


def _number(opts: dict[str, Any], key: str, default: float) -> float:
    """A numeric option, defaulting only when it is genuinely absent.

    Not ``get(key) or default``: that idiom is right for strings, where empty
    means unset, but wrong here -- it would read an explicit 0 as absent and
    hand back the default instead of clamping it to the schema floor.
    """
    value = opts.get(key)
    if value is None or value == "":
        return default
    return float(value)


def from_mapping(opts: dict[str, Any]) -> Options:
    """Build Options, clamping to config.json's schema bounds.

    Clamped rather than trusted: Supervisor validates against the schema, but a
    hand-edited /data/options.json does not go through it, and an unbounded
    interval would stall every sensor indefinitely.
    """
    interval = max(1.0, min(60.0, _number(opts, "interval_seconds", 30)))
    expire_after_multiplier = int(
        max(2, min(10, _number(opts, "expire_after_multiplier", 4)))
    )
    return Options(
        interval=interval,
        discovery_prefix=opts.get("mqtt_discovery_prefix") or "homeassistant",
        base_topic=(opts.get("mqtt_base_topic") or "turbostat").rstrip("/"),
        # `or`, not a get default: an explicit null would otherwise give None,
        # and connecting to None retries forever with no useful error.
        mqtt_host=opts.get("mqtt_host") or "core-mosquitto",
        mqtt_port=int(_number(opts, "mqtt_port", 1883)),
        mqtt_username=opts.get("mqtt_username") or "",
        mqtt_password=opts.get("mqtt_password") or "",
        client_id=opts.get("client_id") or "turbostat-app",
        log_level=(opts.get("log_level") or "INFO").upper(),
        publish_raw=bool(opts.get("publish_raw_sample", False)),
        disconnect_timeout=int(
            max(5, min(600, _number(opts, "mqtt_disconnect_timeout_seconds", 300)))
        ),
        # Floor of 60s: a short interval would otherwise expire entities faster
        # than HA can reasonably notice, flapping them unavailable.
        expire_after_s=max(60, int(interval) * expire_after_multiplier),
    )
