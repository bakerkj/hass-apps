# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Options: CLI over options file over default, clamped and derived.

Split out of app.py so the precedence and clamping are reachable without a
broker or a running intel_gpu_top. The schema bounds in config.json are the
contract, and a hand-edited options.json arrives without Supervisor validation.
"""

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Options:
    interval: int
    discovery_prefix: str
    base_topic: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    client_id: str
    log_level: str
    publish_raw: bool
    preferred_device_regex: str
    disconnect_timeout: int
    restart_grace_seconds: int
    expire_after_s: int

    @property
    def interval_ms(self) -> int:
        return self.interval * 1000

    @property
    def raw_topic(self) -> str:
        return f"{self.base_topic}/raw_sample"

    @property
    def availability_topic(self) -> str:
        return f"{self.base_topic}/availability"

    @property
    def heartbeat_topic(self) -> str:
        return f"{self.base_topic}/heartbeat"

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
                f"  preferred_device:   {self.preferred_device_regex or '(auto)'}",
                f"  publish_raw:        {self.publish_raw}",
                f"  restart_grace:      {self.restart_grace_seconds}s",
                f"  expire_after:       {self.expire_after_s}s",
            ]
        )


def read(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError("options file must contain a JSON object")
    return data


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _pick(cli: Any, opts: dict[str, Any], key: str, default: Any) -> Any:
    """CLI wins, then the options file, then the built-in default.

    ``is None`` at each step rather than falsiness: 0 and "" are meaningful
    values a user can set, and treating them as absent would silently promote
    the default over an explicit choice.
    """
    if cli is not None:
        return cli
    value = opts.get(key)
    return default if value is None else value


def from_sources(cli: dict[str, Any], opts: dict[str, Any]) -> Options:
    """Build Options from parsed CLI args and the options mapping."""
    interval = max(
        1, int(_pick(cli.get("interval_seconds"), opts, "interval_seconds", 5))
    )
    multiplier = max(
        2,
        min(
            10,
            int(
                _pick(
                    cli.get("expire_after_multiplier"),
                    opts,
                    "expire_after_multiplier",
                    4,
                )
            ),
        ),
    )
    return Options(
        interval=interval,
        discovery_prefix=str(
            _pick(
                cli.get("mqtt_discovery_prefix"),
                opts,
                "mqtt_discovery_prefix",
                "homeassistant",
            )
        ),
        base_topic=str(
            _pick(cli.get("mqtt_base_topic"), opts, "mqtt_base_topic", "intel_gpu_top")
        ).rstrip("/"),
        mqtt_host=str(_pick(cli.get("mqtt_host"), opts, "mqtt_host", "")),
        mqtt_port=int(_pick(cli.get("mqtt_port"), opts, "mqtt_port", 1883)),
        mqtt_username=str(_pick(cli.get("mqtt_username"), opts, "mqtt_username", "")),
        mqtt_password=str(_pick(cli.get("mqtt_password"), opts, "mqtt_password", "")),
        client_id=str(
            _pick(cli.get("client_id"), opts, "client_id", "intel-gpu-top-addon")
        ),
        log_level=str(_pick(cli.get("log_level"), opts, "log_level", "INFO")).upper(),
        publish_raw=parse_bool(
            _pick(cli.get("publish_raw_sample"), opts, "publish_raw_sample", True)
        ),
        preferred_device_regex=str(
            _pick(cli.get("preferred_device_regex"), opts, "preferred_device_regex", "")
        ),
        disconnect_timeout=max(
            5,
            min(
                600,
                int(
                    _pick(
                        cli.get("mqtt_disconnect_timeout_seconds"),
                        opts,
                        "mqtt_disconnect_timeout_seconds",
                        300,
                    )
                ),
            ),
        ),
        restart_grace_seconds=max(
            1,
            int(
                _pick(
                    cli.get("intel_restart_grace_seconds"),
                    opts,
                    "intel_restart_grace_seconds",
                    10,
                )
            ),
        ),
        # Floor of 60s: a short interval would otherwise expire entities faster
        # than HA can reasonably notice, flapping them unavailable.
        expire_after_s=max(60, interval * multiplier),
    )
