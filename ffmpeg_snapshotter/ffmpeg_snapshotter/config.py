# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-stream and MQTT configuration dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class StreamCfg:
    name: str
    url: str
    interval_seconds: int
    output_dir: Path
    filename_format: str
    date_dir_format: str
    latest_name: str
    retain_count: int
    retain_days: int
    extra_input_args: str
    extra_output_args: str


@dataclass
class MqttConfig:
    """MQTT publishing config.  Disabled when ``host`` is empty."""

    host: str
    port: int
    username: str
    password: str
    discovery_prefix: str
    base_topic: str
    client_id: str
    publish_interval_seconds: int
    rate_window_seconds: int
    disconnect_timeout_seconds: int
    snapshot_error_timeout_seconds: int

    @property
    def enabled(self) -> bool:
        return bool(self.host)


class MqttHealth:
    """Liveness tracking for the MQTT publisher's watchdog."""

    def __init__(self) -> None:
        self.connected: bool = False
        self.last_connect_ok: float = 0.0
        self.last_disconnect: float = 0.0
        self.last_state_publish_ok: float = 0.0


def load_mqtt_config(opts: dict) -> MqttConfig:
    """Build an :class:`MqttConfig` from parsed HA add-on options."""
    return MqttConfig(
        host=str(opts.get("mqtt_host", "") or ""),
        port=int(opts.get("mqtt_port", 1883)),
        username=str(opts.get("mqtt_username", "") or ""),
        password=str(opts.get("mqtt_password", "") or ""),
        discovery_prefix=str(opts.get("mqtt_discovery_prefix", "homeassistant")),
        base_topic=str(opts.get("mqtt_base_topic", "ffmpeg_snapshotter")),
        client_id=str(opts.get("mqtt_client_id", "ffmpeg-snapshotter")),
        publish_interval_seconds=int(opts.get("mqtt_publish_interval_seconds", 60)),
        rate_window_seconds=int(opts.get("mqtt_rate_window_seconds", 300)),
        disconnect_timeout_seconds=max(
            5, int(opts.get("mqtt_disconnect_timeout_seconds", 300))
        ),
        snapshot_error_timeout_seconds=max(
            10, int(opts.get("mqtt_snapshot_error_timeout_seconds", 300))
        ),
    )
