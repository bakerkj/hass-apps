# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Option loading, redaction, and MQTT broker resolution."""

import argparse
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

OPTION_KEYS: set[str] = {
    "mode",
    "watchdog_timeout_ms",
    "power_outage_time_limit",
    "power_outage_voltage_limit",
    "interval_seconds",
    "shutdown_via",
    "mqtt_host",
    "mqtt_port",
    "mqtt_username",
    "mqtt_password",
    "mqtt_discovery_prefix",
    "mqtt_base_topic",
    "client_id",
    "expire_after_multiplier",
    "log_level",
}

SENSITIVE_OPTION_KEYS: set[str] = {"mqtt_password"}

# Any option whose name contains one of these is redacted too, so a secret added
# later (mqtt_token, tls_secret, ...) is not logged in the clear by default.
SENSITIVE_KEY_MARKERS: tuple[str, ...] = ("password", "token", "secret")

SUPERVISOR_MQTT_URL = "http://supervisor/services/mqtt"

log = logging.getLogger(__name__)


def load_options_file(path: str, ap: argparse.ArgumentParser) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except OSError as exc:
        ap.error(f"unable to read options file {path}: {exc}")
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # UnicodeDecodeError (non-UTF-8 bytes) is also a ValueError but not a
        # JSONDecodeError, so it would otherwise escape and crash with a
        # traceback instead of a clean argparse error.
        ap.error(f"invalid JSON or encoding in options file {path}: {exc}")

    if not isinstance(payload, dict):
        ap.error("options file must contain a JSON object")

    nested = payload.get("options")
    if isinstance(nested, dict) and not any(key in payload for key in OPTION_KEYS):
        opts = nested
    else:
        opts = payload

    if not isinstance(opts, dict):
        ap.error("options object must be a JSON object")

    return opts


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return key in SENSITIVE_OPTION_KEYS or any(
        marker in lowered for marker in SENSITIVE_KEY_MARKERS
    )


def redact_options_for_log(opts: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in opts.items():
        if _is_sensitive_key(key) and str(value).strip():
            redacted[key] = "***"
        else:
            redacted[key] = value
    return redacted


def _supervisor_mqtt_service(timeout: float = 10.0) -> dict[str, Any] | None:
    """Fetch broker details from the Supervisor services API.

    The add-on declares ``services: ["mqtt:need"]``, so Supervisor exposes the
    configured broker here. Preferred over hardcoded credentials in options:
    nothing to keep in sync when the broker's password changes.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log.debug("SUPERVISOR_TOKEN unset; cannot query the Supervisor for MQTT")
        return None

    req = urllib.request.Request(
        SUPERVISOR_MQTT_URL, headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        log.warning("Could not read MQTT service config from Supervisor: %s", exc)
        return None

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict) or not data.get("host"):
        log.warning("Supervisor returned no usable MQTT service configuration")
        return None
    return data


def resolve_mqtt_config(opts: dict[str, Any]) -> dict[str, Any]:
    """Merge explicit options over Supervisor-provided broker details.

    Explicit options always win, so an external broker can be used even when
    the Supervisor has one configured.
    """
    service = _supervisor_mqtt_service() or {}

    host = opts.get("mqtt_host") or service.get("host")
    port = opts.get("mqtt_port") or service.get("port") or 1883
    username = opts.get("mqtt_username")
    password = opts.get("mqtt_password")

    if username is None and not opts.get("mqtt_host"):
        # Only inherit Supervisor credentials when also using its broker;
        # pairing them with a different host would just fail to authenticate.
        username = service.get("username")
        password = service.get("password")

    if not host:
        raise RuntimeError(
            "No MQTT broker configured. Install the Mosquitto add-on, or set "
            "mqtt_host/mqtt_port in this add-on's configuration."
        )

    if service and not opts.get("mqtt_host"):
        log.info("Using Supervisor-provided MQTT broker at %s:%s", host, port)
    else:
        log.info("Using configured MQTT broker at %s:%s", host, port)

    return {
        "host": str(host),
        "port": int(port),
        "username": str(username) if username else None,
        "password": str(password) if password else None,
    }
