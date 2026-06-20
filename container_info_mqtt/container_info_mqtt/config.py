# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Option-file loading + redaction."""

import argparse
import json
from typing import Any

from .util import SENSITIVE_OPTION_KEYS


OPTION_KEYS: set[str] = {
    "interval_seconds",
    "docker_timeout_seconds",
    "include_metrics",
    "summary_metrics",
    "container_include_regex",
    "container_exclude_regex",
    "mqtt_host",
    "mqtt_port",
    "mqtt_username",
    "mqtt_password",
    "mqtt_discovery_prefix",
    "mqtt_base_topic",
    "client_id",
    "log_level",
    "expire_after_multiplier",
    "mqtt_disconnect_timeout_seconds",
}


def load_options_file(path: str, ap: argparse.ArgumentParser) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except OSError as exc:
        ap.error(f"unable to read options file {path}: {exc}")
    except json.JSONDecodeError as exc:
        ap.error(f"invalid JSON in options file {path}: {exc}")

    if not isinstance(payload, dict):
        ap.error("options file must contain a JSON object")

    nested = payload.get("options")
    if isinstance(nested, dict) and not any(key in payload for key in OPTION_KEYS):
        opts = nested
    else:
        opts = payload

    if not isinstance(opts, dict):
        ap.error("options object must be a JSON object")

    known_keys = sorted(key for key in opts.keys() if key in OPTION_KEYS)
    if not known_keys:
        ap.error(
            "options file does not contain recognized add-on keys; "
            "expected top-level keys or an 'options' object"
        )

    return opts


def redact_options_for_log(opts: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in opts.items():
        if key in SENSITIVE_OPTION_KEYS and str(value).strip():
            redacted[key] = "***"
        else:
            redacted[key] = value
    return redacted
