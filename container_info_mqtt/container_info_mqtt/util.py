# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Small utilities: slug/name helpers, safe coercion, deep-get, subprocess error."""

import re
import subprocess
from typing import Any

SENSITIVE_OPTION_KEYS: set[str] = {"mqtt_password"}

DOCKER_SOCKET_PATH = "/var/run/docker.sock"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"


def container_display_name(value: str) -> str:
    display = value.strip()
    if not display:
        return "Unknown"

    # Retained MQTT topics may use either historical prefix — treat both alike.
    for prefix in ("addon_", "app_"):
        if display.lower().startswith(prefix):
            display = display[len(prefix) :]
            parts = display.split("_", 1)
            if len(parts) == 2 and re.fullmatch(r"[0-9a-f]+", parts[0], re.IGNORECASE):
                display = parts[1]
            break

    display = re.sub(r"[_\-]+", " ", display)
    display = re.sub(r"\s+", " ", display).strip()
    if not display:
        return "Unknown"

    return " ".join(part.capitalize() for part in display.split(" "))


def safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def safe_text(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def deep_get(container: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = container
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def cmd_error(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()
