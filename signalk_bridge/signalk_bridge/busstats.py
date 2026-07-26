# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""NMEA 2000 bus health as Home Assistant entities.

Two sources, both cheap: the SocketCAN interface counters in sysfs (frame rate
and errors -- readable because the add-on runs with host networking) and Signal
K's source inventory (how many devices are on the bus and how many are live).
Everything here groups under one "NMEA 2000 Bus" device.
"""

import logging
import time
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

GROUP_ID = "n2k_bus"
GROUP_LABEL = "NMEA 2000 Bus"

# A device is "active" if it has sent any PGN within this window.
_ACTIVE_WINDOW_S = 60.0


def _read_int(path: str) -> int | None:
    try:
        with open(path, encoding="utf-8") as f:
            return int(f.read().strip())
    except OSError, ValueError:
        return None


def _read_str(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return None


def _entity(
    key: str, name: str, state: str, *, component: str = "sensor", **extra: Any
) -> tuple[str, dict[str, Any]]:
    ent = {
        "component": component,
        "name": name,
        "state": state,
        "group_id": GROUP_ID,
        "group_label": GROUP_LABEL,
        "unit": None,
        "device_class": None,
        "state_class": None,
        "icon": None,
        "attributes": None,
    }
    ent.update(extra)
    return key, ent


def _count_devices(sources: dict[str, Any]) -> tuple[int, int]:
    """Return (total, active) N2K device counts from a Signal K sources tree.

    N2K devices are identified by their ``n2k`` substructure rather than the
    provider label, which varies across installs (``n2k-can0``, ``can0``, ...).
    """
    now = datetime.now(UTC)
    total = active = 0
    for devices in sources.values():
        if not isinstance(devices, dict):
            continue
        for addr, info in devices.items():
            if not addr.isdigit() or not isinstance(info, dict):
                continue
            n2k = info.get("n2k")
            if not isinstance(n2k, dict):
                continue
            total += 1
            if _seen_recently(n2k.get("pgns"), now):
                active += 1
    return total, active


def _seen_recently(pgns: Any, now: datetime) -> bool:
    if not isinstance(pgns, dict):
        return False
    for ts in pgns.values():
        try:
            when = datetime.fromisoformat(str(ts))
        except ValueError, TypeError:
            continue
        # A tz-naive timestamp would raise TypeError against our aware `now`.
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        if (now - when).total_seconds() <= _ACTIVE_WINDOW_S:
            return True
    return False


class BusStats:
    """Samples CAN interface counters and derives bus-health entities."""

    def __init__(self, iface: str = "can0") -> None:
        self._base = f"/sys/class/net/{iface}/statistics"
        self._link = f"/sys/class/net/{iface}"
        self._prev_rx: int | None = None
        self._prev_t: float | None = None

    def available(self) -> bool:
        return _read_int(f"{self._base}/rx_packets") is not None

    def sample(self, sources: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        now = time.monotonic()

        rx = _read_int(f"{self._base}/rx_packets")
        rx_err = _read_int(f"{self._base}/rx_errors")
        rx_drop = _read_int(f"{self._base}/rx_dropped")
        operstate = _read_str(f"{self._link}/operstate") or "unknown"

        rate: float | None = None
        if rx is not None and self._prev_rx is not None and self._prev_t is not None:
            dt = now - self._prev_t
            if dt > 0:
                rate = max(0.0, (rx - self._prev_rx) / dt)
        if rx is not None:
            self._prev_rx, self._prev_t = rx, now

        if rate is not None:
            k, e = _entity(
                "n2k_bus_frame_rate",
                "Frame rate",
                f"{rate:.1f}",
                unit="frame/s",
                icon="mdi:gauge",
                state_class="measurement",
            )
            out[k] = e
        if rx_err is not None:
            k, e = _entity(
                "n2k_bus_rx_errors",
                "Receive errors",
                str(rx_err),
                icon="mdi:alert-circle-outline",
                state_class="total_increasing",
            )
            out[k] = e
        if rx_drop is not None:
            k, e = _entity(
                "n2k_bus_rx_dropped",
                "Receive dropped",
                str(rx_drop),
                icon="mdi:trash-can-outline",
                state_class="total_increasing",
            )
            out[k] = e

        if sources:
            total, active = _count_devices(sources)
            k, e = _entity(
                "n2k_bus_devices",
                "Devices on bus",
                str(total),
                icon="mdi:devices",
                state_class="measurement",
            )
            out[k] = e
            k, e = _entity(
                "n2k_bus_active_devices",
                "Active devices",
                str(active),
                icon="mdi:access-point-check",
                state_class="measurement",
            )
            out[k] = e

        # Link up = "connected"; the frame-rate sensor conveys traffic. (Gating
        # on rate > 0 would flap OFF on any idle cycle or counter reset.)
        connected = operstate == "up"
        k, e = _entity(
            "n2k_bus_status",
            "Bus status",
            "ON" if connected else "OFF",
            component="binary_sensor",
            device_class="connectivity",
            icon="mdi:bus-alert",
        )
        out[k] = e
        return out
