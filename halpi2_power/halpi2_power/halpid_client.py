# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Minimal HTTP client for halpid's UNIX-socket API.

halpid serves a small REST API over a UNIX socket rather than TCP. `http.client`
supports this with a custom connection class, so no extra dependency is needed.

This module is the *only* thing that talks to halpid, and nothing here touches
I2C -- halpid owns the bus. Two processes poking `/dev/i2c-1` concurrently would
interleave register reads and corrupt each other's results.
"""

import http.client
import json
import socket
from typing import Any


class UnixSocketHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that dials a UNIX socket instead of a TCP host."""

    def __init__(self, socket_path: str, timeout: float = 10.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.socket_path)
        except OSError:
            # Don't leak the fd on a failed dial (halpid not up yet); the raise
            # still reaches _request's OSError handler as a HalpidError.
            sock.close()
            raise
        self.sock = sock


class HalpidError(Exception):
    """halpid could not be reached, or returned something unusable."""


def _request(socket_path: str, method: str, path: str, timeout: float) -> Any:
    conn = UnixSocketHTTPConnection(socket_path, timeout=timeout)
    try:
        conn.request(method, path)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise HalpidError(
                f"{method} {path} returned HTTP {resp.status}: "
                f"{body.decode(errors='replace').strip()}"
            )
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise HalpidError(f"{method} {path} returned invalid JSON: {exc}") from exc
    except (OSError, http.client.HTTPException) as exc:
        raise HalpidError(f"{method} {path} failed: {exc}") from exc
    finally:
        conn.close()


def get_values(socket_path: str, timeout: float = 10.0) -> dict[str, Any]:
    """Read current measurements and power state from halpid.

    Returns the decoded ``/values`` object. The field names are halpid's real
    ones (``V_in``, ``T_mcu``, ``state``, ...), NOT the ``dcin_voltage`` /
    ``power_state`` names the upstream README documents -- see sensors.py.
    Example::

        {"5v_output_enabled": true, "I_in": 0.246, "T_mcu": 324.53,
         "T_pcb": 311.77, "V_cap": 10.35, "V_in": 13.81,
         "state": "OperationalCoOp", "daemon_version": "5.1.1",
         "device_id": "0011223344556677", "firmware_version": "3.3.1",
         "hardware_version": "N/A", "num_leds": 5, "watchdog_elapsed": 0.0,
         "watchdog_enabled": true, "watchdog_timeout": 10.0}
    """
    payload = _request(socket_path, "GET", "/values", timeout)
    if not isinstance(payload, dict):
        raise HalpidError(f"/values returned {type(payload).__name__}, expected object")
    return payload


def get_version(socket_path: str, timeout: float = 10.0) -> Any:
    """Read halpid's own version. Used as a readiness probe at startup."""
    return _request(socket_path, "GET", "/version", timeout)
