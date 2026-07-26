# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Fixtures for the signalk_bridge end-to-end test.

Spins up a stub Signal K REST server and a real Mosquitto broker (an apt-installed
binary, or the eclipse-mosquitto image as a local fallback) so the actual bridge
process can run against them and its MQTT output be observed on the wire -- the
one path the unit suite deliberately stubs.
"""

import importlib
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

# A small vessel snapshot whose conversions we assert once they reach the broker.
VESSEL_TREE: dict[str, Any] = {
    "navigation": {
        "speedOverGround": {"value": 3.086},  # m/s, published as-is
        "courseOverGroundTrue": {"value": 1.5708},  # rad -> ~90 deg
        "position": {"value": {"latitude": 40.0, "longitude": -70.0}},
    },
    "environment": {"water": {"temperature": {"value": 288.15}}},  # K -> 15 C
    "electrical": {
        "batteries": {"house": {"stateOfCharge": {"value": 0.87}}}  # -> 87 %
    },
}

_SERVER_INFO = {"server": {"id": "signalk-server", "version": "0.0.0-e2e"}}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _SKHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.rstrip("/") == "/signalk":
            body: Any = _SERVER_INFO
        elif self.path.startswith("/signalk/v1/api/vessels/self"):
            body = VESSEL_TREE
        elif self.path.startswith("/signalk/v1/api/sources"):
            body = {}
        else:
            self.send_response(404)
            self.end_headers()
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_a: Any) -> None:  # silence the request log
        pass


@pytest.fixture
def signalk_stub() -> Iterator[str]:
    """A minimal Signal K REST server returning a fixed vessel snapshot."""
    srv = HTTPServer(("127.0.0.1", _free_port()), _SKHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    host, port = cast("tuple[str, int]", srv.server_address)
    try:
        yield f"http://{host}:{port}"
    finally:
        srv.shutdown()
        thread.join(timeout=5)


def _wait_port(host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"nothing listening on {host}:{port} after {timeout:.0f}s")


def _find_mosquitto() -> str | None:
    found = shutil.which("mosquitto")
    if found:
        return found
    # The Debian/Ubuntu broker package installs to /usr/sbin, which isn't always
    # on a non-login PATH.
    for candidate in ("/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto"):
        if os.path.exists(candidate):
            return candidate
    return None


@pytest.fixture
def mosquitto(tmp_path: Path) -> Iterator[tuple[str, int]]:
    """A real Mosquitto broker on a free port.

    Prefers a local ``mosquitto`` binary (what CI installs via apt). Falls back
    to the same ``mosquitto -c`` invocation from the eclipse-mosquitto image with
    host networking, so a docker-only dev box exercises the identical config.
    Skips when neither is available. No Docker Hub pull happens in CI.
    """
    port = _free_port()
    conf = tmp_path / "mosquitto.conf"
    conf.write_text(f"listener {port}\nallow_anonymous true\n")

    binary = _find_mosquitto()
    cmd: list[str]
    if binary:
        cmd = [binary, "-c", str(conf)]
    elif shutil.which("docker"):
        image = os.environ.get("MOSQUITTO_IMAGE", "eclipse-mosquitto:2")
        cmd = [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-v",
            f"{conf}:/mosquitto/config/mosquitto.conf:ro",
            image,
        ]
    else:
        pytest.skip("need a mosquitto binary or docker to run the broker")

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_port("127.0.0.1", port, 20)
        yield ("127.0.0.1", port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _real_paho() -> Any:
    """Import the REAL paho client, dropping the repo-wide import stub that the
    root conftest installs via ``sys.modules.setdefault``."""
    for name in ("paho.mqtt.client", "paho.mqtt", "paho"):
        mod = sys.modules.get(name)
        if mod is not None and not getattr(mod, "__file__", None):
            del sys.modules[name]
    return importlib.import_module("paho.mqtt.client")


class Collector:
    """Thread-safe record of the last payload seen on each MQTT topic."""

    def __init__(self) -> None:
        self.seen: dict[str, str] = {}
        self._lock = threading.Lock()

    def on_message(self, _client: Any, _userdata: Any, msg: Any) -> None:
        with self._lock:
            self.seen[msg.topic] = msg.payload.decode()

    def get(self, topic: str) -> str | None:
        with self._lock:
            return self.seen.get(topic)

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self.seen)


@pytest.fixture
def subscriber(mosquitto: tuple[str, int]) -> Iterator[Collector]:
    """A real paho subscriber on the broker, collecting every retained/live
    message under ``homeassistant/#`` and ``signalk/#``."""
    host, port = mosquitto
    mqtt = _real_paho()
    coll = Collector()
    client = mqtt.Client(client_id="signalk-e2e-sub")

    def _on_connect(c: Any, *_a: Any) -> None:
        c.subscribe("homeassistant/#")
        c.subscribe("signalk/#")

    client.on_connect = _on_connect
    client.on_message = coll.on_message
    client.connect(host, port, keepalive=30)
    client.loop_start()
    try:
        yield coll
    finally:
        client.loop_stop()
        client.disconnect()
