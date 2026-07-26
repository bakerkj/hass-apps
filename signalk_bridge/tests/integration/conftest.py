# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end harness for signalk_bridge.

Builds the REAL add-on image and runs the bridge in a container (its shipped
apk paho, run.sh, s6 -- the actual artifact), against a stub Signal K server and
a real Mosquitto broker, then observes what it publishes with an independent MQTT
client. The observer's paho version is irrelevant; the bridge under test uses the
container's.

Run: pytest signalk_bridge/tests/integration/ -m e2e
Requires: docker, and a Mosquitto broker (a local ``mosquitto`` binary, or docker
to run one). The default ``pytest`` run excludes the ``e2e`` marker.
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
import uuid
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

ADDON_SOURCE_DIR = Path(__file__).resolve().parents[2]  # .../signalk_bridge

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


# ---------------------------------------------------------------------------
# docker helpers
# ---------------------------------------------------------------------------


def _docker(*args: str, check: bool = True, capture: bool = True) -> str:
    if capture:
        result = subprocess.run(
            ["docker", *args],
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    else:
        result = subprocess.run(["docker", *args], check=check, text=True)
    return result.stdout or ""


def _have_docker() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        _docker("version", "--format", "{{.Server.Version}}")
    except subprocess.CalledProcessError, FileNotFoundError:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def _require_docker() -> None:
    if not _have_docker():
        pytest.skip("docker daemon not reachable; skipping e2e suite")


@pytest.fixture(scope="session")
def addon_image() -> str:
    """Build (or reuse) the real signalk_bridge add-on image.

    If ``SIGNALK_BRIDGE_IMAGE`` is set, trust the caller (CI prebuild via
    docker/build-push-action with a gha cache) and use that tag.
    """
    prebuilt = os.environ.get("SIGNALK_BRIDGE_IMAGE")
    if prebuilt:
        return prebuilt
    tag = f"signalk_bridge_e2e:{uuid.uuid4().hex[:8]}"
    _docker(
        "build",
        "--build-arg",
        "BUILD_VERSION=e2e",
        "-t",
        tag,
        str(ADDON_SOURCE_DIR),
    )
    return tag


# ---------------------------------------------------------------------------
# stub Signal K server (host loopback; container reaches it via --network host)
# ---------------------------------------------------------------------------


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

    def log_message(self, *_a: Any) -> None:
        pass


@pytest.fixture
def signalk_stub() -> Iterator[tuple[str, int]]:
    srv = HTTPServer(("127.0.0.1", _free_port()), _SKHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    _host, port = cast("tuple[str, int]", srv.server_address)
    try:
        yield ("127.0.0.1", port)
    finally:
        srv.shutdown()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Mosquitto broker (host: a local binary, or docker --network host)
# ---------------------------------------------------------------------------


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
    for candidate in ("/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto"):
        if os.path.exists(candidate):
            return candidate
    return None


@pytest.fixture
def mosquitto(tmp_path: Path) -> Iterator[tuple[str, int]]:
    """A real Mosquitto broker on a free host port. Prefers a local binary
    (what CI installs via apt); falls back to the eclipse-mosquitto image with
    host networking so the same ``mosquitto -c`` config runs either way."""
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


# ---------------------------------------------------------------------------
# the bridge container under test (the real shipped artifact)
# ---------------------------------------------------------------------------


class BridgeContainer:
    def __init__(self, name: str) -> None:
        self.name = name

    def running(self) -> bool:
        out = _docker(
            "inspect", "-f", "{{.State.Running}}", self.name, check=False
        ).strip()
        return out == "true"

    def logs(self) -> str:
        return _docker("logs", self.name, check=False)

    def graceful_stop(self) -> None:
        _docker("stop", "-t", "10", self.name, check=False)

    def remove(self) -> None:
        _docker("rm", "-f", self.name, check=False)


@pytest.fixture
def bridge_container(
    addon_image: str,
    signalk_stub: tuple[str, int],
    mosquitto: tuple[str, int],
    tmp_path: Path,
) -> Iterator[BridgeContainer]:
    sk_host, sk_port = signalk_stub
    mqtt_host, mqtt_port = mosquitto
    options = {
        "signalk_url": f"http://{sk_host}:{sk_port}",
        "signalk_token": "e2e-token",  # non-empty -> skips the request/approve flow
        "mqtt_host": mqtt_host,
        "mqtt_port": mqtt_port,
        "interval_seconds": 1,
        "log_level": "info",
    }
    data = tmp_path / "data"
    data.mkdir()
    (data / "options.json").write_text(json.dumps(options))

    name = f"skb_e2e_{uuid.uuid4().hex[:10]}"
    # --network host: the bridge reaches the host-run stub Signal K + broker on
    # 127.0.0.1 (the add-on ships host_network: true anyway). No --init: the HA
    # base image's s6 must be PID 1 (run.sh uses with-contenv), and s6 forwards
    # signals for the clean-shutdown path. No --rm either, so a crashed
    # container's logs survive for the failure message; the fixture removes it.
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        "host",
        "-v",
        f"{data / 'options.json'}:/data/options.json:ro",
        addon_image,
    )
    handle = BridgeContainer(name)
    try:
        yield handle
    finally:
        handle.remove()


# ---------------------------------------------------------------------------
# MQTT observer (independent client; its paho version is irrelevant)
# ---------------------------------------------------------------------------


def _real_paho() -> Any:
    """Import the REAL paho client, dropping the repo-wide import stub the root
    conftest installs via ``sys.modules.setdefault``."""
    for name in ("paho.mqtt.client", "paho.mqtt", "paho"):
        mod = sys.modules.get(name)
        if mod is not None and not getattr(mod, "__file__", None):
            del sys.modules[name]
    return importlib.import_module("paho.mqtt.client")


class Collector:
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
    host, port = mosquitto
    mqtt = _real_paho()
    coll = Collector()
    client = mqtt.Client(client_id="signalk-e2e-observer")

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


@pytest.fixture
def wait_for() -> Callable[..., bool]:
    def _wait_for(
        predicate: Callable[[], bool], timeout: float, interval: float = 0.2
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return predicate()

    return _wait_for
