# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end harness for halpi2_power.

Runs the REAL add-on image (its s6 tree, run scripts, shipped apk python/paho --
the actual artifact) against a real Mosquitto broker, then observes what the MQTT
publisher emits with an independent client.

halpid itself cannot run here: it drives the RP2040 power controller over
/dev/i2c-1, absent on a CI runner, so without help it would exit at once and s6
would crash-loop it (its finish script restarts rather than halts). So a fake
halpid is bind-mounted over /usr/bin/halpid; it serves the same UNIX-socket
/values + /version API the publisher reads, from the sample a real controller
emits. Everything downstream of the socket -- render-config, the
s6 services, the socket wait in mqtt/run, and the publisher -- is the real thing.

Run: pytest halpi2_power/tests/integration/ -m e2e
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
from pathlib import Path
from typing import Any

import pytest

ADDON_SOURCE_DIR = Path(__file__).resolve().parents[2]  # .../halpi2_power

# Captured verbatim from halpid 5.1.1 on real hardware -- the same sample the
# unit tests assert against. These are halpid's real field names (V_in, T_mcu,
# state, ...), which the sensor map converts to HA entities.
VALUES_SAMPLE: dict[str, Any] = {
    "5v_output_enabled": True,
    "I_in": 0.24633178114891052,
    "T_mcu": 324.5357971191406,
    "T_pcb": 311.7718200683594,
    "V_cap": 10.35076904296875,
    "V_in": 13.8134765625,
    "daemon_version": "5.1.1",
    "device_id": "0011223344556677",
    "firmware_version": "3.3.1",
    "hardware_version": "N/A",
    "num_leds": 5,
    "state": "OperationalCoOp",
    "watchdog_elapsed": 0.0,
    "watchdog_enabled": True,
    "watchdog_timeout": 10.0,
}


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
    """Build (or reuse) the real halpi2_power add-on image.

    If ``HALPI2_POWER_IMAGE`` is set, trust the caller (CI prebuild via
    docker/build-push-action with a gha cache) and use that tag.
    """
    prebuilt = os.environ.get("HALPI2_POWER_IMAGE")
    if prebuilt:
        return prebuilt
    tag = f"halpi2_power_e2e:{uuid.uuid4().hex[:8]}"
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
# fake halpid: serves the UNIX-socket /values + /version API in the container
# ---------------------------------------------------------------------------

# A stand-in for the aarch64 halpid binary, bind-mounted over /usr/bin/halpid.
# The real halpid.conf argument is ignored; it just serves the sample so the
# real publisher has a socket to read. Runs until SIGTERM (the s6 stop path).
_FAKE_HALPID = f"""#!/usr/bin/python3
import json, os, signal, socketserver
from http.server import BaseHTTPRequestHandler

VALUES = {VALUES_SAMPLE!r}
VERSION = {{"version": "5.1.1"}}
SOCK = "/run/halpid/halpid.sock"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.rstrip("/")
        if path == "/values":
            body = json.dumps(VALUES).encode()
        elif path == "/version":
            body = json.dumps(VERSION).encode()
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a):
        pass


class UnixHTTPServer(socketserver.UnixStreamServer):
    # BaseHTTPRequestHandler wants a (host, port) client address tuple.
    def get_request(self):
        request, _ = super().get_request()
        return request, ("unix", 0)


os.makedirs("/run/halpid", exist_ok=True)
try:
    os.unlink(SOCK)
except FileNotFoundError:
    pass
signal.signal(signal.SIGTERM, lambda *_a: os._exit(0))
UnixHTTPServer(SOCK, Handler).serve_forever()
"""


@pytest.fixture(scope="session")
def fake_halpid(tmp_path_factory: pytest.TempPathFactory) -> str:
    path = tmp_path_factory.mktemp("halpid") / "halpid"
    path.write_text(_FAKE_HALPID)
    path.chmod(0o755)
    return str(path)


# ---------------------------------------------------------------------------
# Mosquitto broker (host: a local binary, or docker --network host)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


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
# the add-on container under test (the real shipped artifact)
# ---------------------------------------------------------------------------


class AddonContainer:
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
def addon_container(
    addon_image: str,
    fake_halpid: str,
    mosquitto: tuple[str, int],
    tmp_path: Path,
) -> Iterator[AddonContainer]:
    mqtt_host, mqtt_port = mosquitto
    options = {
        "mode": "coop",
        "watchdog_timeout_ms": 10000,
        "power_outage_time_limit": 5.0,
        "power_outage_voltage_limit": 9.0,
        "interval_seconds": 1,
        "shutdown_via": "mqtt_only",  # never let the fake trigger a host poweroff
        "mqtt_host": mqtt_host,
        "mqtt_port": mqtt_port,
        "mqtt_discovery_prefix": "homeassistant",
        "mqtt_base_topic": "halpi2_power",
        "client_id": "halpi2-power-e2e",
        "expire_after_multiplier": 4,
        "log_level": "INFO",
    }
    data = tmp_path / "data"
    data.mkdir()
    (data / "options.json").write_text(json.dumps(options))

    name = f"halpi2_e2e_{uuid.uuid4().hex[:10]}"
    # --network host: the publisher reaches the host-run broker on 127.0.0.1.
    # The fake halpid stands in for the aarch64 binary so the s6 tree runs and
    # the /values socket exists. No --rm: a crashed container's logs survive for
    # the failure message; the fixture removes it.
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        "host",
        "-v",
        f"{data / 'options.json'}:/data/options.json:ro",
        "-v",
        f"{fake_halpid}:/usr/bin/halpid:ro",
        addon_image,
    )
    handle = AddonContainer(name)
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
    client = mqtt.Client(client_id="halpi2-e2e-observer")

    def _on_connect(c: Any, *_a: Any) -> None:
        c.subscribe("homeassistant/#")
        c.subscribe("halpi2_power/#")

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
