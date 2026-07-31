# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shared e2e scaffolding: the real add-on image, and a mock Supervisor.

No add-on code is stubbed. bashio resolves its endpoint from ``SUPERVISOR_API``
(bashio.sh:29) and ``bashio::config`` fetches ``/addons/self/options/config``
once and caches it, so pointing that variable at a local HTTP stub exercises
the same option-loading path the Supervisor drives in production. Containers
run with ``--network host`` so they can reach the stub on loopback.

There is no RTL-SDR dongle on a test machine, so ``rtl_fm`` always fails to open
a device. That is fine: it fails *after* config validation, sdr.conf rendering
and the process spawn, which is where all of the add-on's own logic lives.
"""

import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

ADDON_DIR = Path(__file__).resolve().parents[2]  # .../direwolf_igate

# Distinctive so an assertion that it never reaches the log cannot pass by
# accident against other digits in the output.
PASSCODE = "31415"

BASE_OPTIONS: dict = {
    "mycall": "N0CALL-10",
    "iglogin_passcode": PASSCODE,
    "igserver": "noam.aprs2.net",
    "frequency": "144.390M",
    "rtl_device": "0",
    "rtl_gain": "",
    "rtl_ppm": 0,
    "latitude": 51.4779,
    "longitude": -0.0015,
    "beacon_comment": "iGate | DireWolf",
    "beacon_symbol": "igate",
    "beacon_overlay": "R",
    "beacon_delay": "0:30",
    "beacon_every": "15:00",
    "send_status_beacon": True,
    "mqtt_enabled": False,
}


def docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.stdout or ""


def have_docker() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        docker("version", "--format", "{{.Server.Version}}")
    except subprocess.CalledProcessError, FileNotFoundError:
        return False
    return True


def require_docker() -> None:
    if not have_docker():
        pytest.skip("docker daemon not reachable; skipping e2e suite")


@pytest.fixture(scope="session")
def addon_image() -> str:
    """CI prebuilds the image (build-push-action + gha cache) and passes its tag
    via DIREWOLF_IGATE_IMAGE; locally, build it from the add-on source."""
    require_docker()
    prebuilt = os.environ.get("DIREWOLF_IGATE_IMAGE")
    if prebuilt:
        return prebuilt
    tag = f"direwolf_e2e:{uuid.uuid4().hex[:8]}"
    docker("build", "--build-arg", "BUILD_VERSION=e2e", "-t", tag, str(ADDON_DIR))
    return tag


class SupervisorStub:
    """Serves the one Supervisor endpoint bashio::config needs.

    ``options`` is swapped per-test before the container starts; bashio caches
    the response after the first fetch, so each container sees one consistent
    snapshot.
    """

    def __init__(self) -> None:
        self.options: dict = {}
        self._tmpdirs: list[Path] = []
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path != "/addons/self/options/config":
                    self.send_error(404)
                    return
                body = json.dumps({"result": "ok", "data": stub.options}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_a: object) -> None:  # silence stderr spam
                pass

        self._server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self._server.server_address[1])

    def serve(self) -> None:
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        for d in self._tmpdirs:
            shutil.rmtree(d, ignore_errors=True)

    def start_addon(self, image: str, options: dict) -> str:
        """Start the add-on detached against this stub; returns the container.

        The Supervisor exposes options two ways and production has both: the
        API that bashio reads (run.sh) and /data/options.json on disk (the
        statistics publisher, matching the sibling MQTT add-ons). Serving only
        one here would leave half the add-on running against config the other
        half never sees.
        """
        self.options = options
        tmp = Path(tempfile.mkdtemp(prefix="direwolf-e2e-"))
        self._tmpdirs.append(tmp)
        (tmp / "options.json").write_text(json.dumps(options))

        name = f"direwolf_e2e_{uuid.uuid4().hex[:10]}"
        docker(
            "run",
            "-d",
            "--name",
            name,
            "--network",
            "host",
            "-v",
            f"{tmp / 'options.json'}:/data/options.json:ro",
            "-e",
            f"SUPERVISOR_API=http://127.0.0.1:{self.port}",
            "-e",
            "SUPERVISOR_TOKEN=e2e-token",
            image,
        )
        return name


@pytest.fixture
def supervisor() -> Iterator[SupervisorStub]:
    stub = SupervisorStub()
    stub.serve()
    try:
        yield stub
    finally:
        stub.close()
