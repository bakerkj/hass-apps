# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: build the REAL signalk add-on image and run it, verifying the
shipped artifact starts the Signal K server as the unprivileged node user, on
its port, with a node-owned config dir -- not just that the image builds. The
build gate already covers the canboatjs native binding; this covers "does it
run".

The image is built from the Dockerfile's own BUILD_FROM default (the base
renovate tracks), so there is no second copy of the version to keep in sync.

Run: pytest signalk/tests/integration/ -m e2e   (requires docker; the default
pytest run excludes the e2e marker).
"""

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

ADDON_DIR = Path(__file__).resolve().parents[2]  # .../signalk
CAN_BINDING = (
    "/home/node/signalk/node_modules/@canboat/canboatjs/build/Release/canSocket.node"
)


def _docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.stdout or ""


def _have_docker() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        _docker("version", "--format", "{{.Server.Version}}")
    except subprocess.CalledProcessError, FileNotFoundError:
        return False
    return True


def _http(url: str, timeout: float = 2.0) -> tuple[int, str] | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.URLError, OSError:
        return None


def _build_image() -> str:
    """CI prebuilds the image (build-push-action + gha cache) and passes its tag
    via SIGNALK_SERVER_IMAGE; locally, build it here from the add-on source."""
    prebuilt = os.environ.get("SIGNALK_SERVER_IMAGE")
    if prebuilt:
        return prebuilt
    tag = f"signalk_e2e:{uuid.uuid4().hex[:8]}"
    _docker("build", "--build-arg", "BUILD_VERSION=e2e", "-t", tag, str(ADDON_DIR))
    return tag


def test_signalk_server_starts_as_node() -> None:
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])
    name = f"sk_e2e_{uuid.uuid4().hex[:10]}"

    # Map the container's 3000 to a free host port. host_network isn't needed --
    # there's no can0 to reach in CI; we only verify the server starts. No --rm,
    # so a crashed container's logs survive for the failure message.
    _docker("run", "-d", "--name", name, "-p", f"127.0.0.1:{port}:3000", _build_image())
    try:
        base = f"http://127.0.0.1:{port}"

        # the Node server takes a few seconds to boot; poll its /signalk endpoint
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and _http(f"{base}/signalk") is None:
            time.sleep(0.5)

        resp = _http(f"{base}/signalk")
        if resp is None:
            running = _docker(
                "inspect", "-f", "{{.State.Running}}", name, check=False
            ).strip()
            logs = _docker("logs", name, check=False)[-3000:]
            pytest.fail(f"server never responded on {base}; running={running}\n{logs}")
        status, body = resp
        assert status == 200
        assert "server" in json.loads(body)  # {"server": {"id": "signalk-server", ...}}

        # the server process runs as the unprivileged node user, not root
        ps = _docker("exec", name, "ps", "-eo", "user,args", check=False)
        procs = [
            ln for ln in ps.splitlines() if "signalk-server" in ln and "grep" not in ln
        ]
        assert procs, f"no signalk-server process found:\n{ps}"
        assert procs[0].split()[0] == "node", f"server not running as node:\n{procs[0]}"

        # its config dir is node-owned (the chown worked) and the CAN binding shipped
        owner = _docker(
            "exec", name, "stat", "-c", "%U", "/data/signalk", check=False
        ).strip()
        assert owner == "node", f"/data/signalk not owned by node: {owner!r}"
        assert (
            subprocess.run(
                ["docker", "exec", name, "test", "-f", CAN_BINDING],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )
    finally:
        _docker("rm", "-f", name, check=False)
