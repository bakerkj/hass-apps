# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""End-to-end: build the REAL dashboard_web_proxy image and run it against a
stub upstream, verifying the shipped artifact actually reverse-proxies and
performs the header surgery that makes a device UI embeddable -- not just that
the image builds.

The addon runs with ``--network host`` so its nginx binds :18800 on the host and
can reach the stub upstream on loopback; the request carries a distinct ``Host``
header so the ``proxy_cookie_domain`` rewrite (upstream host -> request host) is
observable. No Docker Hub pull: the stub is a host thread, the only image is the
add-on's own (built from the Dockerfile default).

Run: pytest dashboard_web_proxy/tests/integration/ -m e2e   (requires docker;
the default pytest run excludes the e2e marker).
"""

import http.server
import json
import os
import shutil
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.e2e

ADDON_DIR = Path(__file__).resolve().parents[2]  # .../dashboard_web_proxy
LISTEN_PORT = 18800  # must be one of the addon's static ports (18800-18819)
PROXY_HOST = "proxy.test"  # distinct from the upstream so the cookie rewrite shows


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


def _build_image() -> str:
    """CI prebuilds the image (build-push-action + gha cache) and passes its tag
    via DASHBOARD_WEB_PROXY_IMAGE; locally, build it from the add-on source."""
    prebuilt = os.environ.get("DASHBOARD_WEB_PROXY_IMAGE")
    if prebuilt:
        return prebuilt
    tag = f"dwp_e2e:{uuid.uuid4().hex[:8]}"
    _docker("build", "--build-arg", "BUILD_VERSION=e2e", "-t", tag, str(ADDON_DIR))
    return tag


class _StubHandler(http.server.BaseHTTPRequestHandler):
    """Upstream device UI: sets the frame-blocking headers and a Domain cookie
    the proxy is supposed to strip / rewrite."""

    def do_GET(self) -> None:
        body = b"hello from upstream device"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("X-Frame-Options", "SAMEORIGIN")
        self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
        self.send_header("Set-Cookie", "session=abc; Domain=127.0.0.1; Path=/")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a: object) -> None:  # silence the default stderr spam
        pass


@pytest.fixture
def stub_upstream() -> Iterator[int]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _StubHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()


def _make_self_signed_cert(tmp_path: Path) -> tuple[Path, Path]:
    """Generate a throwaway self-signed cert/key via openssl.

    Only used by the HTTPS-upstream test to model the "device has a self-signed
    cert" case that motivates ``upstream_ssl_verify: false`` in the addon.
    """
    key = tmp_path / "stub.key"
    crt = tmp_path / "stub.crt"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "1",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(crt),
            "-subj",
            "/CN=stub-upstream",
            "-addext",
            "subjectAltName=DNS:127.0.0.1,IP:127.0.0.1",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return crt, key


@pytest.fixture
def stub_https_upstream(tmp_path: Path) -> Iterator[int]:
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available to mint a self-signed cert")
    crt, key = _make_self_signed_cert(tmp_path)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(crt), keyfile=str(key))
    server = http.server.HTTPServer(("127.0.0.1", 0), _StubHandler)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()


def _run_addon(image: str, sites: list[dict], tmp_path: Path) -> str:
    """Start the add-on container with --network host and the given sites."""
    (tmp_path / "options.json").write_text(
        json.dumps({"sites": sites, "log_level": "info"})
    )
    name = f"dwp_e2e_{uuid.uuid4().hex[:10]}"
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        "host",
        "-v",
        f"{tmp_path / 'options.json'}:/data/options.json:ro",
        image,
    )
    return name


def _get(url: str, host: str) -> Any:
    req = urllib.request.Request(url, headers={"Host": host})
    try:
        return urllib.request.urlopen(req, timeout=2.0)
    except urllib.error.HTTPError as e:
        return e  # an error response is still a response (has .status/.headers)
    except urllib.error.URLError, OSError:
        return None


def test_proxy_strips_frame_headers_and_rewrites_cookie(
    stub_upstream: int, tmp_path: Path
) -> None:
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    sites = [
        {
            "name": "device",
            "upstream": "127.0.0.1",
            "upstream_port": stub_upstream,
            "listen_port": LISTEN_PORT,
        }
    ]
    name = _run_addon(image, sites, tmp_path)
    try:
        url = f"http://127.0.0.1:{LISTEN_PORT}/"
        deadline = time.monotonic() + 30
        resp = None
        while time.monotonic() < deadline:
            resp = _get(url, PROXY_HOST)
            if resp is not None and resp.status == 200:
                break
            time.sleep(0.5)
        if resp is None or resp.status != 200:
            pytest.fail(
                f"proxy never served 200 on {url}; "
                f"running={_docker('inspect', '-f', '{{.State.Running}}', name, check=False).strip()}\n"
                f"{_docker('logs', name, check=False)[-3000:]}"
            )

        body = resp.read().decode()
        assert "hello from upstream device" in body  # actually proxied

        # the frame-blocking headers are stripped so HA can iframe it
        assert resp.headers.get("X-Frame-Options") is None
        assert resp.headers.get("Content-Security-Policy") is None

        # the session cookie's Domain is rewritten from the upstream (127.0.0.1)
        # to the request host, so login sticks through the proxy
        set_cookie = resp.headers.get("Set-Cookie", "")
        assert "session=abc" in set_cookie
        assert "127.0.0.1" not in set_cookie, set_cookie
        assert PROXY_HOST in set_cookie, set_cookie
    finally:
        _docker("rm", "-f", name, check=False)


def test_https_upstream_proxied_through_self_signed(
    stub_https_upstream: int, tmp_path: Path
) -> None:
    """upstream_scheme=https + upstream_ssl_verify=false: the proxy should
    connect via TLS to a self-signed stub, skip peer verification, and still
    perform the same X-Frame-Options / cookie surgery as the http path.
    Covers the SNI + proxy_ssl_verify block emitted by run.sh."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    sites = [
        {
            "name": "device",
            "upstream": "127.0.0.1",
            "upstream_port": stub_https_upstream,
            "upstream_scheme": "https",
            "upstream_ssl_verify": False,
            "listen_port": LISTEN_PORT,
        }
    ]
    name = _run_addon(image, sites, tmp_path)
    try:
        url = f"http://127.0.0.1:{LISTEN_PORT}/"
        deadline = time.monotonic() + 30
        resp = None
        while time.monotonic() < deadline:
            resp = _get(url, PROXY_HOST)
            if resp is not None and resp.status == 200:
                break
            time.sleep(0.5)
        if resp is None or resp.status != 200:
            pytest.fail(
                f"https-upstream proxy never served 200 on {url}; "
                f"running={_docker('inspect', '-f', '{{.State.Running}}', name, check=False).strip()}\n"
                f"{_docker('logs', name, check=False)[-3000:]}"
            )

        body = resp.read().decode()
        assert "hello from upstream device" in body

        assert resp.headers.get("X-Frame-Options") is None
        assert resp.headers.get("Content-Security-Policy") is None

        set_cookie = resp.headers.get("Set-Cookie", "")
        assert "session=abc" in set_cookie
        assert "127.0.0.1" not in set_cookie, set_cookie
        assert PROXY_HOST in set_cookie, set_cookie
    finally:
        _docker("rm", "-f", name, check=False)


def test_invalid_upstream_is_rejected(tmp_path: Path) -> None:
    """A non-host/IP upstream (config injection attempt) must fail loudly: the
    container logs a fatal error and exits rather than generating nginx config."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    sites = [
        {
            "name": "evil",
            "upstream": "127.0.0.1; return 500",
            "listen_port": LISTEN_PORT,
        }
    ]
    name = _run_addon(image, sites, tmp_path)
    try:
        # give it a moment to parse config and exit
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            running = _docker(
                "inspect", "-f", "{{.State.Running}}", name, check=False
            ).strip()
            if running == "false":
                break
            time.sleep(0.5)
        logs = _docker("logs", name, check=False)
        assert "invalid upstream" in logs, logs
    finally:
        _docker("rm", "-f", name, check=False)
