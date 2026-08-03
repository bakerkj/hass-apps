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


class _HtmlStubHandler(http.server.BaseHTTPRequestHandler):
    """Upstream device UI serving text/html so ``sub_filter`` can rewrite it.

    ``<head lang="en">`` (attributed opening tag) is the common real-world
    spelling that a literal ``<head>`` string match would miss; using it here
    keeps the anchor honest.
    """

    def do_GET(self) -> None:
        body = (
            b'<!doctype html><html><head lang="en">'
            b"<title>upstream</title></head><body>upstream-html-marker</body></html>"
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a: object) -> None:
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


@pytest.fixture
def stub_html_upstream() -> Iterator[int]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _HtmlStubHandler)
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


def _assert_proxied_ok(name: str, url: str, host: str) -> Any:
    """Poll ``url`` (up to 30s) for the proxied 200; assert the shipped
    surgery (X-Frame-Options / CSP stripped, Set-Cookie domain rewritten from
    upstream 127.0.0.1 to request host). Returns the response so tests can add
    scheme-specific assertions. On failure, dumps container state + logs."""
    deadline = time.monotonic() + 30
    resp = None
    while time.monotonic() < deadline:
        resp = _get(url, host)
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
    assert resp.headers.get("X-Frame-Options") is None
    assert resp.headers.get("Content-Security-Policy") is None

    set_cookie = resp.headers.get("Set-Cookie", "")
    assert "session=abc" in set_cookie
    assert "127.0.0.1" not in set_cookie, set_cookie
    assert host in set_cookie, set_cookie
    return resp


def _assert_config_rejected(
    sites: list[dict], expected_log: str, tmp_path: Path
) -> None:
    """Start the addon with a site config the run.sh guards should reject;
    assert the container exits and its log names the offending field."""
    image = _build_image()
    name = _run_addon(image, sites, tmp_path)
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            running = _docker(
                "inspect", "-f", "{{.State.Running}}", name, check=False
            ).strip()
            if running == "false":
                break
            time.sleep(0.5)
        logs = _docker("logs", name, check=False)
        assert expected_log in logs, logs
    finally:
        _docker("rm", "-f", name, check=False)


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
        _assert_proxied_ok(name, f"http://127.0.0.1:{LISTEN_PORT}/", PROXY_HOST)
    finally:
        _docker("rm", "-f", name, check=False)


def test_head_prepend_splices_before_head_close(
    stub_html_upstream: int, tmp_path: Path
) -> None:
    """``head_prepend`` must land immediately before ``</head>``, survive an
    attributed opening tag (the stub uses ``<head lang="en">``), and pass the
    operator's HTML through verbatim -- the addon must not interpret or wrap
    it."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    snippet = '<meta name="test-marker" content="head-prepend-worked">'
    sites = [
        {
            "name": "device",
            "upstream": "127.0.0.1",
            "upstream_port": stub_html_upstream,
            "listen_port": LISTEN_PORT,
            "head_prepend": snippet,
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
        assert resp is not None and resp.status == 200, (
            f"proxy never served 200: {_docker('logs', name, check=False)[-2000:]}"
        )
        body = resp.read().decode()
        assert "upstream-html-marker" in body
        assert snippet + "</head>" in body
    finally:
        _docker("rm", "-f", name, check=False)


def test_head_prepend_absent_leaves_body_untouched(
    stub_html_upstream: int, tmp_path: Path
) -> None:
    """No ``head_prepend`` (default) must pass the upstream HTML through
    byte-for-byte -- confirms the feature is truly opt-in."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    sites = [
        {
            "name": "device",
            "upstream": "127.0.0.1",
            "upstream_port": stub_html_upstream,
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
        assert resp is not None and resp.status == 200
        body = resp.read().decode()
        assert "upstream-html-marker" in body
        assert "test-marker" not in body
    finally:
        _docker("rm", "-f", name, check=False)


def test_head_prepend_rejects_single_quote(tmp_path: Path) -> None:
    """A single quote in ``head_prepend`` would break out of the nginx
    single-quoted ``sub_filter`` string; the guard must refuse it before the
    config file is generated."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    _assert_config_rejected(
        [
            {
                "name": "bad",
                "upstream": "127.0.0.1",
                "listen_port": LISTEN_PORT,
                "head_prepend": "<script>x='bad'</script>",
            }
        ],
        "must not contain single quotes",
        tmp_path,
    )


def test_head_prepend_rejects_head_close(tmp_path: Path) -> None:
    """``</head>`` inside the value would corrupt the sub_filter anchor, so
    the guard must reject any casing of it."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    _assert_config_rejected(
        [
            {
                "name": "bad",
                "upstream": "127.0.0.1",
                "listen_port": LISTEN_PORT,
                "head_prepend": "<script>x=1</SCRIPT></HEAD>",
            }
        ],
        "must not contain </head>",
        tmp_path,
    )


def test_https_upstream_proxied_through_self_signed(
    stub_https_upstream: int, tmp_path: Path
) -> None:
    """upstream_scheme=https + upstream_ssl_verify=false: the proxy should
    connect via TLS to a self-signed stub, skip peer verification, and still
    perform the same X-Frame-Options / cookie surgery as the http path.
    Covers the SNI + proxy_ssl_verify off branch emitted by run.sh."""
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
        _assert_proxied_ok(name, f"http://127.0.0.1:{LISTEN_PORT}/", PROXY_HOST)
    finally:
        _docker("rm", "-f", name, check=False)


def test_https_upstream_verify_on_rejects_self_signed(
    stub_https_upstream: int, tmp_path: Path
) -> None:
    """upstream_ssl_verify=true: nginx must find /etc/ssl/cert.pem to start
    (proves the ca-certificates apk pin), then must actually enforce the check
    -- our stub's self-signed cert is not in the system CA bundle, so the
    upstream handshake fails and nginx returns 502. If /etc/ssl/cert.pem were
    missing, nginx -t would fail and the container would exit before serving
    anything; if verify=on were silently downgraded, we'd see the same 200 the
    verify=off test asserts. This test catches both regressions."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    image = _build_image()
    sites = [
        {
            "name": "device",
            "upstream": "127.0.0.1",
            "upstream_port": stub_https_upstream,
            "upstream_scheme": "https",
            "upstream_ssl_verify": True,
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
            if resp is not None:
                break
            time.sleep(0.5)
        # The container must be running (nginx -t passed => CA bundle present)
        # and the proxy must have returned a non-200 upstream-error status.
        running = _docker(
            "inspect", "-f", "{{.State.Running}}", name, check=False
        ).strip()
        assert running == "true", (
            f"container exited (nginx likely failed to start -- CA bundle?): "
            f"{_docker('logs', name, check=False)[-3000:]}"
        )
        assert resp is not None, "no response at all from listen port"
        assert resp.status in (502, 504), (
            f"expected upstream-verify failure (502/504), got {resp.status}: "
            f"{_docker('logs', name, check=False)[-2000:]}"
        )
    finally:
        _docker("rm", "-f", name, check=False)


def test_invalid_upstream_is_rejected(tmp_path: Path) -> None:
    """A non-host/IP upstream (config injection attempt) must fail loudly: the
    container logs a fatal error and exits rather than generating nginx config."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    _assert_config_rejected(
        [
            {
                "name": "evil",
                "upstream": "127.0.0.1; return 500",
                "listen_port": LISTEN_PORT,
            }
        ],
        "invalid upstream",
        tmp_path,
    )


def test_invalid_upstream_scheme_is_rejected(tmp_path: Path) -> None:
    """Bypasses the Supervisor JSON schema (tests write options.json direct);
    the run.sh guard on ${scheme} must still refuse anything other than
    http/https so a stale supervisor manifest or hand-edited config can't
    inject arbitrary text into the generated proxy_pass directive."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    _assert_config_rejected(
        [
            {
                "name": "bad",
                "upstream": "127.0.0.1",
                "upstream_scheme": "gopher",
                "listen_port": LISTEN_PORT,
            }
        ],
        "invalid upstream_scheme",
        tmp_path,
    )


def test_invalid_upstream_ssl_verify_is_rejected(tmp_path: Path) -> None:
    """Symmetric guard for upstream_ssl_verify -- must be strict true|false
    even when a non-bool slips past Supervisor validation."""
    if not _have_docker():
        pytest.skip("docker daemon not reachable")

    _assert_config_rejected(
        [
            {
                "name": "bad",
                "upstream": "127.0.0.1",
                "upstream_scheme": "https",
                "upstream_ssl_verify": "maybe",
                "listen_port": LISTEN_PORT,
            }
        ],
        "invalid upstream_ssl_verify",
        tmp_path,
    )
