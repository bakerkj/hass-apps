# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Integration-test harness: a real Home Assistant + the real proxy +
a real headless Chrome.

Bring-up costs are paid once per test session via ``scope="session"``
fixtures — HA bootstrap is ~30-60 s, paid once. Each test creates its
own dashboards/data via HA's WebSocket API for isolation.

Run with ``pytest dashboard_entity_proxy/tests/integration/ -m e2e``.

Requirements: docker, a free ``ghcr.io/home-assistant/home-assistant``
image pull, headless Chrome (system ``google-chrome`` + the
``selenium`` package's bundled driver-manager).
"""

import asyncio
import contextlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import websockets
from aiohttp import web
from dashboard_entity_proxy.customization import Customization
from dashboard_entity_proxy.session import SessionRegistry
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions

from dashboard_entity_proxy import proxy as proxy_mod

# Pinned HA version so the integration suite stays deterministic across
# upstream releases. Renovate tracks this via the digest comment below;
# bump deliberately when validating against a new HA.
# renovate: datasource=docker depName=ghcr.io/home-assistant/home-assistant
HA_IMAGE = "ghcr.io/home-assistant/home-assistant:2026.6.2"
HA_BOOT_TIMEOUT = 120

# The addon's docker BUILD_FROM base. Mirrors the value Dockerfile
# defaults to; pinned for reproducibility. Renovate tracks via the
# comment so a bump opens a PR on both this and Dockerfile together.
# renovate: datasource=docker depName=ghcr.io/home-assistant/amd64-base
ADDON_BASE_IMAGE = "ghcr.io/home-assistant/amd64-base:3.24"
ADDON_BOOT_TIMEOUT = 30
ADDON_SOURCE_DIR = Path(__file__).resolve().parents[2]


def _docker_available() -> bool:
    """True if a working docker daemon is reachable."""
    try:
        r = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=5, check=False
        )
        return r.returncode == 0
    except OSError, subprocess.SubprocessError:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip every e2e test in this directory when docker isn't
    available — the harness can't bring HA up, so the tests would all
    error out otherwise.
    """
    if _docker_available():
        return
    skip = pytest.mark.skip(reason="docker not available; HA e2e tests need it")
    for item in items:
        if "e2e" in item.keywords and "dashboard_entity_proxy/tests/integration" in str(
            item.fspath
        ):
            item.add_marker(skip)


USER = {
    "name": "Test User",
    "username": "tester",
    "password": "test_password_for_integration",
    "client_id": "http://localhost/",
    "language": "en",
}


@contextlib.contextmanager
def _phase(label: str) -> Iterator[None]:
    """Log a session-fixture bring-up phase with elapsed time to stderr.

    GHA prefixes every log line with its own timestamp, so the deltas
    surface directly in the job log — useful for seeing where the ~80 s
    of silent pre-first-test time is actually spent.
    """
    start = time.monotonic()
    print(f"[dep-e2e] {label}: start", file=sys.stderr, flush=True)
    try:
        yield
    except BaseException:
        print(
            f"[dep-e2e] {label}: failed after {time.monotonic() - start:.1f}s",
            file=sys.stderr,
            flush=True,
        )
        raise
    else:
        print(
            f"[dep-e2e] {label}: done in {time.monotonic() - start:.1f}s",
            file=sys.stderr,
            flush=True,
        )


from _aiohttp_helpers import _free_port


def _wait_for_ha(url: str, deadline: float) -> None:
    """Block until HA's HTTP responds (any status code) or deadline."""
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/", timeout=2)
            return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"HA at {url} did not become ready in time")


def _http_post_json(
    url: str, payload: dict[str, Any], headers: dict[str, str] | None = None
) -> Any:
    """Sync POST returning parsed JSON. Used for the HA onboarding flow."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _http_post_form(url: str, fields: dict[str, str]) -> Any:
    """Sync POST application/x-www-form-urlencoded. Used by /auth/token."""
    data = urllib.parse.urlencode(fields).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _onboard(ha_url: str) -> dict[str, str]:
    """Run HA's first-boot onboarding flow programmatically.

    Returns a dict with ``access_token`` and ``refresh_token``. All
    four onboarding steps (user, core_config, analytics, integration)
    are completed — leaving any of them pending makes the frontend
    redirect every page to ``/onboarding.html``.
    """
    # Step 1: create the initial user. HA replies with ``auth_code``.
    resp = _http_post_json(
        ha_url + "/api/onboarding/users",
        {
            "client_id": USER["client_id"],
            "name": USER["name"],
            "username": USER["username"],
            "password": USER["password"],
            "language": USER["language"],
        },
    )
    auth_code = resp["auth_code"]
    # Step 2: trade the code for tokens.
    tokens = _http_post_form(
        ha_url + "/auth/token",
        {
            "client_id": USER["client_id"],
            "code": auth_code,
            "grant_type": "authorization_code",
        },
    )
    auth_header = {"Authorization": f"Bearer {tokens['access_token']}"}
    # Step 3+: mark the remaining onboarding steps done. ``core_config``
    # and ``analytics`` accept an empty body. ``integration`` requires
    # ``client_id`` + ``redirect_uri`` and returns its own auth_code
    # (which we discard — we already have working tokens). The
    # ``integration`` step is the final gate that flips the frontend
    # from ``/onboarding.html`` to the regular dashboard router.
    _http_post_json(ha_url + "/api/onboarding/core_config", {}, headers=auth_header)
    _http_post_json(ha_url + "/api/onboarding/analytics", {}, headers=auth_header)
    _http_post_json(
        ha_url + "/api/onboarding/integration",
        {"client_id": USER["client_id"], "redirect_uri": USER["client_id"]},
        headers=auth_header,
    )
    return tokens


def _ws_call(ws_url: str, token: str, cmd: dict[str, Any]) -> dict[str, Any]:
    """Run one WebSocket command against HA and return the result frame."""

    async def go() -> dict[str, Any]:
        async with websockets.connect(ws_url, max_size=None) as ws:
            json.loads(await ws.recv())  # auth_required
            await ws.send(json.dumps({"type": "auth", "access_token": token}))
            json.loads(await ws.recv())  # auth_ok
            cmd_with_id = {"id": 1, **cmd}
            await ws.send(json.dumps(cmd_with_id))
            return json.loads(await ws.recv())

    return asyncio.run(go())


@pytest.fixture(scope="session")
def ha() -> Iterator[dict[str, str]]:
    """Start HA in a docker container; yield a dict with ``url``,
    ``ws_url``, ``access_token``, ``refresh_token``.

    Session-scoped: the ~30-60 s bootstrap is paid once per pytest run.
    """
    port = _free_port()
    config_dir = Path(tempfile.mkdtemp(prefix="ha_test_"))
    # Bake a deterministic set of test entities so individual tests can
    # put them in / out of scope without each one needing to provision
    # its own integration. The names ``int.test_*`` give tests a stable
    # vocabulary for assertions.
    (config_dir / "configuration.yaml").write_text(
        "default_config:\n"
        "frontend:\n"
        "input_boolean:\n"
        "  int_test_a: {name: Int Test A}\n"
        "  int_test_b: {name: Int Test B}\n"
        "  int_test_c: {name: Int Test C}\n"
        "  int_test_d: {name: Int Test D}\n"
        "input_text:\n"
        "  int_text_a: {name: Int Text A, initial: hello}\n"
        "  int_text_b: {name: Int Text B, initial: world}\n"
        # A template weather entity that supports ``weather/subscribe_forecast``
        # so tests can exercise the namespaced-subscription routing
        # path without depending on a real weather integration. The
        # ``forecasts:`` section declares a daily forecast template,
        # without which HA replies ``forecast_not_supported`` to a
        # ``forecast_type: daily`` subscribe request.
        "template:\n"
        "  - weather:\n"
        "      name: Int Test Weather\n"
        '      condition_template: "sunny"\n'
        '      temperature_template: "70"\n'
        '      temperature_unit: "°F"\n'
        '      humidity_template: "50"\n'
        "      forecasts:\n"
        '        - day_template: "{{ now().isoformat() }}"\n'
        '          condition_template: "sunny"\n'
        '          temperature_template: "72"\n'
        '          templow_template: "55"\n'
    )
    container = f"ha_int_test_{port}"

    # Clean up any stray container with this name.
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    with _phase("ha container start"):
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                "-p",
                f"{port}:8123",
                "-v",
                f"{config_dir}:/config",
                HA_IMAGE,
            ],
            check=True,
            capture_output=True,
        )

    url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/api/websocket"
    try:
        with _phase("ha http ready"):
            _wait_for_ha(url, time.time() + HA_BOOT_TIMEOUT)
        with _phase("ha onboarding"):
            tokens = _onboard(url)
        yield {
            "url": url,
            "ws_url": ws_url,
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "config_dir": str(config_dir),
            "container": container,
        }
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container], capture_output=True, check=False
        )
        shutil.rmtree(config_dir, ignore_errors=True)


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    """Block until ``host:port`` accepts a TCP connection or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except TimeoutError, OSError:
            time.sleep(0.5)
    raise TimeoutError(f"{host}:{port} not reachable within {timeout}s")


# Path to the production nginx config template. We render it for tests
# instead of maintaining a parallel copy: that way every production
# change (B8 location split, brotli/zstd/gzip, access_log, etc.) flows
# into the integration suite automatically. The drift between this
# template and the test harness has historically been a source of bugs
# that only surfaced after deploy.
_NGINX_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2] / "rootfs/etc/nginx/nginx.conf.tmpl"
)


def _render_nginx_conf(listen_port: int, ha_port: int) -> str:
    """Render the production nginx.conf template with test-specific
    substitutions. Matches what ``rootfs/usr/bin/render-nginx-conf`` does
    at addon boot, plus a listen-port override so several parallel
    integration runs don't collide on a single shared 8126.

    ``transparent: true`` is implied for the test HA (which trusts no
    X-Forwarded-* proxies anyway), so ``__XFWD_BLOCK__`` resolves empty.
    """
    template = _NGINX_TEMPLATE_PATH.read_text()
    return (
        template.replace("__HA_HOST__", "127.0.0.1")
        .replace("__HA_PORT__", str(ha_port))
        .replace("__XFWD_BLOCK__", "")
        # Match the INFO-level form ``render-nginx-conf`` produces. The
        # test harness never flips log_level to DEBUG, so the gated
        # ``if=$nginx_loggable`` directive is the correct shape here.
        .replace(
            "__STDERR_ACCESS_LOG__",
            "access_log /dev/stderr combined if=$nginx_loggable;",
        )
        .replace("listen 8126 default_server", f"listen {listen_port} default_server")
        .replace(
            "listen [::]:8126 default_server",
            f"listen [::]:{listen_port} default_server",
        )
    )


# Empty stand-in for /usr/bin/render-nginx-conf so the init-nginx-conf
# cont-init step doesn't overwrite the test-rendered nginx.conf we
# bind-mount above.
_RENDER_NOOP = "#!/command/with-contenv bash\nexit 0\n"


@pytest.fixture(scope="session")
def addon_image() -> Iterator[str]:
    """Build the addon docker image once per session from the local
    source tree. Returns the image tag.

    Uses ``docker buildx build --load`` against the same base image
    config.json points to; the resulting image is what ships to users —
    nginx + python + s6 services + the rootfs/ overlay.

    CI pre-builds the image with a GHA-backed buildkit cache and hands
    us the tag via ``DEP_PROXY_IMAGE``. In that mode we skip both the
    build and the cleanup (CI handles teardown).
    """
    if pre_built := os.environ.get("DEP_PROXY_IMAGE"):
        print(
            f"[dep-e2e] addon image: using prebuilt {pre_built}",
            file=sys.stderr,
            flush=True,
        )
        yield pre_built
        return
    tag = f"dep_int_test:{int(time.time())}"
    with _phase("addon docker build"):
        subprocess.run(
            [
                "docker",
                "buildx",
                "build",
                "--build-arg",
                f"BUILD_FROM={ADDON_BASE_IMAGE}",
                "--build-arg",
                "BUILD_VERSION=int-test",
                "--load",
                "-t",
                tag,
                str(ADDON_SOURCE_DIR),
            ],
            check=True,
            capture_output=True,
        )
    yield tag
    subprocess.run(["docker", "rmi", "-f", tag], capture_output=True, check=False)


@pytest.fixture(scope="session")
def proxy_url(addon_image: str, ha: dict[str, str]) -> Iterator[str]:
    """Run the addon container, pointed at the test HA. Yields the
    nginx-fronted URL.

    Session-scoped: one container instance shared across tests. The
    container exercises the same nginx + s6 + python stack that ships
    to users, so tests catch nginx-config bugs (WebSocket upgrade
    headers, timeout settings, …) as well as Python-side regressions.
    """
    ha_parts = urllib.parse.urlsplit(ha["url"])
    assert ha_parts.port is not None, (
        "ha fixture must publish a url with an explicit port"
    )
    addon_port = _free_port()
    # Write options + an nginx override (the override only swaps the
    # listen port so several parallel runs don't collide on 8126).
    options_path = Path(tempfile.mkstemp(suffix=".json")[1])
    options_path.write_text(
        json.dumps(
            {
                "homeassistant_url": f"http://127.0.0.1:{ha_parts.port}",
                "transparent": True,
                "state_update_interval": "",
                "extra_entities": [],
                "include_entity_globs": [],
                "exclude_entity_globs": [],
                "passthrough_all": False,
                "log_level": "INFO",
                "customization_file": "",
                "show_client_paths": True,
                "registry_mode": "incremental",
                # 0 disables; integration tests don't want timer
                # flakiness from the periodic refresh.
                "registry_refetch_interval": 0,
                "registry_burst_threshold": 50,
            }
        )
    )
    nginx_conf_path = options_path.parent / f"dep_int_nginx_{addon_port}.conf"
    nginx_conf_path.write_text(_render_nginx_conf(addon_port, ha_parts.port))
    # No-op stand-in for the init-nginx-conf cont-init renderer so it
    # doesn't overwrite the bind-mounted nginx.conf above.
    render_noop_path = options_path.parent / f"dep_int_render_noop_{addon_port}.sh"
    render_noop_path.write_text(_RENDER_NOOP)
    render_noop_path.chmod(0o755)

    container = f"dep_int_addon_{addon_port}"
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    with _phase("addon container start"):
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                # ``--network host`` is the simplest way to give the
                # container reach into HA on the host's loopback. Each
                # test run picks a fresh ``addon_port`` so parallel
                # invocations don't collide on a single shared 8126.
                "--network",
                "host",
                "-v",
                f"{options_path}:/data/options.json:ro",
                "-v",
                f"{nginx_conf_path}:/etc/nginx/nginx.conf:ro",
                "-v",
                f"{render_noop_path}:/usr/bin/render-nginx-conf:ro",
                addon_image,
            ],
            check=True,
            capture_output=True,
        )
    try:
        with _phase("addon port ready"):
            _wait_for_port("127.0.0.1", addon_port, ADDON_BOOT_TIMEOUT)
        yield f"http://127.0.0.1:{addon_port}"
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container], capture_output=True, check=False
        )
        options_path.unlink(missing_ok=True)
        render_noop_path.unlink(missing_ok=True)
        nginx_conf_path.unlink(missing_ok=True)


def _spawn_proxy(
    target_url: str, **option_overrides: Any
) -> tuple[str, threading.Event]:
    """Start a proxy instance pointed at ``target_url`` in a background
    asyncio loop. Returns ``(url, stop_event)``: set the event to ask
    the proxy to shut down. Caller owns lifecycle.

    Any keyword passed via ``option_overrides`` is forwarded to
    :class:`proxy_mod.Options`; ``target_url`` and the default
    ``SessionRegistry`` / ``Customization`` are set automatically.

    If the background thread's ``main()`` raises before ``started`` flips,
    the exception is captured and re-raised on the main thread as the
    ``__cause__`` of a ``RuntimeError`` — otherwise the failure shows up
    as a misleading "proxy did not start in time" timeout.
    """
    port = _free_port()
    started = threading.Event()
    stop = threading.Event()
    thread_exc: list[BaseException] = []

    def run() -> None:
        async def main() -> None:
            opts = proxy_mod.Options(
                target_url=target_url,
                registry=SessionRegistry(),
                customization=option_overrides.pop("customization", Customization()),
                # Disable periodic refetch by default so tests are
                # deterministic; per-test overrides can re-enable it.
                registry_refetch_interval=option_overrides.pop(
                    "registry_refetch_interval", 0.0
                ),
                **option_overrides,
            )
            app = proxy_mod.create_app(opts)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            started.set()
            while not stop.is_set():
                await asyncio.sleep(0.1)
            await runner.cleanup()

        try:
            asyncio.new_event_loop().run_until_complete(main())
        except BaseException as exc:  # noqa: BLE001 — forward across threads
            thread_exc.append(exc)
            # Wake the main thread so it doesn't sit in started.wait().
            started.set()

    threading.Thread(target=run, daemon=True).start()
    if not started.wait(timeout=10):
        raise RuntimeError("proxy did not start in time")
    if thread_exc:
        raise RuntimeError("proxy thread raised before listen") from thread_exc[0]
    return f"http://127.0.0.1:{port}", stop


@pytest.fixture
def proxy_factory(ha: dict[str, str]):
    """Per-test factory that spins up a fresh proxy with custom options.

    Returns a callable taking the same keyword arguments as
    :class:`proxy_mod.Options` (e.g. ``customization=...``,
    ``registry_refetch_interval=...``). The proxy lives until the test
    ends, then is shut down by the cleanup hook.
    """
    stops: list[threading.Event] = []

    def make(**kwargs: Any) -> str:
        url, stop = _spawn_proxy(ha["url"], **kwargs)
        stops.append(stop)
        return url

    yield make

    for s in stops:
        s.set()


def _hass_tokens_payload(base: str, ha: dict[str, str]) -> dict[str, Any]:
    """Build the ``hassTokens`` localStorage entry the HA frontend expects.

    The access token is a fresh JWT minted from the long-lived
    refresh-token's JWT key — short-circuits the auth-flow redirect so
    Selenium can land on a dashboard immediately.
    """
    return {
        "hassUrl": base,
        "clientId": base + "/",
        "refresh_token": ha["refresh_token"],
        "access_token": ha["access_token"],
        "expires_in": 1800,
        "expires": int(time.time() * 1000) + 1800 * 1000,
        "token_type": "Bearer",
    }


@pytest.fixture(scope="session")
def chrome() -> Iterator[webdriver.Chrome]:
    """One headless Chrome instance, shared across tests."""
    opts = ChromeOptions()
    for arg in (
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--window-size=1280,800",
    ):
        opts.add_argument(arg)
    opts.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    with _phase("chrome driver start"):
        drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(30)
    yield drv
    drv.quit()


@pytest.fixture
def browser(chrome: webdriver.Chrome, ha: dict[str, str], proxy_url: str):
    """A pre-authed browser pointed at the proxy. Per-test fixture so
    each test starts with cleared session/local storage.
    """

    def visit(url: str) -> webdriver.Chrome:
        # ``url`` can be an absolute URL (``http://...``) or a path
        # (``/lighting-test/0``); the path form is normalised against
        # the proxy base. ``urllib.parse`` does the assembly so we don't
        # hand-roll fragile string-splitting.
        if url.startswith("/"):
            base_parts = urllib.parse.urlsplit(proxy_url)
            target = urllib.parse.urlunsplit(base_parts._replace(path=url))
        else:
            base_parts = urllib.parse.urlsplit(url)
            target = url
        base = urllib.parse.urlunsplit(
            (base_parts.scheme, base_parts.netloc, "", "", "")
        )
        chrome.get(base + "/")
        chrome.execute_script(
            "window.localStorage.setItem('hassTokens', arguments[0]);",
            json.dumps(_hass_tokens_payload(base, ha)),
        )
        # Drain pre-existing console logs so each test sees its own.
        with contextlib.suppress(Exception):
            chrome.get_log("browser")
        chrome.get(target)
        return chrome

    yield visit

    # Per-test cleanup: clear storage so the next test starts fresh.
    with contextlib.suppress(Exception):
        chrome.execute_script(
            "window.localStorage.clear(); window.sessionStorage.clear();"
        )
        chrome.delete_all_cookies()


@pytest.fixture
def ha_ws(ha: dict[str, str]):
    """Helper that runs one synchronous WS command against the test HA
    using the bootstrapped access token. Returns the result frame.
    """

    def call(cmd: dict[str, Any]) -> dict[str, Any]:
        return _ws_call(ha["ws_url"], ha["access_token"], cmd)

    return call
