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
import shutil
import subprocess
import sys
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

# Compose stack: the HA image tag is pinned in docker-compose.yml (the
# single source of truth; Renovate tracks it via the file). Fixture-
# side lifecycle happens through ``docker compose up/down`` — no image
# constant needed here.
_COMPOSE_FILE = Path(__file__).parent / "docker-compose.yml"
# The compose file bind-mounts a few files from this dir into the
# ``dep-proxy`` service. Prepared by the ``_compose_stack`` fixture
# below before ``compose up`` runs.
_STACK_DIR = Path("/tmp/dep-proxy-e2e")


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


def _compose_port(service: str, container_port: int) -> int:
    """Return the host port compose published ``service:container_port`` on.

    ``docker compose port`` prints ``0.0.0.0:PORT`` (or ``[::]:PORT`` on
    v6-first daemons) for the requested mapping.
    """
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(_COMPOSE_FILE),
            "port",
            service,
            str(container_port),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(result.stdout.strip().rsplit(":", 1)[1])


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


# The addon's /data/options.json content. Deterministic across the whole
# session — no per-test variation needed here (per-test proxy configs
# use ``proxy_factory`` below, which spawns fresh Python proxies).
_OPTIONS = {
    "homeassistant_url": "http://ha:8123",
    "transparent": True,
    "state_update_interval": "",
    "extra_entities": [],
    "include_entity_globs": [],
    "exclude_entity_globs": [],
    "passthrough_all": False,
    "log_level": "INFO",
    "customization_file": "",
    "show_client_paths": True,
    # 0 disables; integration tests don't want timer flakiness from the
    # periodic refresh.
    "registry_mode": "incremental",
    "registry_refetch_interval": 0,
    "registry_burst_threshold": 50,
}


@pytest.fixture(scope="session", autouse=True)
def _compose_stack() -> Iterator[None]:
    """Bring up the compose stack (ha + dep-proxy) and tear it down after
    the session.

    ``docker compose up -d --wait`` blocks until both services'
    healthchecks pass — replaces the pre-compose ``_wait_for_ha`` /
    ``_wait_for_port`` polling. Bind-mount files under ``_STACK_DIR`` are
    prepared here because they're generated from the shipped nginx
    template (rendered once, per-session) — not something the checked-in
    compose file can hold static.
    """
    # Belt-and-suspenders: an aborted prior session (or a manual
    # ``compose up`` before file prep) leaves compose containers holding
    # bind-mount refs, and docker creates empty root-owned dirs at the
    # missing bind-source paths. Bring any prior stack down first, then
    # clean the tmp dir via a rootful container (own-user rm can't
    # remove root-owned entries).
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "down", "-v"],
        check=False,
        capture_output=True,
    )
    if _STACK_DIR.exists():
        # Bind the parent and remove _STACK_DIR itself — a prior aborted
        # ``compose up`` may have made the dir (and its entries)
        # root-owned, which pytest's user can't rm and can't overwrite.
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{_STACK_DIR.parent}:/x",
                "alpine:3.20",
                "sh",
                "-c",
                f"rm -rf /x/{_STACK_DIR.name}",
            ],
            check=False,
            capture_output=True,
        )
    _STACK_DIR.mkdir(parents=True, exist_ok=True)
    (_STACK_DIR / "options.json").write_text(json.dumps(_OPTIONS))
    (_STACK_DIR / "nginx.conf").write_text(_render_nginx_conf())
    render_noop = _STACK_DIR / "render-nginx-conf"
    render_noop.write_text(_RENDER_NOOP)
    render_noop.chmod(0o755)

    with _phase("compose up"):
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(_COMPOSE_FILE),
                "up",
                "-d",
                "--wait",
            ],
            check=True,
            capture_output=True,
        )
    try:
        yield
    finally:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(_COMPOSE_FILE),
                "down",
                "-v",
            ],
            check=False,
            capture_output=True,
        )
        shutil.rmtree(_STACK_DIR, ignore_errors=True)


@pytest.fixture(scope="session")
def ha(_compose_stack: None) -> dict[str, str]:
    """Return a dict with ``url``, ``ws_url``, ``access_token``,
    ``refresh_token`` for the compose-started HA. Onboarding runs on the
    first request (~2 s) so the first test to depend on ``ha`` triggers
    it — subsequent tests reuse the session-scoped result.
    """
    ha_port = _compose_port("ha", 8123)
    url = f"http://127.0.0.1:{ha_port}"
    ws_url = f"ws://127.0.0.1:{ha_port}/api/websocket"
    with _phase("ha onboarding"):
        tokens = _onboard(url)
    return {
        "url": url,
        "ws_url": ws_url,
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
    }


# Path to the production nginx config template. We render it for tests
# instead of maintaining a parallel copy: that way every production
# change (B8 location split, brotli/zstd/gzip, access_log, etc.) flows
# into the integration suite automatically. The drift between this
# template and the test harness has historically been a source of bugs
# that only surfaced after deploy.
_NGINX_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2] / "rootfs/etc/nginx/nginx.conf.tmpl"
)


def _render_nginx_conf() -> str:
    """Render the production nginx.conf template with test-specific
    substitutions. Matches what ``rootfs/usr/bin/render-nginx-conf`` does
    at addon boot.

    The addon runs on the compose network and reaches HA via DNS
    (``http://ha:8123``) — no per-session port template needed. Listen
    port stays at the shipped 8126.

    ``transparent: true`` is implied for the test HA (which trusts no
    X-Forwarded-* proxies anyway), so ``__XFWD_BLOCK__`` resolves empty.
    """
    template = _NGINX_TEMPLATE_PATH.read_text()
    return (
        template.replace("__HA_HOST__", "ha")
        .replace("__HA_PORT__", "8123")
        .replace("__XFWD_BLOCK__", "")
        # Match the INFO-level form ``render-nginx-conf`` produces. The
        # test harness never flips log_level to DEBUG, so the gated
        # ``if=$nginx_loggable`` directive is the correct shape here.
        .replace(
            "__STDERR_ACCESS_LOG__",
            "access_log /dev/stderr combined if=$nginx_loggable;",
        )
    )


# Empty stand-in for /usr/bin/render-nginx-conf so the init-nginx-conf
# cont-init step doesn't overwrite the test-rendered nginx.conf we
# bind-mount above.
_RENDER_NOOP = "#!/command/with-contenv bash\nexit 0\n"


@pytest.fixture(scope="session")
def proxy_url(_compose_stack: None) -> str:
    """Return the URL the nginx-fronted addon is published on.

    The addon container is brought up by ``_compose_stack``; here we
    just discover the published host port for the compose ``dep-proxy``
    service's 8126.
    """
    return f"http://127.0.0.1:{_compose_port('dep-proxy', 8126)}"


@pytest.fixture(scope="session")
def status_url(_compose_stack: None) -> str:
    """Return the URL the addon's Python status UI (``/api/sessions``,
    …) is published on. Same shape as ``proxy_url`` — pre-compose these
    were one port because the container ran ``--network host``; on the
    compose network they're two separate published ports.
    """
    return f"http://127.0.0.1:{_compose_port('dep-proxy', 8098)}"


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
