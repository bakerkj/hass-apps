# Dashboard Entity Proxy — End-to-End Tests

End-to-end tests that drive a real Home Assistant + the real proxy + a real
headless Chrome through scenarios in-process fake-HA tests can't model (real
browser timing, real frontend code, real WebSocket compression, etc.).

Each test is marked `@pytest.mark.e2e` and lives under
`dashboard_entity_proxy/tests/integration/`. The default `pytest` run for this
repo excludes e2e tests via `-m "not e2e"` in CI; you can also run them on
demand locally.

## Requirements

- **Docker** with a working daemon and the `docker compose` v2 CLI. Conftest
  skips every e2e test if `docker info` fails, so missing docker is a clean skip
  rather than a failure.
- **System Google Chrome** (selenium auto-downloads the matching driver). On
  Debian/Ubuntu: `apt install google-chrome-stable`.
- **Python deps** (already in the project's `uv` env): `selenium`, `websockets`,
  `pyjwt`, `aiohttp`. The proxy passes compressed HTTP responses through
  unchanged (`auto_decompress=False`), so no `Brotli` Python package is
  required.

## Running

```bash
# 1) Build the addon image with the tag the compose file references:
docker build -t dep_int_test:ci dashboard_entity_proxy

# 2) Run the e2e suite. Pytest's session-autouse fixture brings the compose
#    stack up (with --wait for healthchecks) and tears it down after.
uv run pytest dashboard_entity_proxy/tests/integration/ -m e2e --no-cov -s

# A single test:
uv run pytest dashboard_entity_proxy/tests/integration/test_client_config_scope.py -m e2e --no-cov -s
```

The `-s` isn't strictly required, but without it the `[dep-e2e] …` phase prints
from the compose-stack fixture get swallowed by pytest's default capture and
only surface on failure.

To bring the stack up outside pytest (useful when iterating on a single test or
poking at HA with `docker compose logs -f ha`):

```bash
cd dashboard_entity_proxy/tests/integration
docker compose up -d --wait
# ... run/rerun tests, inspect logs, docker exec, etc. ...
docker compose down -v
```

A first run pulls the pinned HA image (~1.5 GB). Subsequent runs reuse it. The
HA container boots once per pytest session (~30-45 s to healthy) and the addon
container comes up when HA is healthy (~5 s); each test reuses both via
session-scoped fixtures.

The addon container exercises the full nginx + s6 + python proxy stack as it
ships, so tests catch nginx-config bugs (WebSocket upgrade headers, timeout
settings, …) along with Python-side regressions. The `proxy_factory` fixture
spins up a bare in-process Python proxy for tests that need per-test `Options`
overrides (e.g. custom `Customization`).

## Fixtures

- `_compose_stack` (session, autouse) — renders `options.json` + `nginx.conf` +
  the `render-nginx-conf` no-op into `/tmp/dep-proxy-e2e/`, runs
  `docker compose up -d --wait`, tears down with `docker compose down -v` at
  session end.
- `ha` (session) — discovers the compose-published HA port, completes HA
  onboarding, mints long-lived tokens. Yields a dict with `url`, `ws_url`,
  `access_token`, `refresh_token`.
- `proxy_url` (session) — discovers the compose-published dep-proxy port.
- `proxy_factory` (per-test) — fallback in-process python proxy with arbitrary
  `Options` overrides, for tests that need a custom `Customization` or other
  config the container can't easily express.
- `chrome` (session) — one headless Chrome instance for the whole run.
- `browser` (per-test) — pre-authed visitor function: `browser("/foo")` loads
  `proxy_url/foo` with `hassTokens` already in localStorage.
- `ha_ws` (per-test) — `ha_ws({"type": "...", ...})` runs one WS command against
  the test HA and returns the result frame.

## Pre-baked test entities

`configuration.yaml` bundles a deterministic set of entities so tests don't each
have to provision their own integration:

- `input_boolean.int_test_{a,b,c,d}`
- `input_text.int_text_{a,b}`

Plus a template weather entity that supports `weather/subscribe_forecast` for
the namespaced-subscription tests.

These give tests a stable vocabulary for asserting in-scope / out-of-scope
behavior.

## Adding a test

1. Drop a `test_*.py` under `dashboard_entity_proxy/tests/integration/`.
2. Mark every test with `@pytest.mark.e2e`.
3. Use `ha_ws` to create dashboards or fixtures via the HA API.
4. Use `browser("/your-dashboard/0")` to drive Chrome through the proxy.
5. Use `chrome.execute_script(...)` to inspect / assert on the browser's
   `hass.states`, console errors, etc.

## HA version pinning

The HA image tag lives in `docker-compose.yml` (`services.ha.image`). Renovate
tracks the pin via the `# renovate:` comment immediately above and will open a
PR when a new release is available. Bump deliberately to validate against new HA
versions.
