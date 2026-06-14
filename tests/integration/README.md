# Dashboard Entity Proxy — Integration Tests

End-to-end tests that drive a real Home Assistant + the real proxy + a real
headless Chrome through scenarios the in-process fake-HA e2e tests can't model
(real browser timing, real frontend code, real WebSocket compression, etc.).

Each test marked `@pytest.mark.integration` and lives under
`tests/integration/`. The default `pytest` run for this repo excludes
integration tests via `-m "not integration"` in CI; you can also run them on
demand locally.

## Requirements

- **Docker** with a working daemon. Conftest skips every integration test if
  `docker info` fails, so missing docker is a clean skip rather than a failure.
- **System Google Chrome** (selenium auto-downloads the matching driver). On
  Debian/Ubuntu: `apt install google-chrome-stable`.
- **Python deps** (already in the project's `uv` env): `selenium`, `websockets`,
  `pyjwt`, `aiohttp`. The proxy passes compressed HTTP responses through
  unchanged (`auto_decompress=False`), so no `Brotli` Python package is
  required.

## Running

```bash
# Just the integration suite (boots HA + proxy + Chrome):
uv run pytest tests/integration/ -m integration --no-cov

# A single test:
uv run pytest tests/integration/test_client_config_scope.py -m integration --no-cov

# With diagnostic prints (-s disables pytest's output capture):
uv run pytest tests/integration/ -m integration --no-cov -s
```

A first run pulls the pinned HA image (~1.5 GB) and builds the addon image from
local source (~30-60 s). Subsequent runs reuse both. The HA container boots once
per pytest session (~30-45 s) and the addon container once per session (~5 s
startup); each test reuses both via session-scoped fixtures.

The addon container exercises the full nginx + s6 + python proxy stack as it
ships, so tests catch nginx-config bugs (WebSocket upgrade headers, timeout
settings, …) along with Python-side regressions. The `proxy_factory` fixture
spins up a bare in-process Python proxy for tests that need per-test `Options`
overrides (e.g. custom `Customization`).

## Fixtures

- `ha` (session) — starts HA in docker, completes onboarding, mints long-lived
  tokens. Yields a dict with `url`, `ws_url`, `access_token`, `refresh_token`.
- `addon_image` (session) — builds the addon docker image from local source.
- `proxy_url` (session) — runs the addon container (nginx + python + s6) pointed
  at the test HA. Yields the nginx-fronted URL.
- `proxy_factory` (per-test) — fallback in-process python proxy with arbitrary
  `Options` overrides, for tests that need a custom `Customization` or other
  config the container can't easily express.
- `chrome` (session) — one headless Chrome instance for the whole run.
- `browser` (per-test) — pre-authed visitor function: `browser("/foo")` loads
  `proxy_url/foo` with `hassTokens` already in localStorage.
- `ha_ws` (per-test) — `ha_ws({"type": "...", ...})` runs one WS command against
  the test HA and returns the result frame.

## Pre-baked test entities

The HA config bundles a deterministic set of entities so tests don't each have
to provision their own integration:

- `input_boolean.int_test_{a,b,c,d}`
- `input_text.int_text_{a,b}`

These give tests a stable vocabulary for asserting in-scope / out-of-scope
behavior.

## Adding a test

1. Drop a `test_*.py` under `tests/integration/`.
2. Mark every test with `@pytest.mark.integration`.
3. Use `ha_ws` to create dashboards or fixtures via the HA API.
4. Use `browser("/your-dashboard/0")` to drive Chrome through the proxy.
5. Use `chrome.execute_script(...)` to inspect / assert on the browser's
   `hass.states`, console errors, etc.

## HA version pinning

`HA_IMAGE` in `conftest.py` is pinned to a specific HA release (e.g.
`2026.6.2`). Renovate tracks the pin via the `# renovate:` comment immediately
above and will open a PR when a new release is available. Bump deliberately to
validate against new HA versions.
