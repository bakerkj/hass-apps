# Dashboard Entity Proxy

A reverse proxy between browser clients and Home Assistant that intercepts the
`/api/websocket` `subscribe_entities` subscription and serves each client only
the entities its current dashboard/view needs. Slow tablets and kiosks stop
spending CPU on entity updates they never display.

## How it works

For each client connection, the proxy opens one paired connection to Home
Assistant, relays the auth handshake unchanged (the client authenticates as
itself, no proxy token), then multiplexes its own unfiltered
`subscribe_entities` on a namespaced id to build a per-connection state mirror.
The client's own `subscribe_entities` is answered from the mirror, scoped to the
current view, with add/remove diffs as the client navigates.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

## Installation

Add this repository to the Home Assistant add-on store:

1. In Home Assistant, go to **Settings → Add-ons → Add-on Store**.
2. Open the overflow menu (⋮ top right) → **Repositories**.
3. Add `https://github.com/bakerkj/hass-apps` and click **Add**.
4. Refresh the store, then install **Dashboard Entity Proxy** from the new
   section.

### Recommended companion: browser_mod

The proxy works without any HACS dependency: dashboard navigation is detected
from the client's own `lovelace/config` requests. However, installing
[browser_mod](https://github.com/thomasloven/hass-browser_mod) (via HACS) lets
the proxy follow path changes in real time via `browser_mod/update` messages,
giving more precise scope on Settings → Devices / Integrations / Areas pages.
Without browser_mod, those settings pages fall back to a request-pattern
heuristic that widens scope to all entities until the client navigates back to a
dashboard.

## Pointing clients at the proxy

The proxy listens on host port `8126`. Configure the slow tablet/kiosk to use
the proxy URL instead of the Home Assistant URL:

```
http://<hass-host>:8126
```

The browser's auth flow, dashboards, and WebSocket all work exactly as if it
were talking to Home Assistant directly. Faster devices (your phone, laptop) can
keep talking to Home Assistant directly at port 8123; the proxy is optional per
device.

## Options

| Option                                          | Default                               | Purpose                                                                                                                                                                                                                                                      |
| ----------------------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `homeassistant_url`                             | `http://homeassistant:8123`           | Where the proxy forwards requests.                                                                                                                                                                                                                           |
| `transparent`                                   | `true`                                | Strip `X-Forwarded-*` so HA treats the proxy as a direct client (no `trusted_proxies` setup needed).                                                                                                                                                         |
| `dashboard_url_path`                            | (empty)                               | URL path of the initial dashboard each client lands on. The proxy then follows navigation.                                                                                                                                                                   |
| `state_update_interval`                         | (empty)                               | Batch state updates per entity, flushed at most this often (e.g. `2s`, `500ms`).                                                                                                                                                                             |
| `extra_entities`                                | `[]`                                  | Always include these entity ids in every scope. Useful for template references the walker cannot detect.                                                                                                                                                     |
| `include_entity_globs` / `exclude_entity_globs` | `[]`                                  | Final include/exclude filters applied to every scope.                                                                                                                                                                                                        |
| `passthrough_all`                               | `false`                               | Disable filtering entirely; reverse proxy the WebSocket unchanged. Use as an escape hatch while debugging.                                                                                                                                                   |
| `customization_file`                            | `/config/dashboard_entity_proxy.yaml` | Path under an HA-exposed volume (e.g. `/config`) to a YAML file extending the proxy's built-in scope knowledge. If the file is missing the addon starts with no customization applied (a log line notes the missing path). See `customization.example.yaml`. |
| `registry_mode`                                 | `incremental`                         | How registry-update events are applied: `incremental` (per-entity `get`, falls back to a full list past `registry_burst_threshold`) or `full` (always refetch the list). See ARCHITECTURE.md "Registry tracking modes".                                      |
| `registry_refetch_interval`                     | `60`                                  | Seconds between periodic full registry refreshes (safety net for dropped events). `0` disables. See ARCHITECTURE.md "Registry tracking modes".                                                                                                               |
| `registry_burst_threshold`                      | `50`                                  | In `incremental` mode, promote a debounced burst larger than this to a single full-list refetch. See ARCHITECTURE.md "Registry tracking modes".                                                                                                              |
| `show_client_paths`                             | `true`                                | Show each client's current URL path in the Ingress status UI. Disable if multiple users have ingress access and per-client browsing should be hidden.                                                                                                        |
| `log_level`                                     | `INFO`                                | Verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`).                                                                                                                                                                                                             |

Per-option descriptions are also surfaced in the add-on UI via
`translations/en.yaml`.

## Status UI

Open the Ingress panel ("Entity Proxy") to see each active client, the
dashboard/view it is on, the entities currently in scope, and queue depth.
Recently-disconnected clients linger for ~60 seconds so quick reconnects are
visible.

## Customization file

For sites with bespoke cards whose YAML does not name the entities they read at
runtime, copy `customization.example.yaml` into
`/config/dashboard_entity_proxy.yaml` (the default `customization_file` path)
and add per-card implicit-entity entries. The file is fully additive; built-in
defaults for popular HACS cards stay in effect. If the file is absent the addon
starts normally with only the built-in defaults applied — a log line records the
missing path.

## Troubleshooting

- **A card stops updating after navigation.** Set `log_level: DEBUG` and watch
  the proxy logs as the client navigates; the scope diff is printed on every
  view change. Most likely an entity is referenced in a way the walker does not
  yet recognise; add the entity id to `extra_entities` as a quick fix and
  consider a `customization_file` entry for the longer term.
- **Settings pages show no entities.** The proxy infers scope from the device /
  area / integration registry; if the client does not have `browser_mod`
  installed, the fallback heuristic widens to all entities for those pages. This
  is expected.
- **Something fundamentally broken.** Set `passthrough_all: true` to bypass the
  filtering pipeline entirely and confirm the issue is the proxy (passthrough is
  byte-for-byte transparent). If the problem persists with passthrough on, it is
  not the filter.
