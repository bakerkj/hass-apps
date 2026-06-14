# Dashboard Entity Proxy — Architecture

The Dashboard Entity Proxy is a reverse proxy that sits between a Home Assistant
client and the HA WebSocket API and filters the entity-state stream so each
client only receives updates for the entities its current dashboard actually
displays. By default HA broadcasts every state change for every entity to every
connected client; on installations with thousands of entities this means a
Lovelace view with twenty entities still pays the bandwidth and CPU cost of the
full system, painful on low-power kiosks, browsers on slow wifi, or shared
Ingress sessions.

The proxy opens one paired upstream connection per client, relays the auth
handshake unchanged (the client authenticates as itself; no proxy-managed
token), then injects its own unfiltered `subscribe_entities` to build a
per-connection state mirror (`StateStore`). The client's own
`subscribe_entities` is answered from that mirror, scoped to whatever view the
client is currently on. As the user navigates between dashboards, device pages,
and panels, scope is re-resolved and add/remove diffs are emitted against the
previous frame.

For installation and option reference, see [README.md](README.md). The rest of
this document covers the design.

## Session lifecycle

After the auth handshake completes, the proxy runs a fixed startup sequence
against HA before forwarding any client traffic. Every request gets a
consecutive id from the per-session allocator (see "Message-id namespacing"
below), so HA's strictly-increasing rule is satisfied by construction:

1. `subscribe_entities`: long-lived state mirror.
2. `config/entity_registry/list`: initial entity registry snapshot.
3. `subscribe_events` for `entity_registry_updated`: live registry tracking.
4. `config/device_registry/list`: initial device registry snapshot.
5. `subscribe_events` for `device_registry_updated`: live registry tracking.
6. `subscribe_events` for `lovelace_updated`: invalidates the proxy's
   dashboard-scope cache when a user edits a dashboard.
7. `energy/get_prefs`: entity list for the `/energy` panel.
8. `lovelace/config` for the configured default dashboard.

The session is "ready to serve" once `_mirror_ready` (on Session) and
`ScopeResolver.ready` (on the scope subsystem) are both set. `_mirror_ready`
flips when the initial mirror snapshot arrives. `ScopeResolver.ready` flips when
the first view is resolved: for plain dashboard views that needs only the
`lovelace/config` response, but for Settings pages (Devices / Integrations /
Areas / Helpers) the registry index must already be built, so the registry
fetches run eagerly at startup rather than lazily on first navigation. Until
both flags are set, any client `subscribe_entities` is parked in a pending list
and served as soon as readiness flips.

Five of the eight startup requests are fatal on failure: `subscribe_entities`
(the mirror), both registry `list`s, and both registry-update
`subscribe_events`. Without any of those, the proxy cannot serve a correct
filtered view. The other three are non-fatal: a failed `lovelace_updated`
subscription just disables dashboard-cache auto-invalidation (edits land on the
next navigation); a failed `energy/get_prefs` widens `/energy` to all entities;
a failed `lovelace/config` falls back to scope = all entities, on the principle
that a broken dashboard shouldn't take down the session. The same all-entities
fallback applies to strategy-mode dashboards and dashboards that parse cleanly
but reference no entities, so an empty snapshot never reaches the frontend.

## Subsystems

`Session` orchestrates. The rest of the behaviour lives in collaborators it
composes, with dependencies passed in through the constructor:

- `RegistryStore`: raw entity and device maps, the
  `(domain, area, integration, device)` index, and the debounced apply pipeline
  for `*_registry_updated` events. See "Registry tracking modes".
- `ScopeResolver`: per-view scope resolution. Takes a `ScopeFilters` value
  object bundling `extra`, `include`, `exclude`, and the loaded `customization`.
  Tracks `widened_by_watchdog` so a late `lovelace/config` doesn't emit a
  multi-MB remove burst.
- `InflightTable`: the dispatch table keyed by allocator id, a reverse client-id
  to HA-id index for unsubscribe translation, the grace-period sweep for
  unclassified one-shot commands, and the recall ring used to attribute events
  that arrive after their entry has been popped.
- `DashboardCache`: per-dashboard resolved scope plus latest-fetch-id tracking
  so stale `lovelace/config` responses don't clobber a newer view. Also drives
  invalidation from the `lovelace_updated` event stream.
- `NavigationManager`: current view, URL path, and browser_mod state.
- `SubscriptionSet`: live client `subscribe_entities` subscriptions plus the
  pending list parked while readiness flips.
- `ThrottleBuffer`: pending add/remove ids during a `state_update_interval`
  window, flushed by the throttle loop.
- `ScopeWatchdog`: the timeout-based widen-to-all safety net for an unresponsive
  HA on the eager `lovelace/config` leg.

The inflight dispatch in `Session._dispatch_inflight` is a `match entry:` over
the sealed `InflightEntry` union from `inflight_types.py`, with a trailing
`assert_never(unhandled)`. Adding a new variant without a `case` is a mypy error
at the dispatch site, not a runtime surprise.

## Scope resolution

Scope (the set of entities the proxy forwards to a given client at a given
moment) is resolved from four sources:

- **Lovelace dashboard config.** The walker (`dashboard.py`) recursively
  harvests entity ids from the cards on the current view, augmented by
  `browser_mod` path signals when that integration is installed.
- **Entity / device registries.** For Settings → Devices / Integrations / Areas
  pages, the proxy looks up "which entities belong to this device / integration
  / area" in the registry index. The index is built from the initial
  `config/entity_registry/list` and `config/device_registry/list` snapshots at
  session start and kept live for the rest of the session by the
  `*_registry_updated` subscriptions. See "Registry tracking modes" below for
  how change events are applied.
- **Configurable helper-platform list.** The Helpers page is a UI category, not
  a registry attribute, so the proxy carries a list of integration domains that
  count as helpers (`HELPER_PLATFORMS` in `const.py`), extendable via
  `customization.helpers.extra_platforms`.
- **Domain-panel rule.** Single-domain panels like `/calendar` and `/todo` scope
  to that domain's entities verbatim (see `navigation.py`).

When `browser_mod` is unavailable, the proxy falls back to inferring
settings-page transitions from request patterns (`is_settings_signal` in
`navigation.py`); settings pages then get a widen-to-all heuristic until the
next dashboard navigation.

### Watchdog ↔ late `lovelace/config` interaction

The scope watchdog (see "Threading and concurrency model") and the eager startup
`lovelace/config` fetch can race: an HA that's slow to answer `lovelace/config`
lets the watchdog fire at `SCOPE_READY_TIMEOUT`, widening scope to "all
entities" so the client gets a usable (if oversized) snapshot. If the late
`lovelace/config` then arrives and resolves to a narrow scope, a naive diff
would emit one `remove` per id outside the new scope. On a 14k-entity install
that's a single multi-megabyte frame the client has to re-render. The session
suppresses that burst: when narrowing follows a watchdog widening, only adds for
newly-in-scope ids are emitted; the client keeps the wider view (harmlessly, a
superset is what it already had) until the next navigation resolves against the
narrow scope as the new baseline. See `ScopeResolver.apply` and the
`ScopeResolver.widened_by_watchdog` flag in `scope_resolver.py`.

## Walker coverage

The walker is deliberately structure-agnostic. It recurses through every nested
dict and list and harvests entity ids from a small set of well-known keys
wherever they appear, so it covers built-in cards, container cards (stack, grid,
sections, panel, conditional, picture-elements, …), and the custom-card
ecosystem (mushroom, plotly, apexcharts, multiple-entity-row, battery-state,
history-explorer, mini-graph, flex-table, …) without a per-type rule book.

Two patterns defer entity selection to runtime:

- `custom:auto-entities` `filter` blocks with `include` / `exclude` lists whose
  items carry an `entity_id` (glob) or `domain` filter.
- `custom:flex-table-card` `entities: {include: <regex>, exclude: <regex>}`
  blocks.

These cannot be resolved to concrete ids at parse time, so the walker returns
them alongside the static ids as patterns; the session expands them against the
state mirror at scope-resolution time and folds newly-arriving entities into the
live scope as they appear. Filter conditions that depend on runtime state
(`state`, `attributes`, `last_changed`, …) are ignored — we deliver a superset
of what the card will actually render.

## Customization layer

An optional `customization.yaml` extends the add-on's built-in scope knowledge
with site-specific entries:

- **`baseline.entities`**: entity ids that must appear in EVERY scope, on top of
  the built-in `FRONTEND_BASELINE` (`zone.home` and `sun.sun`, both of which
  HA's auto-strategy dashboards and several weather-themed cards assume exist).
- **`cards.<type>.implicit_entities`**: entity ids a card's renderer subscribes
  to at runtime even though they don't appear in the card's YAML.
- **`cards.<type>.entity_keys.singles` / `.lists`**: extra dict keys on this
  card type that hold entity references, beyond the canonical keys the walker
  already recognises.
- **`helpers.extra_platforms`**: extra integration domains whose entities should
  appear on the Helpers settings page.

See `customization.example.yaml` for the full schema with worked examples.
Validation uses `voluptuous`; schema errors are raised as `ValueError` with a
path-anchored message and the add-on exits non-zero rather than booting with
surprising defaults.

## Registry tracking modes

Registry events are funneled through a 500 ms trailing-edge debouncer (the same
window HA's own frontend uses, so the proxy's batched refetch lands on the same
cadence the UI itself would) so a burst of related changes coalesces into one
batch before any work runs. The batch is then handled per the configured
`registry_mode`:

- **`full`** matches the HA frontend's pattern. Any event in the window triggers
  one full `config/{entity,device}_registry/list` refetch and an index rebuild.
  Simple, but at 14k entities the per-event refetch is several MB of JSON per
  change.

- **`incremental`** (default) applies the events incrementally:
  - **Entity `remove`**: drop the row from the cached list locally. Zero HA
    round-trips.
  - **Entity `create` / `update`**: issue one `config/entity_registry/get` for
    that single entity. Bandwidth scales with the number of changed entities,
    not the total registry size.
  - **Device `remove`**: drop the row locally.
  - **Device `create` / `update`**: HA exposes no per-device `get`, so the proxy
    refetches the full device list. The device list is much smaller than the
    entity list (typically hundreds vs. thousands), so this is cheap.

`incremental` mode promotes to a single full-list refetch when a debounced burst
exceeds `registry_burst_threshold` (default 50); past that count, one big
refetch beats many small round-trips.

Both modes also support a periodic full refresh via `registry_refetch_interval`
(default 60 s, `0` disables). This is a safety net for missed events: a
subscription that silently dropped, an event the proxy couldn't apply for
unexpected reasons. The periodic refresh self-heals any drift.

A failed subscribe to the registry-update event stream is fatal (the proxy
disconnects the session). Without live tracking the registry index would
silently drift.

## Message-id namespacing

HA's WebSocket protocol numbers each request/response pair with a positive
integer `id` that must strictly increase within a connection. HA's
`ActiveConnection` enforces `cur_id > last_id` and returns `ERR_ID_REUSE` with
the message "Identifier values have to increase." on violation.

The proxy guarantees this rule by construction:

- A single `itertools.count(1)` allocator (`self._next_ha_id`) is the only
  source of ids the proxy ever sends to HA on a given session. Every outbound
  proxy id is `next(self._next_ha_id)` — strictly larger than every id used so
  far.
- A single dispatch table (`InflightTable`, accessed via Session as
  `self._inflight_table`) records the purpose of each allocated id. Each entry
  is a frozen dataclass in `inflight_types.py`, forming a sealed union so
  `match`-based dispatch is exhaustive and mypy-checked. The variants are:
  - **Client-originated** (carry the original client id so the response can be
    rewritten back): `ClientReq`, `ClientConfig` (a translated client
    `lovelace/config` — response is forwarded AND fed into scope resolution),
    `ClientUnsubscribe` (a translated client `unsubscribe_events`, retained
    briefly so the ack can be rewritten back).
  - **Proxy-originated, long-lived subscriptions**: `Mirror` (state mirror),
    `EntityUpdates` / `DeviceUpdates` (registry-update event streams),
    `LovelaceUpdates` (dashboard-edit event stream).
  - **Proxy-originated, one-shot requests**: `EntityList` / `DeviceList`
    (registry list responses; latest-wins semantics ignore stale responses from
    earlier refetches), `EntityGet` (per-entity incremental-mode fetch),
    `ConfigFetch` (a proxy-injected `lovelace/config`), `EnergyPrefs`.

Any mid-session request (a new `lovelace/config` on view change, a registry
refetch triggered by an update event, a future addition) just calls
`next(self._next_ha_id)` and lands in the dispatch table. Strict monotonicity
holds at every moment in the session because the allocator is monotonic,
allocation happens at enqueue time, and a single writer task drains the outbound
queue in FIFO order.

The proxy also mirrors HA's strictly-increasing rule on the **client face**: a
client message whose id is less than or equal to a previously-seen one is
rejected with `id_reuse` and not forwarded. Without this, a buggy or malicious
client could send the same id twice, the proxy would allocate two HA ids for it,
two responses would come back, and both would be rewritten back to the same
client id, observable on the client as duplicate-id ambiguity. Mirroring HA's
rule eliminates that case and makes the proxy semantically indistinguishable
from HA on the wire.

### Inflight retention

Client commands fall into two categories on the wire:

- **One-shot commands.** A single `result` frame, then no further traffic on
  that id.
- **Streaming subscriptions.** A `result` ack followed by events on the same id,
  for the life of the subscription.

The two shapes are indistinguishable at request time, so the proxy classifies
each id through three mechanisms:

1. **Whitelist of known subscriptions.** `HA_SUBSCRIPTION_COMMANDS` in
   `const.py` lists every HA command type whose handler registers
   `connection.subscriptions[msg["id"]]` server-side. On translate, a matching
   `mtype` adds the allocator id to `InflightTable.known_subscriptions`; the
   entry is kept for the life of the connection. The list is regenerated from HA
   source by `dashboard_entity_proxy/scripts/scan_ha_subscriptions.py` when the
   pinned HA version moves.

2. **Grace-period sweep for unclassified commands.** Anything not in the
   whitelist gets a pop deadline (`INFLIGHT_GRACE_SECONDS = 600 s`) when its
   `result` arrives. An event arriving within the window promotes the entry to a
   known subscription and clears the deadline; otherwise a periodic
   `InflightTable.sweep` call (driven by `PeriodicTask` at
   `INFLIGHT_SWEEP_INTERVAL = 60 s`) reclaims the entry. Real HA subscriptions
   emit their first event within milliseconds, so 10 minutes is overwhelmingly
   conservative.

3. **Explicit unsubscribe cleanup.** `unsubscribe_events` translates the
   client's `subscription` field back to the allocator id and drops the entry
   from the `InflightTable` (which clears the dispatch slot, the
   known-subscription set, and any pending pop deadline in one call).

Two additional bookkeeping mechanisms back-stop the above:

- **Swept-recall ring.** When the sweep pops a `ClientReq` entry that never saw
  a HA frame, its allocator id and original client id are kept in a bounded
  `OrderedDict` (`InflightTable.swept_to_client_id`,
  `INFLIGHT_SWEPT_RECALL_MAX = 256`) for one more late-routing chance. A late
  `result` or `event` for that id after pop still reaches the original client;
  the recall slot is consumed on hit, so a second late event from the same id
  (vanishingly rare in practice) is dropped silently.
- **Hard cap with back-pressure.** `InflightTable` rejects new client-kind
  inserts once the table holds `INFLIGHT_MAX = 15000` entries; the client
  receives a `result` frame with `error.code = "inflight_cap"`, no allocator id
  is consumed, and `InflightTable.cap_hits` ticks for the status UI. At the
  current grace this caps sustained client-command rate at
  `INFLIGHT_MAX / INFLIGHT_GRACE_SECONDS = 25 cmd/sec/session` — generous
  headroom over a busy frontend's render_template / call_service burst, tight
  bound on a runaway client. The first cap hit per session logs WARNING;
  subsequent hits don't re-log to avoid spam.

## Threading and concurrency model

Everything runs on a single asyncio event loop. There are no locks. (The
integration-test harness in `tests/integration/conftest.py` spawns the proxy on
a daemon thread with its own loop so pytest's main thread can drive Selenium;
that's a test-only deviation. In production the proxy owns the process's event
loop.)

- One read pump per WebSocket (`_pump_ha`, `_pump_client`) parses incoming
  frames and dispatches them synchronously.
- One writer task per outbound queue drains a bounded (`OUTBOUND_BUFFER = 1024`)
  `asyncio.Queue`. A full queue disconnects the slow peer; bounded memory beats
  unbounded backlog.
- A `ScopeWatchdog` fires once if the eager startup `lovelace/config` leg hasn't
  resolved scope within `SCOPE_READY_TIMEOUT = 15 s`, and only then widens to
  "serve all entities" as a fallback. The watchdog is its own
  `asyncio.create_task` running in parallel with the read pumps, writers, and
  periodic tasks — nothing else awaits on the timer itself. Its only indirect
  effect on other coroutines is that client `subscribe_entities` requests that
  arrive before scope is ready park until either the real `lovelace/config`
  lands or the watchdog forces the widen; 15 s is the upper bound on that
  parking. Safety net for an unresponsive HA, not the normal path.
- An optional throttle loop coalesces add/remove diffs at
  `state_update_interval` via `ThrottleBuffer`.
- An inflight-sweep `PeriodicTask` (`INFLIGHT_SWEEP_INTERVAL = 60 s`) calls
  `InflightTable.sweep` to pop unwhitelisted client-command entries past their
  grace deadline. See "Inflight retention" above.

## Ingress status UI

A small page (`statusui.py` + `index.html`) lists active intercepted sessions
and raw tunnels, their resolved view, the entities currently in scope, the queue
depth toward the client, and message counts. The proxy-level `SessionRegistry`
(`session_registry.py`) tracks live and recently-disconnected sessions and
produces the JSON snapshot the page renders; recently-disconnected sessions
linger as frozen snapshots for ~60 s so quick reconnects are visible. The
`show_client_paths` option redacts the per-client URL path from the snapshot for
installs where multiple users have Ingress access.

Plain-HTTP requests don't reach the Python proxy — nginx forwards them directly
to HA — so for those the status UI relies on a separate tracker
(`http_traffic.py`) that tails nginx's access log (`/dev/shm/dep-access.log`, on
tmpfs). The tailer parses each line for source IP, target path, status code, and
rx/tx byte counts, then folds them into per-client per-target rows registered
with the same `SessionRegistry`. Per-target rows expire after the same
disconnect-retention window as intercepted sessions; the whole client entry
drops once all of its rows have aged out.
