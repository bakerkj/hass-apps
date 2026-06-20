# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Module-wide constants shared across the package.

Protocol values, default config values, message-id contracts, and HA
domain knowledge live here so the rest of the package can reference a
single source of truth. Parser/validator internals (allow-list key
sets, walker key tables, schema-validation guards) stay with their
modules.
"""

import re


# ---- Entity-id shape -------------------------------------------------------

# Conservative entity-id matcher: ``domain.name`` with only lowercase
# letters, digits, and underscores in each half. Avoids matching arbitrary
# dotted strings (file paths, URLs, etc.). Used by both the dashboard
# parser and the config loader so the two callers cannot drift.
ENTITY_RE = re.compile(r"^[a-z][a-z0-9_]*\.[a-z0-9_]+$")


# ---- Ports / network -------------------------------------------------------

# Container-side port the Python proxy binds on (loopback only). nginx
# sits in front on the host-facing port and proxies to this — the
# Python process is never reachable directly from outside the
# container. ``rootfs/etc/nginx/nginx.conf`` upstream block must match.
PROXY_BIND_PORT = 8125

# Container-side port the Supervisor ingress hits for the status UI.
# The status app is served directly by the Python process (not through
# nginx) because the Supervisor reaches it via the addon network
# rather than the host port.
INGRESS_PORT = 8098


# ---- Home Assistant WebSocket protocol -------------------------------------

# The single WebSocket path HA exposes for state subscriptions.
HA_WS_PATH = "/api/websocket"

# Default heartbeat for aiohttp WebSockets; None disables aiohttp's
# protocol-level ping. HA's WebSocket API has its own application-level
# ping/pong (``{"type": "ping"}`` / ``{"type": "pong"}`` JSON messages),
# so we deliberately do not layer a second WS-protocol-level heartbeat
# on top, which would just risk racing the application keepalive.
HEARTBEAT: float | None = None

# 0 = no cap. HA's frontend can ship payloads larger than aiohttp's 4 MiB
# default (per-registry dumps on large installs); raise rather than
# disconnect mid-message.
WS_MAX_MSG_SIZE = 0


# ---- HA domain knowledge ---------------------------------------------------

# System entities the HA frontend assumes always exist, always carried
# into every scope unless explicitly excluded by the user.
#
# ``zone.home``: used by the home-overview auto-strategy dashboard as a
#   placeholder for unassigned-area tiles.
# ``sun.sun``: read directly from ``hass.states`` by weather-themed
#   cards; ``weather-chart-card`` hardcodes the id, and
#   ``clock-weather-card``'s ``sun_entity`` config key defaults to
#   ``sun.sun`` (so most users never write it in YAML).
FRONTEND_BASELINE = frozenset({"zone.home", "sun.sun"})

# Domains whose entities are always carried in scope regardless of the
# current view. The HA system panels for these domains (``/calendar``,
# ``/todo``, also reachable as ``/shopping-list``) read their entity
# list from ``hass.states`` synchronously at render time. If the panel
# renders before the proxy's scope-narrowing add-event reaches the
# frontend reducer, the panel sees no entities and renders empty until
# the user reloads. Keeping these small domains in every scope (~5-10
# entities on a typical install) eliminates that render race for a
# negligible bandwidth cost.
ALWAYS_IN_SCOPE_DOMAINS: frozenset[str] = frozenset({"calendar", "todo"})

# Integration domains whose entities appear on the ``/config/helpers``
# settings page. Approximate and version-dependent; Home Assistant doesn't
# expose a definitive list, so we enumerate the common helper integrations
# and let the user extend via ``customization.helpers.extra_platforms``.
HELPER_PLATFORMS: list[str] = [
    "input_boolean",
    "input_button",
    "input_number",
    "input_select",
    "input_text",
    "input_datetime",
    "counter",
    "timer",
    "schedule",
    "template",
    "group",
    "derivative",
    "threshold",
    "min_max",
    "utility_meter",
    "trend",
    "history_stats",
    "integration",
    "statistics",
    "tod",
    "random",
    "switch_as_x",
    "generic_thermostat",
    "generic_hygrostat",
    "mold_indicator",
    "filter",
    "bayesian",
]


# ---- Default config values -------------------------------------------------

# Default upstream HA URL the proxy forwards to when none is configured.
# Matches the in-cluster Supervisor DNS name.
DEFAULT_HA_URL = "http://homeassistant:8123"


# ---- HTTP forwarding timeouts ---------------------------------------------

# Bounds applied to the shared ``aiohttp.ClientSession`` that forwards
# plain HTTP requests to HA. The aiohttp default is a 5-minute total
# timeout, which would pin the forwarder if HA stalls on a single
# request. The values here are an upper bound (a normal HA response
# completes in well under a second), and an upstream stall surfaces as
# a clean 504 instead of a half-minute hang.
HTTP_FORWARD_TIMEOUT_TOTAL = 60.0
HTTP_FORWARD_TIMEOUT_CONNECT = 10.0
HTTP_FORWARD_TIMEOUT_SOCK_READ = 30.0


# ---- Session timing / sizing -----------------------------------------------

# asyncio.Queue depth for both client-bound and HA-bound message streams.
# A slow client backs up here; queue-full disconnects the session as a
# protection against unbounded memory growth. Real-world dashboards with
# heavy custom-card use (button-card, card-mod templated styles, etc.)
# open many hundreds of ``render_template`` subscriptions on first
# connect; each opening fires an initial-result event, producing a burst
# in the low thousands of frames within a few hundred ms. Observed peak
# on a 12k-entity install was ~2500 items, so 16384 leaves comfortable
# headroom even for larger installs or simultaneous reconnects. Memory
# impact is bounded: each slot is a small WS frame tuple, so worst-case
# peak is on the order of a few MB per session.
OUTBOUND_BUFFER = 16384

# Maximum seconds the session waits for its first scope to resolve before
# falling back to "scope is the whole entity registry." Covers clients
# that take a long time to issue their first lovelace/config request.
SCOPE_READY_TIMEOUT = 15.0

# How long a disconnected session lingers in the status UI registry after
# disconnect, so quick reconnects are visible as "this tab was here a
# moment ago" rather than blinking out instantly.
DEFAULT_DISCONNECT_RETENTION_SECONDS = 60.0

# nginx access-log path the Python tailer reads. nginx is configured to
# write both this file (for the tailer to populate the HTTP-traffic
# status-UI pane) and stderr (so requests appear in ``ha apps logs``).
# /dev/shm is the container's tmpfs — restart clears it. The tailer
# auto-truncates the file once it grows past HTTP_ACCESS_LOG_MAX_BYTES
# so the file size stays bounded between container restarts.
HTTP_ACCESS_LOG_PATH = "/dev/shm/dep-access.log"
HTTP_ACCESS_LOG_MAX_BYTES = 100 * 1024 * 1024  # 100 MB

# Grace window allowing writer tasks to flush their current in-flight send
# during session teardown before the cleanup path cancels them. Items that
# the writer has already pulled from its queue but not yet sent are lost on
# cancel; this short deadline lets the steady-state final ack land before
# the hard cancel. Sized to be invisible at human-reaction speed while
# still bounding teardown latency under a misbehaving peer.
CLEANUP_DRAIN_TIMEOUT = 0.5

# Debounce window for batching registry-updated events before flushing
# them as either per-entity gets (incremental mode) or a full list
# refetch (full mode, or incremental-mode burst over the threshold).
# Matches HA's own frontend, which uses 500 ms.
REGISTRY_DEBOUNCE_INTERVAL = 0.5

# How long the proxy holds a translated client request's inflight entry
# after the result frame, in case it turns out to be a streaming
# subscription that emits events past the initial ack. Real HA
# subscriptions emit their first event within milliseconds, so 10 minutes
# is overwhelmingly conservative; the swept-recall ring catches any
# truly-late single event past that. The grace only applies to commands
# NOT in ``HA_SUBSCRIPTION_COMMANDS`` (known subs are kept forever, never
# subject to the sweep).
INFLIGHT_GRACE_SECONDS = 600.0

# Period between sweeps that pop expired inflight entries. Low enough
# that pop_at deadlines are honored close to time, high enough that we
# don't burn CPU walking the table.
INFLIGHT_SWEEP_INTERVAL = 60.0

# Hard cap on the per-session ``_inflight_table`` dispatch table. A
# misbehaving client issuing many unique-id one-shots would otherwise
# accumulate entries for the full ``INFLIGHT_GRACE_SECONDS`` window
# before the sweep reclaims them. Once the table reaches this size,
# new client-kind inserts are rejected with a protocol error until
# existing entries pop. At the current grace this caps sustained
# client-command rate at ``INFLIGHT_MAX / INFLIGHT_GRACE_SECONDS`` =
# 25 cmd/sec/session — generous headroom over a busy frontend's burst
# of render_template / call_service traffic.
INFLIGHT_MAX = 15000

# Bounded ring of recently-swept client-command inflight entries. When
# the grace-period sweep reclaims a ``("client", N)`` entry that never
# saw a HA response, we keep its original client id here briefly so a
# late HA result can still be routed back to the waiting client. Sized
# small because the window is tiny in practice: the sweep only fires
# after ``INFLIGHT_GRACE_SECONDS`` and HA's WS rarely lags that long.
INFLIGHT_SWEPT_RECALL_MAX = 256


# ---- HA WebSocket subscription whitelist ----------------------------------

# Command types whose handler registers a subscription via
# ``connection.subscriptions[msg["id"]] = unsub`` in HA's source. The
# proxy keeps the inflight entry for these forever (until the client
# explicitly unsubscribes or the session ends): they emit events long
# past the initial ``result`` ack and popping breaks routing.
#
# Generated from HA's source by
# ``dashboard_entity_proxy/scripts/scan_ha_subscriptions.py``;
# bump whenever the pinned HA version moves. Commands NOT in this set
# fall through to the grace-period sweep (see
# :data:`INFLIGHT_GRACE_SECONDS`), so a missing entry just means
# slightly delayed memory reclaim, never wrong behaviour.
HA_SUBSCRIPTION_COMMANDS: frozenset[str] = frozenset(
    {
        "assist_pipeline/device/capture",
        "assist_satellite/intercept_wake_word",
        "backup/subscribe_events",
        "bluetooth/subscribe_connection_allocations",
        "bluetooth/subscribe_scanner_details",
        "bluetooth/subscribe_scanner_state",
        "calendar/event/subscribe",
        "camera/webrtc/offer",
        "condition_platforms/subscribe",
        "config_entries/flow/subscribe",
        "config_entries/subscribe",
        "conversation/chat_log/subscribe",
        "conversation/chat_log/subscribe_index",
        "dhcp/subscribe_discovery",
        "frontend/subscribe_extra_js",
        "frontend/subscribe_system_data",
        "frontend/subscribe_user_data",
        "group/start_preview",
        "hardware/subscribe_system_status",
        "history/stream",
        "history_stats/start_preview",
        "knx/subscribe_telegrams",
        "labs/subscribe",
        "logbook/event_stream",
        "mobile_app/push_notification_channel",
        "mold_indicator/start_preview",
        "mqtt/subscribe",
        "persistent_notification/subscribe",
        "render_template",
        "ssdp/subscribe_discovery",
        "statistics/start_preview",
        "subscribe_bootstrap_integrations",
        "subscribe_condition",
        "subscribe_entities",
        "subscribe_events",
        "subscribe_trigger",
        "system_health/info",
        "template/start_preview",
        "thread/discover_routers",
        "threshold/start_preview",
        "time_date/start_preview",
        "todo/item/subscribe",
        "trace/debug/breakpoint/subscribe",
        "trigger_platforms/subscribe",
        "weather/subscribe_forecast",
        "zha/devices/permit",
    }
)


# ---- Customization file schema --------------------------------------------

# Versions of the customization YAML schema this build understands. Bumped
# when the file format changes in a non-backward-compatible way.
SUPPORTED_VERSIONS = frozenset({1})
