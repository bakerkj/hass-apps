# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Classifies a browser URL path into a ``View`` (the abstract concept of
"what is the client currently looking at") that the session uses to
resolve scope.

The proxy needs to know whether a URL points at a Lovelace dashboard
(scope = entities the cards reference), a settings page (scope =
entities linked to a device/integration/area), the helpers page (scope
= entities from helper-style integrations), or a domain-specific panel
like ``/calendar`` (scope = entities of that domain). This module owns
that classification so the session code doesn't have to switch on raw
URL fragments inline.

Inputs come from two places at runtime:
  * The ``browser_mod`` integration (if installed) emits a
    ``browser_mod/update`` message on every navigation; see
    ``path_from_browser_mod``.
  * Without browser_mod, the session falls back to inferring page
    transitions from the client's own request patterns; see
    ``is_settings_signal``.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit


class ViewKind(enum.Enum):
    """The category of thing a client URL points at. Drives scope
    resolution: each kind has a different rule for what entities the
    client needs.
    """

    DASHBOARD = "dashboard"
    DEVICE = "device"
    INTEGRATION = "integration"
    AREA = "area"
    HELPERS = "helpers"
    DOMAIN = "domain"
    ENERGY = "energy"
    ALL = "all"


_LABEL_TEMPLATES: dict[ViewKind, str] = {
    ViewKind.DEVICE: "device {key}",
    ViewKind.INTEGRATION: "integration {key}",
    ViewKind.AREA: "area {key}",
    ViewKind.HELPERS: "helpers",
    ViewKind.DOMAIN: "domain {key}",
    ViewKind.ENERGY: "energy",
    ViewKind.ALL: "all entities",
}


@dataclass(frozen=True)
class View:
    """What the client is currently looking at. Frozen so it is comparable."""

    kind: ViewKind
    key: str = ""

    def label(self) -> str:
        """Human-readable label shown in the Ingress status UI and the
        debug logs. E.g. ``"device abc123"``, ``"area kitchen"``,
        ``"all entities"``, or the dashboard slug.
        """
        tmpl = _LABEL_TEMPLATES.get(self.kind)
        if tmpl is not None:
            return tmpl.format(key=self.key)
        return self.key or "(default dashboard)"


def _segments(path: str) -> list[str]:
    """Split a URL path into ``/``-delimited segments, dropping the leading
    slash and stripping any query / fragment suffix. Empty input yields
    ``[""]`` so callers can index ``[0]`` without bounds-checking.
    """
    return urlsplit(path).path.lstrip("/").split("/")


def path_to_dashboard(path: str) -> str:
    """The Lovelace url_path for a dashboard path ("" = default dashboard)."""
    first = _segments(path)[0]
    return "" if first in ("", "lovelace") else first


_SYSTEM_PANELS: frozenset[str] = frozenset(
    {
        "developer-tools",
        "history",
        "logbook",
        "profile",
    }
)


# System panels whose entire content comes from a small set of entity
# domains. Scoping narrows from "all entities" to "all entities in
# these domains", which is a huge win on large installs without losing
# any frontend behavior. New entities arriving in any of the listed
# domains fold in via the pattern-match path.
#
# The View.key for a multi-domain panel is the comma-joined domain
# list; ``_domain_scope`` splits it back on demand. Single-domain
# panels just have a one-element tuple.
_DOMAIN_PANELS: dict[str, tuple[str, ...]] = {
    "calendar": ("calendar",),
    "todo": ("todo",),
    # ``/shopping-list`` is the legacy alias for the todo panel; HA's modern
    # shopping list is a todo entity, so the domain is still ``todo``.
    "shopping-list": ("todo",),
    # ``/map`` renders entities with location attributes (device trackers,
    # persons, zones, and geo-location feeds) and nothing else.
    "map": ("device_tracker", "person", "zone", "geo_location"),
    # ``/media-browser`` renders media-player entities only.
    "media-browser": ("media_player",),
}


def classify_path(path: str) -> View:
    """Classify a browser path into the view it represents."""
    segs = _segments(path)
    if segs[0] == "config":
        if len(segs) >= 4:
            if segs[1] == "devices" and segs[2] == "device":
                return View(ViewKind.DEVICE, segs[3])
            if segs[1] == "integrations" and segs[2] == "integration":
                return View(ViewKind.INTEGRATION, segs[3])
            if segs[1] == "areas" and segs[2] == "area":
                return View(ViewKind.AREA, segs[3])
        if len(segs) >= 2 and segs[1] == "helpers":
            return View(ViewKind.HELPERS)
        return View(ViewKind.ALL)
    if segs[0] in _DOMAIN_PANELS:
        return View(ViewKind.DOMAIN, ",".join(_DOMAIN_PANELS[segs[0]]))
    if segs[0] == "energy":
        return View(ViewKind.ENERGY)
    if segs[0] in _SYSTEM_PANELS:
        return View(ViewKind.ALL)
    return View(ViewKind.DASHBOARD, path_to_dashboard(path))


def is_settings_signal(msg_type: str) -> bool:
    """Whether a client request implies a settings page (no-browser_mod
    fallback). Restricted to the config_entries family, and excluding the
    ``*subscribe*`` variants. The HA frontend issues
    ``config_entries/subscribe`` on every page load to keep the integrations
    badge in sync, so treating it as a settings signal would widen scope to
    all entities for every dashboard before browser_mod has had a chance to
    report the real path.
    """
    if not (
        msg_type.startswith("config_entries/")
        or msg_type.startswith("config/config_entries")
    ):
        return False
    return "subscribe" not in msg_type


def path_from_browser_mod(msg: dict[str, Any]) -> str | None:
    """Extract the path reported by a browser_mod/update message.

    browser_mod's wire format wraps the browser state under a ``browser``
    key (``{"data": {"browser": {"path": "/calendar", ...}}}``), and the
    proxy needs that nested location to drive navigation for system
    panels like ``/calendar`` and ``/todo``. A flat ``data.path`` shape is
    accepted as a fallback so a hypothetical fork or future format change
    doesn't silently regress this.
    """
    data = msg.get("data")
    if isinstance(data, dict):
        browser = data.get("browser")
        if isinstance(browser, dict):
            path = browser.get("path")
            if isinstance(path, str) and path:
                return path
        path = data.get("path")
        if isinstance(path, str) and path:
            return path
    return None


def url_path_from_config_request(msg: dict[str, Any]) -> str:
    """Extract url_path from a lovelace/config request ("" when null/absent)."""
    url = msg.get("url_path")
    return url if isinstance(url, str) else ""
