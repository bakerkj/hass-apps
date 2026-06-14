# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tracks where the client is right now, and how the proxy got that signal.

Holds the classified ``View`` the client is currently on, the raw URL
path last reported, and a flag that records whether browser_mod has been
seen. Once browser_mod is reporting paths, the heuristic settings-page
widen stays silent because browser_mod is authoritative.

The manager mutates ``current_view`` via ``navigate``, drives the
browser_mod path through ``navigate_from_browser_mod``, and exposes a
``heuristic_widen_to_all`` fallback for clients without browser_mod.
Side-effecting work (dashboard config fetch, scope re-resolution) is
delegated to callbacks supplied by Session.
"""

from __future__ import annotations

import logging
from typing import Callable

from .dashboard_cache import DashboardCache
from .navigation import View, ViewKind, classify_path


class NavigationManager:
    def __init__(
        self,
        *,
        initial_view: View,
        log: logging.Logger,
        dashboard_cache: DashboardCache,
        inject_config_fetch: Callable[[str], None],
        resolve_current_view: Callable[[], None],
    ) -> None:
        self.current_view: View = initial_view
        self.current_path: str = ""
        self.using_browser_mod: bool = False
        self._log = log
        self._dashboard_cache = dashboard_cache
        self._inject_config_fetch = inject_config_fetch
        self._resolve_current_view = resolve_current_view

    def navigate(
        self,
        view: View,
        allow_inject: bool,
        path: str | None = None,
    ) -> None:
        """Switch the current view, lazily fetching dashboard config when
        the new view needs one. No-op if the view is unchanged (only the
        raw path is updated). ``allow_inject`` gates whether the proxy
        will issue its own ``lovelace/config`` request; false when the
        client is already fetching the same config itself, to avoid
        duplicate fetches.

        Registry-backed views (Devices / Integrations / Areas / Helpers)
        don't need a per-navigation fetch: the registry index is
        maintained live for the life of the session by the startup
        subscriptions.
        """
        if view == self.current_view:
            if path is not None:
                self.current_path = path
            return
        self._log.debug(
            "navigate: %s -> %s (allow_inject=%s)",
            self.current_view.label(),
            view.label(),
            allow_inject,
        )
        self.current_view = view
        if path is not None:
            self.current_path = path
        if view.kind is ViewKind.DASHBOARD:
            if view.key not in self._dashboard_cache and allow_inject:
                self._inject_config_fetch(view.key)
        self._resolve_current_view()

    def navigate_from_browser_mod(self, path: str) -> None:
        """Drive navigation from a ``browser_mod/update`` message. Sets
        ``using_browser_mod`` so the heuristic fallback stays quiet;
        browser_mod is authoritative when available.
        """
        self.using_browser_mod = True
        self.navigate(classify_path(path), allow_inject=True, path=path)

    def heuristic_widen_to_all(self) -> None:
        """Fallback for clients without browser_mod: when a settings-page
        request pattern is observed, widen scope to all entities.
        Silenced if browser_mod is reporting paths, since those are the
        source of truth. The next intercepted ``lovelace/config`` will
        re-navigate to the dashboard view unconditionally, so no flag is
        needed to remember a prior widen.
        """
        if self.using_browser_mod:
            return
        self.navigate(View(ViewKind.ALL), allow_inject=False)
