# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Unit tests for NavigationManager.

Constructs the manager directly against fake callbacks. Each test reads
as 'given this prior state, calling X has these effects on the manager's
fields and these calls on the callbacks.'
"""

from __future__ import annotations

import logging
from typing import Any

from dashboard_entity_proxy.dashboard import ScopeSet
from dashboard_entity_proxy.dashboard_cache import DashboardCache
from dashboard_entity_proxy.navigation import View, ViewKind
from dashboard_entity_proxy.navigation_manager import NavigationManager


def _nav(
    *,
    initial_view: View | None = None,
    dashboard_cache: DashboardCache | None = None,
    inject_calls: list[str] | None = None,
    resolve_calls: list[None] | None = None,
) -> NavigationManager:
    """Build a NavigationManager with capturing callbacks."""

    def _inject(url: str) -> None:
        if inject_calls is not None:
            inject_calls.append(url)

    def _resolve() -> None:
        if resolve_calls is not None:
            resolve_calls.append(None)

    return NavigationManager(
        initial_view=initial_view or View(ViewKind.DASHBOARD),
        log=logging.getLogger("test_dep_navigation_manager"),
        dashboard_cache=dashboard_cache or DashboardCache(),
        inject_config_fetch=_inject,
        resolve_current_view=_resolve,
    )


# ---- navigate --------------------------------------------------------------


def test_navigate_to_same_view_only_updates_path():
    nav = _nav(initial_view=View(ViewKind.DASHBOARD, "x"))
    nav.current_path = "/old"

    nav.navigate(View(ViewKind.DASHBOARD, "x"), allow_inject=True, path="/new")

    assert nav.current_view == View(ViewKind.DASHBOARD, "x")
    assert nav.current_path == "/new"


def test_navigate_to_new_view_updates_view_and_path_and_resolves():
    resolves: list[None] = []
    nav = _nav(resolve_calls=resolves)

    nav.navigate(View(ViewKind.ALL), allow_inject=False, path="/dev-tools")

    assert nav.current_view == View(ViewKind.ALL)
    assert nav.current_path == "/dev-tools"
    assert resolves == [None]  # scope re-resolution triggered exactly once


def test_navigate_to_dashboard_injects_when_not_cached_and_allowed():
    injects: list[str] = []
    nav = _nav(inject_calls=injects)

    nav.navigate(View(ViewKind.DASHBOARD, "my-dash"), allow_inject=True)

    assert injects == ["my-dash"]


def test_navigate_to_dashboard_skips_inject_when_already_cached():
    cache = DashboardCache()
    cache.set("cached-dash", ScopeSet(ids=["light.x"]))
    injects: list[str] = []
    nav = _nav(dashboard_cache=cache, inject_calls=injects)

    nav.navigate(View(ViewKind.DASHBOARD, "cached-dash"), allow_inject=True)

    assert injects == []  # cache hit — no fetch


def test_navigate_to_dashboard_skips_inject_when_allow_inject_false():
    """``allow_inject=False`` is the case where the client is already
    issuing its own lovelace/config — the proxy must not duplicate it.
    """
    injects: list[str] = []
    nav = _nav(inject_calls=injects)

    nav.navigate(View(ViewKind.DASHBOARD, "dash"), allow_inject=False)

    assert injects == []


# ---- navigate_from_browser_mod ---------------------------------------------


def test_navigate_from_browser_mod_sets_flag_and_classifies_path():
    nav = _nav()

    nav.navigate_from_browser_mod("/lovelace/home")

    assert nav.using_browser_mod is True
    # ``/lovelace/home`` classifies to a DASHBOARD view with key "lovelace".
    assert nav.current_view.kind is ViewKind.DASHBOARD
    assert nav.current_path == "/lovelace/home"


# ---- heuristic_widen_to_all ------------------------------------------------


def test_heuristic_widen_navigates_to_all():
    nav = _nav(initial_view=View(ViewKind.DASHBOARD, "x"))

    nav.heuristic_widen_to_all()

    assert nav.current_view == View(ViewKind.ALL)


def test_heuristic_widen_silenced_when_using_browser_mod():
    """browser_mod is authoritative — the heuristic must stay quiet
    once browser_mod has reported anything.
    """
    nav = _nav(initial_view=View(ViewKind.DASHBOARD, "x"))
    nav.using_browser_mod = True

    nav.heuristic_widen_to_all()

    assert nav.current_view == View(ViewKind.DASHBOARD, "x")


# ---- typing fixture for pytest-asyncio (none of the tests are async) ------


_ = Any  # silence "imported but unused" warning if asyncio isn't pulled in
