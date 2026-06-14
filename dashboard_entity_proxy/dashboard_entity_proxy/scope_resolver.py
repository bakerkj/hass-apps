# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Resolves and applies the per-session scope: the set of entities the
proxy forwards to a client at the current moment.

Inputs come from the dashboard cache, the entity/device registry index,
the configurable helper-platform list, single-domain panels, and an
explicit "all entities" mode set by the scope-ready watchdog.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from . import dashboard
from .const import ALWAYS_IN_SCOPE_DOMAINS, HELPER_PLATFORMS
from .customization import Customization
from .dashboard import ScopeSet
from .dashboard_cache import DashboardCache
from .navigation import View, ViewKind
from .registry_store import RegistryStore
from .statestore import StateStore


@dataclass(frozen=True)
class ScopeFilters:
    """Per-session inputs to ``dashboard.apply_filters``: the extra ids
    to fold in, the include/exclude glob lists, and the loaded
    customization.
    """

    extra: list[str] = field(default_factory=list)
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    customization: Customization = field(default_factory=Customization)


# HA error codes treated as permission denials on a ``lovelace/config``
# response: the proxy MUST NOT widen scope on these, because that would
# invert the "filter tighter than HA" contract. Any other failure code
# (not_found, config_not_found, home_assistant_error, etc.) falls back
# to "widen to all", since serving an empty snapshot would leave the
# frontend with nothing. Unknown codes fall into the denial bucket
# conservatively.
_CONFIG_DENY_CODES: frozenset[str] = frozenset(
    {
        "unauthorized",
        "forbidden",
        "not_authorized",
        "auth_required",
    }
)


class ScopeResolver:
    def __init__(
        self,
        *,
        log: logging.Logger,
        store: StateStore,
        dashboard_cache: DashboardCache,
        registry_store: RegistryStore,
        filters: ScopeFilters,
        get_current_view: Callable[[], View],
        get_subs: Callable[[], set[int]],
        emit_add: Callable[[list[str]], None],
        emit_remove: Callable[[list[str]], None],
        on_ready: Callable[[], None],
        inject_config_fetch: Callable[[str], None],
    ) -> None:
        self._log = log
        self._store = store
        self._dashboard_cache = dashboard_cache
        self._registry_store = registry_store
        self._filters = filters
        self._get_current_view = get_current_view
        self._get_subs = get_subs
        self._emit_add = emit_add
        self._emit_remove = emit_remove
        self._on_ready = on_ready
        self._inject_config_fetch = inject_config_fetch

        self.ready: bool = False
        self.ids: list[str] | None = None  # None = all entities
        self.set: set[str] | None = None
        self.entry: ScopeSet | None = None
        # Set by the scope-ready watchdog when it widens scope to "all
        # entities" because the initial dashboard config did not arrive
        # within ``SCOPE_READY_TIMEOUT``. Consulted by the first late
        # ``apply`` so a narrow scope arriving after the widening does
        # not emit a multi-thousand-entry remove burst in one frame.
        # Cleared after that one transition.
        self.widened_by_watchdog: bool = False

        # Energy dashboard entities resolved from HA's ``energy/get_prefs``
        # at session start. The /energy panel renders only what the user
        # configured in their energy preferences, so scope narrows to
        # exactly that set rather than widening to all.
        self.energy_entities: set[str] = set()

    # ---- main resolution dispatcher --------------------------------------

    def resolve_current_view(self) -> None:
        """Compute the right ``ScopeSet`` for the current view (using a
        cached dashboard config, the registry index, or the domain-panel
        rule), and apply it. Called whenever something changes that
        might unblock scope resolution: first mirror event, new
        dashboard config response, registry list response, view change,
        etc.
        """
        view = self._get_current_view()
        if view.kind is ViewKind.ALL:
            self._mark_ready_and_apply(ScopeSet(all=True))
        elif view.kind is ViewKind.DASHBOARD:
            entry = self._dashboard_cache.get(view.key)
            if entry is not None:
                self._mark_ready_and_apply(entry)
        elif view.kind is ViewKind.DOMAIN:
            self._mark_ready_and_apply(self._domain_scope(view.key))
        elif view.kind is ViewKind.ENERGY:
            self._mark_ready_and_apply(self._energy_scope())
        elif self._registry_store.index is not None:
            self._mark_ready_and_apply(self._registry_scope(view))

    # ---- per-kind scope builders -----------------------------------------

    def _domain_scope(self, key: str) -> ScopeSet:
        """Scope for a system panel whose content comes from one or more
        entity domains (``/calendar``, ``/todo``, ``/map``, ``/media-browser``).
        Domain entities present in the mirror at resolution time are passed
        through the user's include/exclude filter; the extract pattern is
        kept so later-arriving entities still fold in (subject to the same
        filter via :meth:`fold_in_pattern_matches`).
        """
        domains = tuple(d for d in key.split(",") if d)
        extract = dashboard.ExtractResult(include_domains=domains)
        domain_ids = [
            eid for eid in self._store.ids() if eid.split(".", 1)[0] in domains
        ]
        scoped = dashboard.apply_filters(
            domain_ids,
            self._filters.extra,
            self._filters.include,
            self._filters.exclude,
            self._filters.customization,
        )
        return ScopeSet(ids=scoped, extract=extract)

    def _energy_scope(self) -> ScopeSet:
        """Scope for the ``/energy`` panel: just the entities the user
        configured in their energy preferences. Falls back to ALL when
        that round-trip failed or returned an empty set, since an empty
        panel is worse than an oversized scope.
        """
        if not self.energy_entities:
            return ScopeSet(all=True)
        scoped = dashboard.apply_filters(
            list(self.energy_entities),
            self._filters.extra,
            self._filters.include,
            self._filters.exclude,
            self._filters.customization,
        )
        return ScopeSet(ids=scoped)

    def _registry_scope(self, view: View) -> ScopeSet:
        """Build the scope for a settings page (Devices / Integrations /
        Areas / Helpers) by looking up entities in the registry index.
        Empty result falls back to "all entities" so the page isn't
        blank on an install where the registry doesn't carry the link
        the page needs.
        """
        index = self._registry_store.index
        assert index is not None
        if view.kind is ViewKind.DEVICE:
            ids = index.device(view.key)
        elif view.kind is ViewKind.INTEGRATION:
            ids = index.integration(view.key)
        elif view.kind is ViewKind.AREA:
            ids = index.area(view.key)
        else:  # HELPERS
            ids = index.platforms(
                [*HELPER_PLATFORMS, *self._filters.customization.extra_helper_platforms]
            )
        scoped = dashboard.apply_filters(
            ids,
            self._filters.extra,
            self._filters.include,
            self._filters.exclude,
            self._filters.customization,
        )
        if not scoped:
            self._log.info("no entities resolved for %s; serving all", view.label())
            return ScopeSet(all=True)
        return ScopeSet(ids=scoped)

    # ---- application + emission ------------------------------------------

    def _mark_ready_and_apply(self, entry: ScopeSet) -> None:
        """Flip the scope-ready flag, apply the resolved scope, and
        notify Session so any client subscriptions parked waiting for it
        are served.
        """
        self.ready = True
        self.apply(entry)
        self._on_ready()

    def apply(self, entry: ScopeSet) -> None:
        """Switch the live scope to ``entry``. Expands any pattern rules
        against the current mirror, computes the add/remove diff against
        the previous scope, and emits that diff to every active client
        subscription so the client's view updates without re-subscribing.
        """
        new_ids: list[str] | None
        new_set: set[str] | None
        if entry.all:
            new_ids = None
            new_set = None
        else:
            ids_set = set(entry.ids)
            if entry.extract is not None and entry.has_patterns:
                # Expand card-level patterns against the current mirror.
                # New entities added later are folded in by
                # ``fold_in_pattern_matches``. The expansion only honors
                # the card's own include/exclude patterns; the user's
                # session-level filters are re-applied below.
                ids_set.update(
                    dashboard.expand_patterns(entry.extract, self._store.ids())
                )
            # Always carry entities in ``ALWAYS_IN_SCOPE_DOMAINS`` regardless
            # of the resolved view, so the HA system panels for those
            # domains (calendar, todo) render correctly on first navigation
            # without waiting for the next reload. New entities arriving in
            # those domains are folded in via ``fold_in_pattern_matches``
            # on every mirror event.
            ids_set.update(
                eid
                for eid in self._store.ids()
                if eid.split(".", 1)[0] in ALWAYS_IN_SCOPE_DOMAINS
            )
            # Re-apply user filters once more so card-level pattern matches
            # and ALWAYS_IN_SCOPE_DOMAINS additions still honor the user's
            # ``include_entity_globs`` / ``exclude_entity_globs`` /
            # customization rules.
            ids_set = set(
                dashboard.apply_filters(
                    sorted(ids_set),
                    self._filters.extra,
                    self._filters.include,
                    self._filters.exclude,
                    self._filters.customization,
                )
            )
            new_set = ids_set
            new_ids = sorted(ids_set)
        if self._get_subs():
            if self.widened_by_watchdog:
                # Post-watchdog narrowing: clients already received an
                # all-entities snapshot (and ongoing updates for every
                # mirror id while ``set`` was None), so every id in
                # ``new_set`` is already on the client. A normal diff
                # would emit a remove for every id NOT in ``new_set``,
                # thousands of entries in one frame on large installs.
                # Skip emission for this one transition; the client
                # keeps the wider view until the next navigation
                # re-resolves with a normal diff against ``new_set``.
                pass
            else:
                self._emit_scope_diff(self.set, new_set)
        # Only clear the flag when actually narrowing. An intervening
        # all-entities apply (a no-op diff) must NOT consume the flag,
        # or the next narrow apply would emit the very remove burst the
        # flag exists to suppress.
        if new_set is not None:
            self.widened_by_watchdog = False
        self.ids = new_ids
        self.set = new_set
        self.entry = entry

    def _emit_scope_diff(self, old: set[str] | None, new: set[str] | None) -> None:
        """Compute and send the add/remove diff between two scope sets.
        A ``None`` set means "all entities": anything currently in the
        mirror is considered in-scope on that side of the comparison.
        """
        removed: list[str] = []
        added: list[str] = []
        for eid in self._store.ids():
            old_in = old is None or eid in old
            new_in = new is None or eid in new
            if old_in and not new_in:
                removed.append(eid)
            elif not old_in and new_in:
                added.append(eid)
        if removed:
            self._emit_remove(removed)
        if added:
            self._emit_add(added)

    def widen_to_all_from_watchdog(self) -> None:
        """Watchdog timeout: flip scope to "all entities" and notify
        Session so parked subscriptions get served. Sets the
        ``widened_by_watchdog`` flag so a late-arriving narrow scope
        skips the remove burst (see :meth:`apply`).
        """
        self.ids = None
        self.set = None
        self.entry = ScopeSet(all=True)
        self.ready = True
        self.widened_by_watchdog = True
        self._on_ready()

    # ---- pattern-driven scope growth -------------------------------------

    def fold_in_pattern_matches(self, event: dict[str, Any]) -> None:
        """If the active scope has pattern rules (globs/regexes/domains)
        or the entity belongs to one of :data:`ALWAYS_IN_SCOPE_DOMAINS`,
        grow the scope to include matches, so ``custom:auto-entities``
        and similar pattern-driven cards see brand-new entities as they
        appear, and so HA's system panels for ``calendar`` and ``todo``
        keep their entity list complete without waiting for a reload.
        """
        if self.set is None:
            return
        added = event.get("a")
        if not isinstance(added, dict):
            return
        entry = self.entry
        extract = (
            entry.extract
            if entry is not None and entry.has_patterns and entry.extract is not None
            else None
        )
        exclude = self._filters.exclude
        include = self._filters.include
        for eid in added:
            if eid in self.set:
                continue
            domain = eid.split(".", 1)[0]
            if not (
                domain in ALWAYS_IN_SCOPE_DOMAINS
                or (extract is not None and dashboard.matches_extract(extract, eid))
            ):
                continue
            # Honor the session-level include/exclude filters on late
            # arrivals too, so a card-pattern or ALWAYS_IN_SCOPE_DOMAINS
            # match doesn't sneak past the user's globs.
            if exclude and dashboard._matches_any(eid, exclude):
                continue
            if include and not dashboard._matches_any(eid, include):
                continue
            self.set.add(eid)
            if self.ids is not None:
                # ``ids`` is kept sorted by ``apply``; preserve that
                # invariant on fold-ins so status snapshots stay
                # order-stable.
                bisect.insort(self.ids, eid)

    # ---- ConfigFetch / ClientConfig integration --------------------------

    def cache_and_reapply(
        self, url_path: str, mid: int, entry: ScopeSet | None
    ) -> None:
        """Cache the resolved scope for a dashboard, then re-resolve the
        current view. Applies only if the user is still on this
        dashboard, so a late response for a dashboard the user
        navigated away from can't serve a stale scope.

        ``entry is None`` signals a permission-denied response from
        :meth:`scope_from_config`; the cache is intentionally NOT
        updated so the prior (correctly-scoped) entry stays
        authoritative and the next navigation re-fetches naturally.

        Late-fetch guard: a newer ``lovelace/config`` fetch for the
        same dashboard may have already updated the cache. If ``mid``
        isn't the latest recorded fetch id, drop this response so it
        can't overwrite the cache with older data.
        """
        if entry is None:
            return
        if not self._dashboard_cache.is_latest_fetch(url_path, mid):
            return
        self._dashboard_cache.set(url_path, entry)
        self.resolve_current_view()

    def scope_from_config(self, msg: dict[str, Any]) -> ScopeSet | None:
        """Build a ``ScopeSet`` from a ``lovelace/config`` response.

        Returns ``None`` when the response is a permission denial; the
        caller must NOT update the cache in that case (widening on
        denial would invert the proxy's "tighter than HA" contract).
        Strategy-mode dashboards, parseable-but-entity-less dashboards,
        and not-found / parse-failed dashboards all fall back to "all
        entities", since an empty scope would leave the frontend with
        no state.
        """
        if not msg.get("success"):
            err = msg.get("error") or {}
            code = err.get("code") if isinstance(err, dict) else None
            code_str = code if isinstance(code, str) else ""
            if code_str in _CONFIG_DENY_CODES:
                self._log.warning(
                    "lovelace/config denied (code=%r): keeping scope", code_str
                )
                return None
            self._log.warning(
                "lovelace/config failed (code=%r); serving all entities", code_str
            )
            return ScopeSet(all=True)
        result = msg.get("result")
        if not isinstance(result, dict):
            return ScopeSet(all=True)
        if dashboard.is_strategy(result):
            return ScopeSet(all=True)
        extract = dashboard.extract_scope(result, self._filters.customization)
        if extract.empty():
            self._log.warning(
                "dashboard parser returned no entities; serving all entities"
            )
            return ScopeSet(all=True)
        scoped = dashboard.apply_filters(
            list(extract.ids),
            self._filters.extra,
            self._filters.include,
            self._filters.exclude,
            self._filters.customization,
        )
        return ScopeSet(ids=scoped, extract=extract)

    # ---- energy_prefs response handling ----------------------------------

    def handle_energy_prefs(self, msg: dict[str, Any]) -> None:
        """Parse the ``energy/get_prefs`` response and stash every
        entity-id-shaped string for :meth:`_energy_scope`. A failed
        response is non-fatal: ``_energy_scope`` falls back to ALL.

        If the user happens to be on /energy when this lands, re-apply.
        """
        if not msg.get("success"):
            self._log.warning(
                "energy/get_prefs failed; /energy will widen to all entities"
            )
            return
        result = msg.get("result") or {}
        if not isinstance(result, dict):
            return
        self.energy_entities = dashboard.extract_energy_entities(result)
        self._log.debug("energy prefs: %d entities resolved", len(self.energy_entities))
        if self._get_current_view().kind is ViewKind.ENERGY:
            self.resolve_current_view()
