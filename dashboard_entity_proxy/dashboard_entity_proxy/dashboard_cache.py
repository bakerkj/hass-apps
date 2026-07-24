# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-session cache of resolved dashboard scopes, plus latest-fetch-id
tracking so a late stale ``lovelace/config`` response can't overwrite
the cache with older data.
"""

import logging
from collections.abc import Callable
from typing import Any

from ._msg_utils import subscription_frame
from .dashboard import ScopeSet
from .navigation import View, ViewKind


class DashboardCache:
    def __init__(self) -> None:
        # url_path -> resolved ScopeSet. Populated by
        # ``ScopeResolver.cache_and_reapply``; dropped on
        # ``lovelace_updated`` for the affected dashboard.
        self.scopes: dict[str, ScopeSet] = {}
        # url_path -> most recent proxy-allocated id for a
        # ``lovelace/config`` fetch. Used by ``is_latest_fetch`` to
        # drop stale responses when two fetches race.
        self.latest_fetch_id: dict[str, int] = {}

    # ---- scope cache ------------------------------------------------------

    def get(self, url_path: str) -> ScopeSet | None:
        return self.scopes.get(url_path)

    def __contains__(self, url_path: str) -> bool:
        return url_path in self.scopes

    def set(self, url_path: str, scope: ScopeSet) -> None:
        self.scopes[url_path] = scope

    def drop(self, url_path: str) -> bool:
        """Returns ``True`` if there was an entry to drop."""
        return self.scopes.pop(url_path, None) is not None

    # ---- latest-fetch-id tracking ----------------------------------------

    def record_fetch_id(self, url_path: str, mid: int) -> None:
        self.latest_fetch_id[url_path] = mid

    def is_latest_fetch(self, url_path: str, mid: int) -> bool:
        """Returns ``True`` when no fetch id has been recorded yet (this
        one wins by default).
        """
        latest = self.latest_fetch_id.get(url_path)
        return latest is None or mid >= latest

    # ---- ``lovelace_updated`` subscription event handling ----------------

    def handle_lovelace_updated(
        self,
        msg: dict[str, Any],
        *,
        log: logging.Logger,
        current_view: View,
        inject_config_fetch: Callable[[str], None],
    ) -> None:
        """Process an inbound ``lovelace_updated`` event. HA emits this
        when a dashboard's config is saved (UI editor or
        ``lovelace/config/save``). The event's ``url_path`` identifies
        the affected dashboard; drop the matching entry so the next
        navigation re-resolves against the new config, and if the user
        is currently on that dashboard, force an immediate refetch.

        A subscription failure is logged but NOT fatal: a missed
        invalidation only means stale scope persists until the next
        navigation forces a re-fetch (or the session reconnects).
        """
        event = subscription_frame(
            msg,
            on_failure=lambda: log.warning(
                "lovelace_updated subscription failed; dashboard cache "
                "will not auto-invalidate on edits"
            ),
        )
        if event is None:
            return
        data = event.get("data")
        if not isinstance(data, dict):
            return
        url_path = data.get("url_path")
        # HA reports ``url_path`` as ``None`` for the default lovelace
        # dashboard, which we cache under the empty-string key.
        key = url_path if isinstance(url_path, str) else ""
        if self.drop(key):
            log.debug("%s dashboard cache invalidated", f"/{key}" if key else "/")
            # If the user is on the dashboard that just got edited,
            # force a fresh fetch so scope re-resolves now rather than
            # only on the next navigation.
            if current_view.kind is ViewKind.DASHBOARD and current_view.key == key:
                inject_config_fetch(key)
