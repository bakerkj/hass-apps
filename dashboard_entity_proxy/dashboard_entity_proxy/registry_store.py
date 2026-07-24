# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-session cache of HA's entity and device registries.

Caches the raw rows, derives the per-device / per-platform / per-area
lookup index, debounces ``entity_registry_updated`` /
``device_registry_updated`` events, and applies latest-wins refetch
tracking. Rebuilds the index whenever both raw maps are populated and
calls back into Session via ``resolve_current_view`` so any in-flight
settings page picks up the new data.

See ARCHITECTURE.md for the ``full`` vs ``incremental`` trade-offs and
the burst-threshold promotion rule.
"""

import logging
from collections.abc import Callable
from typing import Any

from . import entity_index
from ._debounce import _TrailingDebouncer
from ._msg_utils import result_index, subscription_frame
from .const import REGISTRY_DEBOUNCE_INTERVAL
from .entity_index import EntityIndex
from .inflight_types import DeviceList, EntityGet, EntityList

# Type alias for the ``send_ha_command`` callback Session supplies.
SendHaCommand = Callable[..., int]


class RegistryStore:
    def __init__(
        self,
        *,
        log: logging.Logger,
        send_ha_command: SendHaCommand,
        resolve_current_view: Callable[[], None],
        fatal: Callable[[str], None],
        mode: str = "incremental",
        burst_threshold: int = 50,
    ) -> None:
        self._log = log
        self._send_ha_command = send_ha_command
        self._resolve_current_view = resolve_current_view
        self._fatal = fatal
        self.mode = mode
        self.burst_threshold = burst_threshold
        self._unknown_action_logged = False

        # Raw row caches keyed for O(1) drop / per-entity merge / lookup.
        self.raw_entities: dict[str, dict[str, Any]] = {}
        self.raw_devices: dict[str, dict[str, Any]] = {}
        self.index: EntityIndex | None = None
        self.have_entities = False
        self.have_devices = False

        # Per-entity / per-device events accumulated during a debounce
        # window; flushed in batch. Multiple events for the same id
        # collapse to the latest action.
        self.pending_entity_events: dict[str, str] = {}
        self.pending_device_events: dict[str, str] = {}
        # Per-entity-get round trips currently in flight; the index is
        # rebuilt once this set drains.
        self.pending_entity_gets: set[str] = set()

        # Track the most recent list-fetch id so refetches issued by
        # registry-update events ignore stale earlier responses
        # ("latest wins" semantics).
        self.latest_entity_list_id = 0
        self.latest_device_list_id = 0

        self.entity_debouncer = _TrailingDebouncer(
            REGISTRY_DEBOUNCE_INTERVAL, self.flush_pending_entities
        )
        self.device_debouncer = _TrailingDebouncer(
            REGISTRY_DEBOUNCE_INTERVAL, self.flush_pending_devices
        )

    # ---- refetch issuers --------------------------------------------------

    def refetch_entity_registry(self) -> None:
        """Issue a fresh entity_registry/list. Latest-wins semantics in
        :meth:`handle_entity_list` ensure earlier in-flight refetches
        whose responses arrive later are ignored.
        """
        self.latest_entity_list_id = self._send_ha_command(
            EntityList(), "config/entity_registry/list"
        )

    def refetch_device_registry(self) -> None:
        """Issue a fresh device_registry/list. See :meth:`refetch_entity_registry`."""
        self.latest_device_list_id = self._send_ha_command(
            DeviceList(), "config/device_registry/list"
        )

    def tick_periodic_refetch(self) -> None:
        """One tick of the periodic safety-net full refresh of both
        registries. Catches drift from missed events (subscribe silently
        dropped, an event the proxy didn't apply correctly, etc.).
        Skipped until the initial startup fetches complete: the periodic
        refresh's job is to heal drift, not to race startup.
        """
        if not (self.have_entities and self.have_devices):
            return
        self.refetch_entity_registry()
        self.refetch_device_registry()

    # ---- subscription-event ingress (per-event, debounced) ---------------

    def handle_subscription_event(
        self, mid: int, msg: dict[str, Any], *, kind: str
    ) -> None:
        """Process an inbound message on one of the registry-update
        subscriptions. ``kind`` is ``"entity"`` or ``"device"``.

        - A ``result`` with ``success: false`` is fatal: without live
          registry tracking the proxy would silently serve stale scope.
        - A ``result`` with ``success: true`` confirms the subscription;
          no further action.
        - Any ``event`` is buffered in the pending-events dict and the
          corresponding debouncer is poked. The flush callback decides
          whether to issue per-entity gets, drop entries locally, or
          promote to a full list refetch (see :meth:`flush_pending_entities`
          and :meth:`flush_pending_devices`).
        """
        event = subscription_frame(
            msg,
            on_failure=lambda: self._fatal(
                f"{kind}_registry update subscription failed"
            ),
        )
        if event is None:
            return
        data = event.get("data")
        if not isinstance(data, dict):
            return
        action = data.get("action")
        if not isinstance(action, str):
            return
        if action not in ("create", "update", "remove"):
            if not self._unknown_action_logged:
                self._unknown_action_logged = True
                self._log.debug(
                    "registry update event for %s carried unknown action %r; "
                    "ignoring (first occurrence this session)",
                    kind,
                    action,
                )
            return
        if kind == "entity":
            entity_id = data.get("entity_id")
            if not isinstance(entity_id, str):
                return
            # Multiple events for the same id within one debounce window
            # collapse to the latest action: a create+remove sequence
            # nets to a remove, an update after a create still needs a
            # fresh fetch, etc.
            self.pending_entity_events[entity_id] = action
            self.entity_debouncer.poke()
        else:  # "device"
            device_id = data.get("device_id")
            if not isinstance(device_id, str):
                return
            self.pending_device_events[device_id] = action
            self.device_debouncer.poke()

    # ---- list-response handlers (initial + refetch) ----------------------

    def handle_entity_list(self, mid: int, msg: dict[str, Any]) -> None:
        """Process an entity_registry/list response. Stale refetches (a
        response whose id is less than the most recent issued list-fetch)
        are ignored: the newer one's data is authoritative. A failed
        initial list is fatal; a failed refetch keeps the existing data.
        """
        if mid != self.latest_entity_list_id:
            return
        if not msg.get("success"):
            if not self.have_entities:
                self._fatal("config/entity_registry/list failed")
            else:
                self._log.warning("entity_registry/list refetch failed; keeping cache")
            return
        self.raw_entities = result_index(msg, "entity_id")
        # The fresh list supersedes any in-flight per-entity gets; their
        # late responses must not overwrite rows the list just delivered.
        self.pending_entity_gets.clear()
        self.have_entities = True
        self.rebuild_index_if_ready()

    def handle_device_list(self, mid: int, msg: dict[str, Any]) -> None:
        """Mirror of :meth:`handle_entity_list` for the device registry."""
        if mid != self.latest_device_list_id:
            return
        if not msg.get("success"):
            if not self.have_devices:
                self._fatal("config/device_registry/list failed")
            else:
                self._log.warning("device_registry/list refetch failed; keeping cache")
            return
        self.raw_devices = result_index(msg, "id")
        self.have_devices = True
        self.rebuild_index_if_ready()

    def handle_entity_get(self, entity_id: str, msg: dict[str, Any]) -> None:
        """Apply a per-entity get response to the cached raw list.
        Rebuilds the index once the whole burst's responses have landed
        (the pending-gets set is empty).

        A registry-remove flush can clear the pending-set entry while a
        get is in flight; this method treats the missing entry as a
        tombstone and skips the merge so a late response can't
        resurrect a removed row.
        """
        try:
            self.pending_entity_gets.remove(entity_id)
        except KeyError:
            return
        if msg.get("success"):
            result = msg.get("result")
            if isinstance(result, dict):
                self.raw_entities[entity_id] = result
        else:
            self._log.warning(
                "entity_registry/get failed for %s; cache row not updated",
                entity_id,
            )
        if not self.pending_entity_gets:
            self.rebuild_index_if_ready()

    # ---- debounced flush -------------------------------------------------

    def flush_pending_entities(self) -> None:
        """Apply the entity events accumulated during the debounce window.

        Mode ``full``: refetch the entire entity list (matches HA's own
        frontend pattern).

        Mode ``incremental``: drop removed rows locally, then either
        issue one ``entity_registry/get`` per remaining (create/update)
        entity, or, if the burst exceeds ``burst_threshold``, promote
        to a single full list refetch (cheaper than N round-trips for
        large bursts).
        """
        pending = self.pending_entity_events
        if not pending:
            return
        self.pending_entity_events = {}

        if self.mode == "full":
            self.refetch_entity_registry()
            return

        creates_updates: list[str] = []
        any_change = False
        for eid, action in pending.items():
            if action == "remove":
                if self.raw_entities.pop(eid, None) is not None:
                    any_change = True
                # Tombstone any in-flight get for this eid so its late
                # response doesn't resurrect the just-removed row. The
                # response handler skips the merge when the eid is no
                # longer in ``pending_entity_gets``.
                self.pending_entity_gets.discard(eid)
            else:
                creates_updates.append(eid)

        if not creates_updates:
            if any_change:
                self.rebuild_index_if_ready()
            return

        if self.burst_threshold > 0 and len(creates_updates) > self.burst_threshold:
            self.refetch_entity_registry()
            return

        for eid in creates_updates:
            self._fetch_entity_row(eid)

    def flush_pending_devices(self) -> None:
        """Apply the device events accumulated during the debounce window.

        Mode ``full``: refetch the entire device list.

        Mode ``incremental``: drop removed rows locally. HA has no
        per-device get command, so any create/update forces a full
        device-list refetch, but the device list is small relative to
        the entity list (typically hundreds vs thousands), so this is
        acceptable.
        """
        pending = self.pending_device_events
        if not pending:
            return
        self.pending_device_events = {}

        if self.mode == "full":
            self.refetch_device_registry()
            return

        needs_refetch = False
        any_change = False
        for did, action in pending.items():
            if action == "remove":
                if self.raw_devices.pop(did, None) is not None:
                    any_change = True
            else:
                needs_refetch = True
        if needs_refetch:
            self.refetch_device_registry()
        elif any_change:
            self.rebuild_index_if_ready()

    # ---- internal helpers ------------------------------------------------

    def _fetch_entity_row(self, entity_id: str) -> None:
        """Issue a per-entity get for an incremental-mode create/update
        event. Tracks the in-flight set so the index rebuild fires once
        the whole burst's responses have landed.
        """
        self.pending_entity_gets.add(entity_id)
        self._send_ha_command(
            EntityGet(entity_id),
            "config/entity_registry/get",
            entity_id=entity_id,
        )

    def rebuild_index_if_ready(self) -> None:
        """Rebuild the registry index from the cached raw lists, but
        only once both lists have arrived. Triggers a current-view
        re-resolve so any settings page waiting for the registry can
        now resolve.
        """
        if not (self.have_entities and self.have_devices):
            return
        self.index = entity_index.build(
            list(self.raw_entities.values()), list(self.raw_devices.values())
        )
        self._resolve_current_view()

    # ---- cleanup ---------------------------------------------------------

    def cancel_debouncers(self) -> None:
        """Cancel both pending-flush debouncer tasks. Called from
        Session cleanup so their handles don't survive the session.
        """
        self.entity_debouncer.cancel()
        self.device_debouncer.cancel()
