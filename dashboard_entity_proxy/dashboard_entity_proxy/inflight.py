# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Bookkeeping for the per-session inflight dispatch table."""

from __future__ import annotations

import collections

from .const import INFLIGHT_SWEPT_RECALL_MAX
from .inflight_types import (
    ClientConfig,
    ClientReq,
    ClientUnsubscribe,
    InflightEntry,
)


class InflightTable:
    """Per-session dispatch-table bookkeeping.

    Maintains the ``mid -> InflightEntry`` dispatch table plus the
    parallel structures it depends on: a known-subscriptions set (ids
    that emit events past the initial ack and so escape the sweep),
    sweep deadlines for unclassified one-shots, a reverse index from
    client id to proxy-allocated id (for O(1) unsubscribe rewrite), a
    bounded ring of recently-swept client-command ids so a late HA
    result can still be routed back, and per-session cap-rejection
    counters. Every ``insert``/``retag``/``pop`` keeps the reverse
    index consistent; every sweep pop records its client id in the
    recall ring.
    """

    def __init__(self) -> None:
        self.table: dict[int, InflightEntry] = {}
        self.known_subscriptions: set[int] = set()
        self.pop_at: dict[int, float] = {}
        self.client_to_ha: dict[int, int] = {}
        self.swept_to_client_id: collections.OrderedDict[int, int] = (
            collections.OrderedDict()
        )
        self.cap_hits: int = 0
        self.cap_log_emitted: bool = False

    # ---- size / membership ------------------------------------------------

    def __len__(self) -> int:
        return len(self.table)

    def get(self, mid: int) -> InflightEntry | None:
        return self.table.get(mid)

    # ---- insert / retag / pop --------------------------------------------

    def insert(self, mid: int, entry: InflightEntry) -> None:
        """Record a brand-new entry for ``mid`` (the contract is that
        ``mid`` has not been issued before).

        Raises ``ValueError`` if ``mid`` is already present, to surface
        duplicate-id bugs immediately rather than silently corrupting
        the parallel sets. Uses an explicit ``raise`` (not ``assert``)
        so the check survives ``python -O``.
        """
        if mid in self.table:
            raise ValueError(
                f"InflightTable.insert called for existing id {mid}; use retag"
            )
        self.table[mid] = entry
        if isinstance(entry, (ClientReq, ClientConfig)):
            self.client_to_ha[entry.client_id] = mid

    def retag(self, mid: int, entry: InflightEntry) -> None:
        """Rewrite the dispatch-table entry for an already-inserted ``mid``.

        Used for the two retag transitions the proxy performs after the
        initial ``ClientReq(cid)`` insert is known: ``ClientReq`` to
        ``ClientConfig`` (lovelace/config feeds scope resolution) and
        ``ClientReq`` to ``ClientUnsubscribe`` (so the dispatcher knows
        which subscription to retire on ack). Keeps the reverse-index
        slot under the same original client id so lookups stay correct.
        """
        if mid not in self.table:
            raise ValueError(
                f"InflightTable.retag called for missing id {mid}; use insert"
            )
        prev = self.table[mid]
        if isinstance(prev, (ClientReq, ClientConfig)):
            self.client_to_ha.pop(prev.client_id, None)
        self.table[mid] = entry
        if isinstance(entry, (ClientReq, ClientConfig)):
            self.client_to_ha[entry.client_id] = mid

    def pop(self, mid: int) -> None:
        """Pop an entry and clear its reverse-index slot. No-op when the
        entry doesn't carry a client id, or when ``mid`` is unknown.
        """
        prev = self.table.pop(mid, None)
        if isinstance(prev, (ClientReq, ClientConfig)):
            self.client_to_ha.pop(prev.client_id, None)

    # ---- subscription classification -------------------------------------

    def is_known(self, mid: int) -> bool:
        return mid in self.known_subscriptions

    def mark_known(self, mid: int) -> None:
        """Promote ``mid`` to a long-lived subscription. Cancels any
        pending sweep deadline.
        """
        self.known_subscriptions.add(mid)
        self.pop_at.pop(mid, None)

    def discard_known(self, mid: int) -> None:
        self.known_subscriptions.discard(mid)

    def retag_as_unsubscribe(self, new_id: int, ha_sub_id: int) -> None:
        """Retag the ``ClientReq`` entry at ``new_id`` (a just-inserted
        client unsubscribe command) as a ``ClientUnsubscribe`` carrying
        ``ha_sub_id``, so the dispatcher can retire the right
        subscription on ack. No-op if the entry isn't a ``ClientReq``.
        """
        entry = self.table.get(new_id)
        if isinstance(entry, ClientReq):
            self.retag(new_id, ClientUnsubscribe(entry.client_id, ha_sub_id))

    def classify_inbound(self, mid: int, mtype: str | None, *, now: float) -> None:
        """Classify the first HA-side frame for an unclassified
        ``ClientReq`` entry. No-op if ``mid`` is already known.

        * An ``event`` frame promotes the entry to a known subscription
          (and cancels any pending sweep, see :meth:`mark_known`).
        * A ``result`` frame schedules the entry for the grace-period
          sweep at ``now + INFLIGHT_GRACE_SECONDS``.
        * Any other ``mtype`` is ignored.
        """
        from .const import INFLIGHT_GRACE_SECONDS

        if mid in self.known_subscriptions:
            return
        if mtype == "event":
            self.mark_known(mid)
        elif mtype == "result":
            self.schedule_pop(mid, now + INFLIGHT_GRACE_SECONDS)

    # ---- sweep ------------------------------------------------------------

    def schedule_pop(self, mid: int, deadline: float) -> None:
        self.pop_at[mid] = deadline

    def cancel_pop(self, mid: int) -> None:
        self.pop_at.pop(mid, None)

    def sweep(self, now: float) -> int:
        """Pop client-command entries whose grace-period deadline is in
        the past. Each popped ``ClientReq`` lands in the swept-recall
        ring so a late HA result can still be routed back.

        Returns the number of entries reclaimed.
        """
        expired = [mid for mid, t in self.pop_at.items() if t <= now]
        for mid in expired:
            entry = self.table.get(mid)
            if isinstance(entry, ClientReq):
                self.record_swept_client_id(mid, entry.client_id)
            self.pop(mid)
            self.pop_at.pop(mid, None)
        return len(expired)

    # ---- swept-recall ring ------------------------------------------------

    def recall_swept(self, mid: int) -> int | None:
        """Look up a swept entry's client id; consumes the recall slot
        on hit (one late ack per swept id).
        """
        return self.swept_to_client_id.pop(mid, None)

    def record_swept_client_id(self, ha_id: int, client_id: int) -> None:
        """Remember a swept ``ClientReq``'s HA-allocated id and original
        client id. Bounded ring: oldest entries evicted past
        ``INFLIGHT_SWEPT_RECALL_MAX``.
        """
        self.swept_to_client_id.pop(ha_id, None)
        self.swept_to_client_id[ha_id] = client_id
        while len(self.swept_to_client_id) > INFLIGHT_SWEPT_RECALL_MAX:
            self.swept_to_client_id.popitem(last=False)

    # ---- reverse index ----------------------------------------------------

    def find_ha_id_for_client(self, client_id: int) -> int | None:
        return self.client_to_ha.get(client_id)

    # ---- cap diagnostics --------------------------------------------------

    def note_cap_hit(self) -> bool:
        """Bump the cap-hit counter. Returns ``True`` on the first call
        per session and ``False`` after; caller uses the return value
        to gate a one-time WARN log so a misbehaving client doesn't
        spam the logs once it hits the cap.
        """
        self.cap_hits += 1
        if self.cap_log_emitted:
            return False
        self.cap_log_emitted = True
        return True

    # ---- status snapshot --------------------------------------------------

    def status(self) -> dict[str, int]:
        """Snapshot slice merged into the per-session ``Session.status``."""
        return {
            "inflight_count": len(self.table),
            "inflight_cap_hits": self.cap_hits,
        }
