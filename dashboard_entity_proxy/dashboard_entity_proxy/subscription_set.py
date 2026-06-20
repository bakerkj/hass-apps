# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-session bookkeeping for client ``subscribe_entities`` subscriptions.

Tracks two sets: ``live`` subscriptions are already receiving state
updates, while ``pending`` subscriptions are parked at request time
because mirror or scope wasn't ready. Pending entries are promoted to
live (and served their initial snapshot) when readiness flips.
"""


class SubscriptionSet:
    def __init__(self) -> None:
        self.live: set[int] = set()
        self.pending: set[int] = set()

    def add_live(self, cmd_id: int) -> None:
        self.live.add(cmd_id)

    def add_pending(self, cmd_id: int) -> None:
        self.pending.add(cmd_id)

    def promote_pending(self) -> list[int]:
        """Move every pending id into ``live`` and return the promoted
        ids in arbitrary order. Caller serves each id's initial snapshot.
        """
        promoted = list(self.pending)
        self.live.update(self.pending)
        self.pending.clear()
        return promoted

    def remove(self, sub: int) -> bool:
        """Remove ``sub`` from either set. Returns ``True`` if it was in
        either (caller should ack the unsubscribe locally); ``False``
        otherwise (caller should forward the unsubscribe to HA, since the
        id belongs to an HA-tracked subscription, not one we intercepted).
        """
        if sub in self.live:
            self.live.discard(sub)
            return True
        if sub in self.pending:
            self.pending.discard(sub)
            return True
        return False
