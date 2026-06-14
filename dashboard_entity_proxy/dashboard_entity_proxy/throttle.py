# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-session throttle buffer for coalescing mirror events into batched
adds/removes.

A burst of HA state-event frames during the throttle window collapses
to a single ``(adds, removes)`` pair at flush time. Union semantics on
both sets; removal supersedes add so an entity that vanishes during the
window only emits the removal.
"""

from __future__ import annotations

from typing import Any, Callable


class ThrottleBuffer:
    """Accumulated adds/removes during one throttle window."""

    def __init__(self) -> None:
        self.dirty: set[str] = set()
        self.removed: set[str] = set()

    def record(self, event: dict[str, Any]) -> None:
        """Buffer the entity ids touched by one mirror event. Multiple
        changes to the same id collapse; a remove ``r`` cancels any
        prior add for the same id and supersedes it.
        """
        for eid in event.get("a") or {}:
            self.removed.discard(eid)
            self.dirty.add(eid)
        for eid in event.get("c") or {}:
            self.removed.discard(eid)
            self.dirty.add(eid)
        for eid in event.get("r") or []:
            self.dirty.discard(eid)
            self.removed.add(eid)

    def drain(self, in_scope: Callable[[str], bool]) -> tuple[list[str], list[str]]:
        """Empty the buffer and return ``(adds, removes)``.

        Adds are filtered against ``in_scope``; don't add entities the
        client shouldn't see. Removes are NOT filtered: if scope tightened
        during the throttle window, an entity that was previously in scope
        but is no longer in the scope set is exactly the entity the client
        needs to be told disappeared. Filtering removals through the new
        scope drops them and leaves the entity stuck in the client's
        state until reconnect.
        """
        add = sorted(eid for eid in self.dirty if in_scope(eid))
        rem = sorted(self.removed)
        self.dirty.clear()
        self.removed.clear()
        return add, rem
