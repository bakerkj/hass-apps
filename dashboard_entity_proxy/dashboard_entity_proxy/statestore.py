# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Maintains a per-session entity-state mirror from the compressed
``subscribe_entities`` stream. HA's compact event format uses three
top-level keys ``a`` / ``c`` / ``r`` for added / changed / removed.

One ``StateStore`` is constructed per intercepted client connection
(see ``session.Session.__init__``); there is NO shared store across
clients. Each session mirrors the unfiltered upstream subscription on
its own namespaced id, and the proxy answers the client's own
(scope-filtered) ``subscribe_entities`` from that mirror.

No mutex: the proxy runs on a single asyncio event loop, so all access
happens cooperatively on one thread and the store is only ever
read/written at await-free points, with no data races to guard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EntityState:
    """One entity's mirrored state: the value string, attribute dict,
    opaque context, and the two timestamps HA tracks (last_changed and
    last_updated). All fields default to empty/zero so a partial diff can
    be applied before a full snapshot has arrived.
    """

    state: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)
    context: Any = None  # opaque: a context id string or an object
    last_changed: float = 0.0
    last_updated: float = 0.0


class StateStore:
    """Full entity-state mirror for one client session, updated by applying
    ``subscribe_entities`` events (added / changed / removed).
    """

    def __init__(self) -> None:
        """Construct an empty mirror; entities are added by ``apply``."""
        self._entities: dict[str, EntityState] = {}

    def apply(self, event: dict[str, Any]) -> None:
        """Apply one compressed ``subscribe_entities`` event to the mirror.
        Adds (``a``), changes (``c``), and removes (``r``) are processed
        in that order. Unknown / empty keys are silently ignored.
        """
        for eid, cs in (event.get("a") or {}).items():
            self._entities[eid] = _from_compressed(cs)
        for eid, diff in (event.get("c") or {}).items():
            self._apply_diff(eid, diff)
        for eid in event.get("r") or []:
            self._entities.pop(eid, None)

    def _apply_diff(self, eid: str, diff: dict[str, Any]) -> None:
        """Apply one entity's change diff. ``+`` carries set fields
        (``s`` state, ``c`` context, ``lc``/``lu`` timestamps, ``a``
        attribute additions); ``-`` carries removed attribute keys. The
        entity is auto-created if the diff arrives before any add, since
        HA sometimes does this for entities that existed pre-subscription.
        """
        es = self._entities.get(eid)
        if es is None:
            es = EntityState()
            self._entities[eid] = es
        plus = diff.get("+")
        if plus:
            if "s" in plus:
                es.state = plus["s"]
            if "c" in plus:
                es.context = plus["c"]
            if "lc" in plus:
                es.last_changed = plus["lc"]
            if "lu" in plus:
                es.last_updated = plus["lu"]
            elif "lc" in plus:
                es.last_updated = plus["lc"]
            for key, val in (plus.get("a") or {}).items():
                es.attributes[key] = val
        minus = diff.get("-")
        if minus:
            for key in minus.get("a") or []:
                es.attributes.pop(key, None)

    def snapshot(self, ids: list[str] | None) -> dict[str, dict[str, Any]]:
        """Compressed "added" map for the given ids (or all when ids is None)."""
        if ids is None:
            return {eid: _to_compressed(es) for eid, es in self._entities.items()}
        return {
            eid: _to_compressed(self._entities[eid])
            for eid in ids
            if eid in self._entities
        }

    def get(self, eid: str) -> EntityState | None:
        """Return the mirrored state for ``eid`` or ``None`` if unknown."""
        return self._entities.get(eid)

    def ids(self) -> list[str]:
        """Sorted list of every entity id currently in the mirror."""
        return sorted(self._entities)

    def __len__(self) -> int:
        """Number of entities currently mirrored. Used for status reporting."""
        return len(self._entities)


def _from_compressed(cs: dict[str, Any]) -> EntityState:
    """Decode HA's compressed ``a`` entry into an ``EntityState``. Missing
    timestamps default to zero; ``last_updated`` falls back to
    ``last_changed`` when omitted (HA's wire convention for "same time").
    """
    last_changed = cs.get("lc", 0.0)
    return EntityState(
        state=cs.get("s", ""),
        attributes=dict(cs.get("a") or {}),
        context=cs.get("c"),
        last_changed=last_changed,
        last_updated=cs.get("lu", last_changed),
    )


def _to_compressed(es: EntityState) -> dict[str, Any]:
    """Encode an ``EntityState`` back into HA's compressed wire form. Omits
    ``c`` and ``lu`` when redundant (same rules as the decoder), so the
    re-emitted snapshot matches what HA would have sent originally.
    """
    cs: dict[str, Any] = {
        "s": es.state,
        "a": dict(es.attributes),
        "lc": es.last_changed,
    }
    if es.context is not None:
        cs["c"] = es.context
    if es.last_updated != es.last_changed:
        cs["lu"] = es.last_updated
    return cs
