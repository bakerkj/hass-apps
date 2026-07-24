# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Shape-tolerant unwrappers for HA WebSocket frames.

HA delivers ``subscribe_events`` notifications as
``{"type": "event", "event": {"data": {...}, ...}}`` and list responses
as ``{"success": true, "result": [<row>, ...]}``. Both shapes have edge
cases (missing keys, wrong types) that handlers want to short-circuit
on rather than raise; these helpers return ``None`` / empty dict so
callers can guard with a single ``if`` line.
"""

from collections.abc import Callable
from typing import Any


def subscription_frame(
    msg: dict[str, Any],
    *,
    on_failure: Callable[[], None],
) -> dict[str, Any] | None:
    """Triage one frame from a long-lived HA subscription.

    Returns ``msg["event"]`` (the inner event dict) when ``msg`` is a
    valid event frame.

    Returns ``None`` and calls ``on_failure()`` when ``msg`` is a
    ``result`` with ``success: false``. The caller decides whether
    that's fatal (mirror, registry subscriptions) or a warning
    (lovelace_updated).

    Returns ``None`` (without calling ``on_failure``) for the success
    ack on subscribe, any non-event/non-result frame, and any frame
    with a missing or wrong-typed ``event`` key.
    """
    mtype = msg.get("type")
    if mtype == "result":
        if not msg.get("success"):
            on_failure()
        return None
    if mtype != "event":
        return None
    event = msg.get("event")
    return event if isinstance(event, dict) else None


def event_data(msg: dict[str, Any]) -> dict[str, Any] | None:
    """Unwrap a HA ``event``-type message to its inner ``data`` dict, or
    ``None`` if either wrapper is missing or malformed.
    """
    event = msg.get("event")
    if not isinstance(event, dict):
        return None
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    return data


def result_index(msg: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    """Fold a list-of-dicts HA response into a dict keyed by ``row[key]``.

    Rows whose key is missing or non-string are silently skipped
    (matches the registry builder's tolerance for malformed rows).
    Returns an empty dict for any non-success / wrong-shape response.
    """
    if not msg.get("success"):
        return {}
    result = msg.get("result")
    if not isinstance(result, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in result:
        if not isinstance(row, dict):
            continue
        k = row.get(key)
        if isinstance(k, str):
            out[k] = row
    return out
