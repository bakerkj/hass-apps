# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Home Assistant WebSocket message helpers: a frame splitter that handles
both single-object frames and coalesced JSON-array frames, plus small
message builders for the few outbound messages the proxy synthesizes.
"""

import json
import logging
from typing import Any

_log = logging.getLogger(__name__)


def parse_messages(data: str) -> list[dict[str, Any]] | None:
    """Parse a WebSocket text frame into its messages.

    Home Assistant may coalesce several messages into one frame as a JSON array;
    each element is returned separately. A single object yields a one-item list.
    Returns ``None`` if the frame is not valid JSON (the caller should forward it
    unchanged).

    Logs at DEBUG when input shape is unexpected (top-level scalar/null) or when
    array filtering drops non-object items; silent filtering of a buggy or
    malicious upstream leaves no diagnostic trail otherwise.
    """
    try:
        value = json.loads(data)
    except json.JSONDecodeError:
        return None
    if isinstance(value, list):
        kept = [m for m in value if isinstance(m, dict)]
        dropped = len(value) - len(kept)
        if dropped:
            _log.debug(
                "parse_messages: dropped %d non-object item(s) from array of %d",
                dropped,
                len(value),
            )
        return kept
    if isinstance(value, dict):
        return [value]
    _log.debug(
        "parse_messages: top-level JSON shape is %s, not dict/list",
        type(value).__name__,
    )
    return []


def dumps(obj: Any) -> str:
    """Serialize a message compactly."""
    return json.dumps(obj, separators=(",", ":"))


def event_message(sub_id: int, event: dict[str, Any]) -> str:
    """Build an ``event`` envelope for the given subscription id."""
    return dumps({"id": sub_id, "type": "event", "event": event})


def result_ok(cmd_id: int) -> str:
    """Build a successful command result."""
    return dumps({"id": cmd_id, "type": "result", "success": True, "result": None})


def command(cmd_id: int, msg_type: str, **fields: Any) -> str:
    """Build a generic command message."""
    return dumps({"id": cmd_id, "type": msg_type, **fields})
