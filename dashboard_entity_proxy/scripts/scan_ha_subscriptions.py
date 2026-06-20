#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Extract every Home Assistant WebSocket subscription command type
from a HA source tree.

Used to regenerate ``HA_SUBSCRIPTION_COMMANDS`` in
``dashboard_entity_proxy/const.py`` when the addon's pinned HA version
bumps. Subscription handlers are detected by their function body's
``connection.subscriptions[msg["id"]] = ...`` registration — that's the
single canonical hook HA uses to mark an id as a long-lived stream.

Usage (against the HA docker image the integration suite pins to)::

    docker run --rm \\
      -v "$(pwd)/dashboard_entity_proxy/scripts/scan_ha_subscriptions.py:/tmp/scan.py" \\
      --entrypoint python3 \\
      ghcr.io/home-assistant/home-assistant:2026.6.2 \\
      /tmp/scan.py /usr/src/homeassistant/homeassistant

Or against a checkout::

    python3 dashboard_entity_proxy/scripts/scan_ha_subscriptions.py path/to/core/homeassistant

Outputs sorted command-type strings, one per line.
"""

import ast
import sys
from pathlib import Path


def _is_subscription_handler(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return True if ``fn`` writes to ``connection.subscriptions[...]``."""
    for node in ast.walk(fn):
        if isinstance(node, ast.Subscript):
            obj = node.value
            if (
                isinstance(obj, ast.Attribute)
                and obj.attr == "subscriptions"
                and isinstance(obj.value, ast.Name)
                and obj.value.id == "connection"
            ):
                return True
    return False


def _cmd_type_from_decorator(dec: ast.expr) -> str | None:
    """Extract the ``type`` string from a ``@websocket_command(...)`` decorator.

    HA schemas use either ``"type": "..."`` (bare key) or
    ``vol.Required("type"): "..."`` — both shapes are recognised.
    """
    if not isinstance(dec, ast.Call):
        return None
    name = ""
    if isinstance(dec.func, ast.Attribute):
        name = dec.func.attr
    elif isinstance(dec.func, ast.Name):
        name = dec.func.id
    if name != "websocket_command":
        return None
    if not dec.args:
        return None
    schema = dec.args[0]
    if not isinstance(schema, ast.Dict):
        return None
    for k, v in zip(schema.keys, schema.values):
        if isinstance(k, ast.Constant) and k.value == "type":
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                return v.value
        elif isinstance(k, ast.Call):
            # vol.Required("type")
            if (
                k.args
                and isinstance(k.args[0], ast.Constant)
                and k.args[0].value == "type"
                and isinstance(v, ast.Constant)
                and isinstance(v.value, str)
            ):
                return v.value
    return None


def scan(root: Path) -> set[str]:
    """Walk ``root`` and return the set of subscription command-type
    strings registered anywhere under it.
    """
    found: set[str] = set()
    for py in root.rglob("*.py"):
        try:
            tree = ast.parse(py.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not _is_subscription_handler(node):
                continue
            for dec in node.decorator_list:
                t = _cmd_type_from_decorator(dec)
                if t:
                    found.add(t)
                    break
    return found


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    if not root.is_dir():
        print(f"not a directory: {root}", file=sys.stderr)
        return 1
    for t in sorted(scan(root)):
        print(t)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
