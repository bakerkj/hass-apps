# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Repo-wide guard: no add-on may join paho's network thread.

paho documents ``loop_stop()`` as blocking until the network thread finishes,
and that thread only exits once ``_out_packet`` and ``_out_messages`` are both
empty (client.py:2302-2306). A qos=1 message the broker never acknowledges --
the retained "offline" farewell, typically -- keeps ``_out_messages`` non-empty,
so the join never returns. Measured at >75s against a wedged broker, well past
the supervisor's SIGKILL grace, at which point the farewell is lost anyway.

Not the connect path: paho sets its own ``_connect_timeout`` of 5s and passes it
to ``socket.create_connection``, so a blackholed broker fails there in seconds
rather than at the kernel's ~127s SYN timeout. The unacked-publish case is the
unbounded one.

The thread is a daemon and every shutdown path here ends in process exit, so
there is nothing to gain by joining it and a hang to lose. This was live in
several add-ons at once, so it is enforced rather than documented.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _addon_sources() -> list[Path]:
    """Every add-on's own modules -- ``<addon>/<package>/*.py``."""
    out: list[Path] = []
    for addon in sorted(p for p in REPO_ROOT.iterdir() if p.is_dir()):
        if addon.name.startswith(".") or addon.name in {"scripts", "tests"}:
            continue
        for pkg in sorted(addon.iterdir()):
            if not pkg.is_dir() or pkg.name in {"tests", "__pycache__"}:
                continue
            out.extend(sorted(pkg.glob("*.py")))
    return out


def _loop_stop_calls(tree: ast.AST) -> list[int]:
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "loop_stop"
    ]


def test_sources_are_discovered() -> None:
    """A glob that silently matched nothing would pass every check below."""
    sources = _addon_sources()
    assert len(sources) > 20, f"only found {len(sources)} add-on modules"
    names = {p.parent.parent.name for p in sources}
    assert "direwolf_igate" in names and "turbostat_mqtt" in names, sorted(names)


@pytest.mark.parametrize("path", _addon_sources(), ids=lambda p: str(p.name))
def test_no_addon_joins_the_paho_network_thread(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    hits = _loop_stop_calls(tree)
    assert not hits, (
        f"{path.relative_to(REPO_ROOT)} calls loop_stop() at line(s) "
        f"{hits}. Joining paho's network thread can block past the SIGKILL "
        f"grace; disconnect() alone is sufficient (see this module's docstring)."
    )
