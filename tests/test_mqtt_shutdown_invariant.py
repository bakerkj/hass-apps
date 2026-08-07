# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Repo-wide guard: no add-on may join paho's network thread.

``loop_stop()`` joins that thread, and ``loop_forever`` only returns on its own
once ``_out_messages`` has drained -- so a retained qos=1 farewell left unacked
by a wedged broker blocks the join indefinitely. ``disconnect()`` does not help
when the thread is already inside a ``connect()`` syscall to a host that is
unreachable rather than refusing: that is a 60-120s TCP timeout on Linux, well
past the supervisor's SIGKILL grace.

The thread is a daemon and every shutdown path here ends in process exit, so
there is nothing to gain by joining it and a hang to lose. This was live in
seven of the eight MQTT add-ons at once, so it is enforced rather than
documented.
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
