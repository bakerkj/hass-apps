# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Repo-wide guard: a function that installs signal handlers must not block.

Python runs signal handlers only on the main thread and only between bytecodes.
A main thread parked in a blocking stream read acts on a signal only if the
syscall is interrupted, and even then PEP 475 retries it -- so a handler that
merely sets a stop flag never gets rechecked until the next line arrives. If
the child has gone quiet that is never, and SIGTERM is lost rather than
delayed, until the supervisor escalates to SIGKILL.

This shipped three times: direwolf_igate (#417, twice misdiagnosed as slow
teardown before that), turbostat_mqtt (#418) and intel_gpu_top_mqtt. Drain on a
worker thread and have the main thread wait with a timeout instead.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

_STREAMS = {"stdin", "stdout", "stderr"}
_BLOCKING_READS = {"read", "readline", "readlines"}


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


def _own_body(fn: ast.AST):
    """Nodes that run on this function's own thread of control.

    Nested ``def``/``lambda`` bodies are skipped: those run wherever they are
    handed to, which for a ``Thread`` target is precisely not here.
    """
    stack = list(getattr(fn, "body", []))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        yield node
        stack.extend(ast.iter_child_nodes(node))


def _is_stream(node: ast.AST) -> bool:
    """A pipe or stdio stream, as opposed to a list or a config object."""
    return isinstance(node, ast.Attribute) and node.attr in _STREAMS


def _installs_handler(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "signal"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "signal"
    )


def blocking_reads_in_handler_scope(source: str, filename: str = "<test>") -> list[str]:
    """Report blocking stream reads sharing a body with signal.signal()."""
    findings: list[str] = []
    for fn in ast.walk(ast.parse(source, filename=filename)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = list(_own_body(fn))
        if not any(_installs_handler(n) for n in body):
            continue
        for node in body:
            if isinstance(node, ast.For) and _is_stream(node.iter):
                findings.append(
                    f"{fn.name}(): blocking iteration at line {node.lineno}"
                )
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _BLOCKING_READS
                and _is_stream(node.func.value)
            ):
                findings.append(
                    f"{fn.name}(): {node.func.attr}() at line {node.lineno}"
                )
    return findings


def test_the_detector_actually_detects() -> None:
    """Positive control, so a refactor cannot quietly turn this into a no-op."""
    bad = (
        "import signal, sys\n"
        "def main():\n"
        "    signal.signal(signal.SIGTERM, lambda *a: None)\n"
        "    for line in sys.stdin:\n"
        "        pass\n"
    )
    assert blocking_reads_in_handler_scope(bad), "detector missed the known-bad shape"

    good = (
        "import signal, sys, threading\n"
        "def main():\n"
        "    signal.signal(signal.SIGTERM, lambda *a: None)\n"
        "    def drain():\n"
        "        for line in sys.stdin:\n"
        "            pass\n"
        "    threading.Thread(target=drain, daemon=True).start()\n"
        "    done.wait(0.25)\n"
    )
    assert not blocking_reads_in_handler_scope(good), "drain on a thread is the fix"


def test_sources_are_discovered() -> None:
    """A glob that silently matched nothing would pass every check below."""
    sources = _addon_sources()
    assert len(sources) > 20, f"only found {len(sources)} add-on modules"


@pytest.mark.parametrize("path", _addon_sources(), ids=lambda p: str(p.name))
def test_handler_scope_never_blocks_on_a_stream(path: Path) -> None:
    findings = blocking_reads_in_handler_scope(
        path.read_text(encoding="utf-8"), str(path)
    )
    assert not findings, (
        f"{path.relative_to(REPO_ROOT)} blocks the main thread while signal "
        f"handlers are installed: {findings}. Drain on a worker thread and wait "
        f"with a timeout (see this module's docstring)."
    )
