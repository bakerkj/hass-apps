# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Tests for the double-fork ``run_detached`` helper."""

import os
import subprocess
from pathlib import Path

import pytest

from frigate_compressor.detached_subprocess import run_detached


# ─── basic behaviour ────────────────────────────────────────────────────────


def test_returncode_zero():
    r = run_detached(["true"], timeout=10)
    assert r.returncode == 0
    assert r.stderr == ""


def test_returncode_nonzero():
    r = run_detached(["sh", "-c", "exit 42"], timeout=10)
    assert r.returncode == 42


def test_stderr_captured():
    r = run_detached(["sh", "-c", "echo oh-no >&2; exit 1"], timeout=10)
    assert r.returncode == 1
    assert "oh-no" in r.stderr


def test_stderr_truncated():
    # Emit 50 KB of stderr; truncate to 1 KB.
    r = run_detached(
        ["sh", "-c", "head -c 50000 /dev/urandom | xxd | head -c 50000 >&2"],
        timeout=10,
        stderr_max_len=1024,
    )
    assert r.returncode == 0
    assert len(r.stderr) <= 1024


def test_timeout_raises():
    with pytest.raises(subprocess.TimeoutExpired):
        run_detached(["sleep", "5"], timeout=0.5)


def test_command_not_found_raises_runtime_error():
    with pytest.raises(RuntimeError, match="manager error"):
        run_detached(["/nonexistent/binary/xyzzy"], timeout=5)


# ─── the empirical IO-attribution test ──────────────────────────────────────


def _read_self_io() -> dict[str, int]:
    """Read ``/proc/self/io`` into a dict."""
    out = {}
    for line in Path("/proc/self/io").read_text().splitlines():
        k, _, v = line.partition(":")
        out[k.strip()] = int(v.strip())
    return out


# A command that reads a lot of bytes off disk.  Each invocation should
# attribute several MB of rchar to whoever wait4()'s it.
_HEAVY_READ_CMD = [
    "sh",
    "-c",
    # Read 16 MB from /dev/urandom into a temp file, then read it back
    # twice.  Uses ``read``/``write`` syscalls so rchar accumulates.
    "dd if=/dev/urandom of=/tmp/_run_detached_test bs=1M count=16 2>/dev/null"
    " && cat /tmp/_run_detached_test >/dev/null"
    " && cat /tmp/_run_detached_test >/dev/null"
    " && rm -f /tmp/_run_detached_test",
]


@pytest.mark.skipif(
    not Path("/proc/self/io").exists(),
    reason="needs Linux /proc/self/io accounting",
)
def test_io_does_not_propagate_to_caller():
    """Run a heavy-IO subprocess and check the caller's rchar barely moves.

    The same workload run via plain ``subprocess.run`` would propagate
    ~32 MB of rchar to this process via wait4 attribution.  Through the
    double-fork helper the caller should see only the JSON status pipe
    traffic — well under 100 KB.
    """
    # Warm up: one untimed run so any first-time imports / page faults
    # don't show up in our delta.
    run_detached(_HEAVY_READ_CMD, timeout=30)

    before = _read_self_io()
    for _ in range(3):
        r = run_detached(_HEAVY_READ_CMD, timeout=30)
        assert r.returncode == 0
    after = _read_self_io()

    delta_rchar = after["rchar"] - before["rchar"]
    # 3 runs × 32 MB heavy-read each = ~96 MB of rchar that *would*
    # propagate via plain subprocess.run.  The pipe payload is a short
    # JSON object per run.  Anything under 1 MB total is comfortably in
    # "the helper worked" territory.
    assert delta_rchar < 1_000_000, (
        f"IO propagated to caller: rchar +{delta_rchar} bytes over 3 runs"
        f" (expected < 1 MB)"
    )


@pytest.mark.skipif(
    not Path("/proc/self/io").exists(),
    reason="needs Linux /proc/self/io accounting",
)
def test_plain_subprocess_run_does_propagate_io():
    """Sanity check: confirm the same workload via ``subprocess.run``
    ACTUALLY propagates IO to this process, so the previous test isn't
    a tautology.

    If this test ever fails (i.e., plain subprocess.run stops propagating)
    the kernel behaviour we're working around has changed and the
    double-fork helper may be unnecessary.
    """
    subprocess.run(_HEAVY_READ_CMD, capture_output=True, timeout=30)  # warm up

    before = _read_self_io()
    for _ in range(3):
        subprocess.run(_HEAVY_READ_CMD, capture_output=True, timeout=30)
    after = _read_self_io()

    delta_rchar = after["rchar"] - before["rchar"]
    # Should see ~3 × 32 MB = ~96 MB.  Use a generous floor (16 MB) to
    # avoid flakiness from caching.
    assert delta_rchar > 16_000_000, (
        f"plain subprocess.run did not propagate IO as expected: rchar"
        f" +{delta_rchar} bytes over 3 runs (expected > 16 MB)"
    )


# ─── orphan reaping check ───────────────────────────────────────────────────


def test_no_zombies_left_behind():
    """After several runs, no defunct children should remain attached."""
    for _ in range(5):
        run_detached(["true"], timeout=5)

    # The outer reaper exits immediately and the caller wait4()'s it.
    # The manager is reparented to init, which reaps it.  Neither should
    # show up as a zombie under us.
    pid = os.getpid()
    children_dir = Path(f"/proc/{pid}/task/{pid}/children")
    if children_dir.exists():
        children = children_dir.read_text().split()
        # Filter out any zombie children of ours.
        zombies = []
        for cpid in children:
            try:
                state_line = Path(f"/proc/{cpid}/stat").read_text()
                # state is the third whitespace-separated field after
                # "(comm)" — handle comm with parens by splitting from the right.
                rest = state_line.rsplit(")", 1)[1].strip().split()
                state = rest[0]
                if state == "Z":
                    zombies.append(cpid)
            except FileNotFoundError:
                pass
        assert not zombies, f"zombies left behind: {zombies}"
