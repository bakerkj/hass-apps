# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Run a subprocess as an init-orphan via double-fork.

Plain ``subprocess.run`` accumulates the child's IO (rchar / read_bytes
etc.) into the parent's ``signal_struct->ioac`` via the ``wait4``
``wait_task_zombie`` path.  At ffmpeg-encoding scale that inflates the
daemon's ``/proc/<pid>/io`` totals by orders of magnitude — every byte
ffmpeg reads off the disk shows up as if the daemon read it.

This module reroutes that propagation away from the caller:

    caller → outer → manager → cmd
                ↑                ↑
                |                └── reaped by init (orphan)
                └── reaped by caller (≈0 IO; outer just forked + exited)

The caller only ``wait4``'s ``outer``, which did near-zero work.
``manager``'s wait4 of ``cmd`` DOES accumulate the IO into its own
``signal->ioac``, but manager is an init orphan, so init reaps it and
the accumulated IO goes to init's accounting — not back up to caller.
"""

from __future__ import annotations

import dataclasses
import json
import os
import subprocess


@dataclasses.dataclass
class DetachedResult:
    """Subset of ``subprocess.CompletedProcess`` forwarded over the pipe.

    ``returncode`` is the child's exit status; ``stderr`` is its captured
    stderr, truncated to ``stderr_max_len`` bytes by the manager.
    """

    returncode: int
    stderr: str


def run_detached(
    cmd: list[str],
    timeout: float | None,
    *,
    stderr_max_len: int = 8192,
) -> DetachedResult:
    """Run ``cmd`` as an init-orphan; return its returncode + stderr.

    Raises ``subprocess.TimeoutExpired`` if ``cmd`` exceeds ``timeout``.
    Raises ``RuntimeError`` if the manager process fails internally
    (e.g. ``cmd[0]`` not found, fork failure, JSON corruption).
    """
    status_r, status_w = os.pipe()
    pid_outer = os.fork()
    if pid_outer == 0:
        # outer (reaper).  One fork → manager → exit.  Manager is now
        # init's orphan; nothing here will wait4 it.
        try:
            os.close(status_r)
            pid_manager = os.fork()
            if pid_manager == 0:
                _manager_main(cmd, timeout, status_w, stderr_max_len)
        finally:
            os._exit(0)

    os.close(status_w)
    try:
        # outer exits immediately, so this returns ~instantly with
        # near-zero accumulated IO.
        os.waitpid(pid_outer, 0)
        # Block on the pipe until manager writes its JSON status — i.e.
        # until ``cmd`` has exited (or timed out, or errored).
        chunks: list[bytes] = []
        while True:
            chunk = os.read(status_r, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        payload = b"".join(chunks)
    finally:
        os.close(status_r)

    if not payload:
        raise RuntimeError("manager wrote no status")
    try:
        info = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"manager returned invalid status: {e}") from e

    if info.get("timeout"):
        raise subprocess.TimeoutExpired(cmd, timeout or 0.0)
    if "error" in info:
        raise RuntimeError(f"manager error: {info['error']}")
    return DetachedResult(
        returncode=info["rc"],
        stderr=info.get("stderr", ""),
    )


def _manager_main(
    cmd: list[str],
    timeout: float | None,
    status_w: int,
    stderr_max_len: int,
) -> None:
    """Run inside the manager process: execute ``cmd``, write JSON status."""
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        payload = json.dumps(
            {
                "rc": p.returncode,
                "stderr": (p.stderr or "")[:stderr_max_len],
            }
        ).encode("utf-8")
    except subprocess.TimeoutExpired:
        payload = json.dumps({"timeout": True}).encode("utf-8")
    except Exception as e:
        payload = json.dumps({"error": f"{type(e).__name__}: {e}"}).encode("utf-8")
    try:
        os.write(status_w, payload)
    finally:
        os.close(status_w)
        os._exit(0)
