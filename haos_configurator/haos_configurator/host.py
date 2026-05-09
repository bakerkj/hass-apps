# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Host-namespace shell-out primitives.

Every internal call here passes argv as a *list* — there is no shell
parsing of any path or filename, so manifest paths cannot inject.  The
single ``sh -c`` call site is :func:`run_user_action`, which is by
design (action commands are user-authored shell strings).
"""

from __future__ import annotations

import subprocess
from typing import IO

# Host mount + UTS + IPC.  *Not* network — actions that genuinely need
# the host net ns can prefix with ``nsenter -t 1 -n -- …`` themselves.
# PID is irrelevant: the add-on already runs with ``host_pid: true``.
_NSENTER = ["nsenter", "-t", "1", "-m", "-u", "-i", "--"]


def host_run(
    argv: list[str],
    *,
    check: bool = True,
    stdin: IO[bytes] | int | None = None,
    stdout: int | None = subprocess.PIPE,
) -> subprocess.CompletedProcess[bytes]:
    """Run ``argv`` in the host's mount/UTS/IPC namespaces.  ``argv`` is
    a list of strings — there is no shell, so user-controlled path
    components cannot escape.
    """
    return subprocess.run(
        [*_NSENTER, *argv],
        check=check,
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
    )


def host_isfile(path: str) -> bool:
    return host_run(["test", "-f", path], check=False).returncode == 0


def host_sha256(path: str) -> str | None:
    """sha256 of a file on the host, or ``None`` if it doesn't exist."""
    if not host_isfile(path):
        return None
    out = host_run(["sha256sum", path]).stdout.decode().strip()
    return out.split(maxsplit=1)[0] if out else None


def run_user_action(cmd: str | list[str]) -> int:
    """Run a user-supplied action on the host.

    Two forms are accepted:

    * **Array form** (recommended): ``cmd`` is a list of argv strings,
      exec'd directly via ``nsenter``.  No shell, so paths and arguments
      cannot shell-inject.  No pipes, redirects, or env-var expansion.
    * **String form**: ``cmd`` is a shell string, exec'd via
      ``nsenter sh -c``.  Use this if the action genuinely needs a shell
      (pipes, redirects, env vars, etc.).

    Output is inherited (stdout/stderr go to the add-on log) so the user
    sees what their command printed.
    """
    if isinstance(cmd, list):
        argv = [*_NSENTER, *cmd]
    else:
        argv = [*_NSENTER, "sh", "-c", cmd]
    return subprocess.run(argv, check=False).returncode
