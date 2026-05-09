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


def run_user_action(cmd: str) -> int:
    """Run a user-supplied action *shell string* on the host via ``sh -c``.

    This is the ONE call site in the add-on that goes through a shell —
    by design.  Action commands are user-authored shell strings and need
    a shell to interpret pipes, redirects, env vars, etc.  ``cmd`` comes
    straight from the manifest and is never assembled from internal
    identifiers, so there is no internal escaping concern.
    """
    return subprocess.run([*_NSENTER, "sh", "-c", cmd], check=False).returncode
