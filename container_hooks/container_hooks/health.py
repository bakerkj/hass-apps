# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Health checks: did our pre-start work actually land on this instance?

Two independent signals per target container, evaluated by
``check_applied`` and ``check_sentinel``:

* ``applied`` — the addon's own record. True iff ``pre-start.log`` for
  the container has a successful ``put_archive`` entry whose timestamp
  is at or after the container's current ``StartedAt``. Answers
  "did container_hooks fire the pre-start hook for this lifecycle."

* ``sentinel`` — an in-target sanity check. Requires ``success_sentinel``
  in the container_overrides entry. The sentinel path is either on
  tmpfs (freshness by existence: tmpfs is remounted at every start) or
  on the writable overlay (freshness by mtime >= StartedAt). The tmpfs
  case is auto-detected via ``findmnt``. Answers "did the payload we
  staged actually execute in the target on this lifecycle."

Both checks return ``None`` when the underlying data isn't available
(container gone, log unreadable, docker exec failing) so the caller
can decide whether to report ``unknown``, hold state, or skip publish.
"""

import asyncio
import datetime
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiodocker

# Tail window for scanning ``pre-start.log`` for the newest successful
# put_archive line. Sized to absorb an unusually chatty single lifecycle
# — apply_patch and pre-start ``*.sh`` scripts also append here, and a
# 64 KiB window can miss the put_archive line if a script writes a lot
# of output. 1 MiB is a generous ceiling before we consider it worth
# reversing over the whole file.
_LAST_LINE_SCAN_BYTES = 1_048_576
_STARTED_AT_KEYS = ("StartedAt", "started_at")
# Wall-clock slack absorbing the gap between when container_hooks logs
# a successful put_archive and when the Docker daemon stamps the
# container's StartedAt. Two cases feed this:
#
# 1. Supervisor-shaped ``docker create → docker start`` fires the events
#    within milliseconds; put_archive completes shortly after create,
#    and StartedAt lands within a couple of seconds.
# 2. Third-party ``docker create X; sleep N; docker start X`` splits
#    create and start by an arbitrary interval — the pre-start hook
#    legitimately fired for the current lifecycle but the log entry
#    predates StartedAt by that whole interval.
#
# 300s is generous enough to cover case 2 for almost anything realistic
# (nobody delay-starts a container more than 5 minutes after creating
# it) without so large that a genuinely-stale hook from a prior
# lifecycle sneaks through. Documented in the README under health.
_APPLIED_SLACK_SECONDS = 300.0
_SENTINEL_SLACK_SECONDS = 30.0
# Ceiling on a single docker-API call (containers.get / show / exec).
# Without this, a hung daemon or a target stuck in D-state can freeze
# the whole health poll cycle for aiodocker's ~60s default, blocking
# every subsequent container's poll. 10s is well past the realistic
# path but bounded enough that a stuck container turns into
# ``unknown`` in the next poll instead of stalling the addon.
_DOCKER_CHECK_TIMEOUT = 10.0


@dataclass(frozen=True)
class HealthResult:
    """Outcome of one health check.

    ``value`` is the boolean state (True = healthy). ``reason`` is a
    short human-readable diagnosis suitable for a log line or an MQTT
    JSON attribute — populated for both outcomes so a red sensor
    always carries a "why".
    """

    value: bool
    reason: str


async def container_started_at(
    docker: aiodocker.Docker, container: str
) -> float | None:
    """Return the target container's ``StartedAt`` as a unix timestamp.

    ``None`` if the container is missing, not started yet, the daemon
    returns a value we cannot parse, or the call takes longer than
    ``_DOCKER_CHECK_TIMEOUT``. The caller treats ``None`` as "cannot
    evaluate this check right now" and leaves the sensor at its
    previous value rather than churning it.
    """
    try:
        info = await asyncio.wait_for(
            _fetch_show(docker, container), timeout=_DOCKER_CHECK_TIMEOUT
        )
    except TimeoutError:
        return None
    except aiodocker.exceptions.DockerError:
        return None
    except Exception:  # noqa: BLE001 -- daemon returned nonsense; bail
        return None
    if info is None:
        return None
    state = info.get("State") or {}
    for key in _STARTED_AT_KEYS:
        raw = state.get(key)
        if raw:
            parsed = _parse_docker_ts(raw)
            if parsed is not None:
                return parsed
    return None


async def _fetch_show(
    docker: aiodocker.Docker, container: str
) -> dict[str, Any] | None:
    c = await docker.containers.get(container)
    return await c.show()


def _parse_docker_ts(raw: str) -> float | None:
    """Parse an ISO-8601 Docker timestamp to a unix float.

    Docker emits ``2026-09-19T22:47:38.140882144Z`` (nanosecond precision,
    trailing ``Z``). ``datetime.fromisoformat`` on 3.11+ accepts ``Z``
    but chokes on the sub-microsecond digits; trim to microseconds first.
    An unparsable value (e.g. the zero-time ``0001-01-01T00:00:00Z``
    Docker uses for a never-started container) returns ``None``.
    """
    s = raw.strip()
    if not s or s.startswith("0001-"):
        return None
    # Truncate to microseconds: Docker uses nanoseconds; Python parses microseconds.
    if "." in s:
        head, _, tail = s.partition(".")
        # tail like "140882144Z" or "140882144+00:00"
        m = re.match(r"(\d+)(.*)$", tail)
        if m:
            frac, rest = m.group(1), m.group(2)
            frac = (frac + "000000")[:6]
            s = f"{head}.{frac}{rest}"
    # Normalize Z -> +00:00 for fromisoformat compatibility on older stdlibs.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


_PUT_ARCHIVE_LOG_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T[^\]]+)\]\s+put_archive ok:"
)


def last_put_archive_ts(log_path: Path) -> float | None:
    """Scan ``pre-start.log`` for the most recent successful put_archive.

    Reads only the tail of the file (last ~64 KiB) — the log grows
    monotonically per container, so the last entry is always what we
    want and rewinding from the end costs one small read. Returns
    ``None`` if the file does not exist, is empty, or contains no
    successful entry yet.
    """
    try:
        st = log_path.stat()
    except FileNotFoundError:
        return None
    except OSError:
        return None
    seeked = False
    with log_path.open("rb") as f:
        if st.st_size > _LAST_LINE_SCAN_BYTES:
            f.seek(st.st_size - _LAST_LINE_SCAN_BYTES)
            seeked = True
        data = f.read()
    # If we seeked, the first line in the buffer is likely a partial
    # (we landed mid-line). Drop everything up to the first newline
    # so a straddling put_archive line doesn't get regex-rejected as
    # a fragment; the newest complete put_archive entry within the
    # scan window is what we're after.
    if seeked:
        nl = data.find(b"\n")
        if nl >= 0:
            data = data[nl + 1 :]
    text = data.decode("utf-8", errors="replace")
    # Iterate lines in reverse so the first match is the newest.
    best: float | None = None
    for line in reversed(text.splitlines()):
        m = _PUT_ARCHIVE_LOG_RE.match(line)
        if not m:
            continue
        parsed = _parse_docker_ts(m.group("ts"))
        if parsed is not None:
            best = parsed
            break
    return best


async def check_applied(
    docker: aiodocker.Docker,
    container: str,
    pre_start_log_path: Path,
) -> HealthResult | None:
    """Compare ``pre-start.log``'s newest entry to the container's start.

    * ``value=True`` when the log's newest entry is at or after
      ``StartedAt``: container_hooks ran the hook for this lifecycle.
    * ``value=False`` when the newest entry predates ``StartedAt`` (the
      lifecycle Supervisor scheduled did not run through us) or there
      is no entry at all but the recipe directory exists on disk.
    * ``None`` when the container is missing or its start time is
      unreadable — caller holds state.
    """
    started = await container_started_at(docker, container)
    if started is None:
        return None
    last = last_put_archive_ts(pre_start_log_path)
    if last is None:
        return HealthResult(False, "no successful put_archive in pre-start.log")
    if last + _APPLIED_SLACK_SECONDS < started:
        return HealthResult(
            False,
            f"last put_archive {started - last:.1f}s before container start",
        )
    return HealthResult(True, "put_archive newer than StartedAt")


async def _exec_output(
    docker: aiodocker.Docker,
    container: str,
    cmd: list[str],
) -> tuple[int, str]:
    """Run one short command in the target and return (rc, stdout+stderr).

    Bounded by ``_DOCKER_CHECK_TIMEOUT`` so a stuck target can't stall
    the whole health poll cycle. Timeout returns rc=127 with a "timeout"
    diagnostic, treated by callers as "check unavailable" (health
    sensor goes ``unknown``).
    """
    try:
        return await asyncio.wait_for(
            _exec_output_impl(docker, container, cmd),
            timeout=_DOCKER_CHECK_TIMEOUT,
        )
    except TimeoutError:
        return 127, f"exec timeout after {_DOCKER_CHECK_TIMEOUT:.0f}s"


async def _exec_output_impl(
    docker: aiodocker.Docker,
    container: str,
    cmd: list[str],
) -> tuple[int, str]:
    try:
        c = await docker.containers.get(container)
        # aiodocker's exec API: create + start with a fresh Stream.
        exe = await c.exec(cmd=cmd, stdout=True, stderr=True)
        async with exe.start(detach=False) as stream:
            chunks: list[bytes] = []
            while True:
                msg = await stream.read_out()
                if msg is None:
                    break
                chunks.append(msg.data)
        inspect = await exe.inspect()
        rc = int(inspect.get("ExitCode") or 0)
        return rc, b"".join(chunks).decode("utf-8", errors="replace").strip()
    except aiodocker.exceptions.DockerError as e:
        return 127, f"docker error: {e}"
    except Exception as e:  # noqa: BLE001 -- diagnostic passthrough
        return 127, f"exec error: {type(e).__name__}: {e}"


async def sentinel_is_tmpfs(
    docker: aiodocker.Docker,
    container: str,
    sentinel_path: str,
) -> bool:
    """Return True iff the sentinel path resolves onto a tmpfs mount.

    Uses ``findmnt -T <path> -no FSTYPE`` inside the target. tmpfs is
    remounted fresh on every container start, so its contents cannot
    survive a stop/start — presence alone becomes a freshness signal.
    Any non-tmpfs (or missing findmnt) falls back to mtime comparison
    upstream. Returns False on any error so the caller uses the safer
    mtime path by default.
    """
    rc, out = await _exec_output(
        docker, container, ["findmnt", "-T", sentinel_path, "-no", "FSTYPE"]
    )
    if rc != 0:
        return False
    return out.strip() == "tmpfs"


async def check_sentinel(
    docker: aiodocker.Docker,
    container: str,
    sentinel_path: str,
) -> HealthResult | None:
    """Verify the sentinel file confirms the payload ran on this lifecycle.

    Behavior:

    * tmpfs sentinel path: exists (rc=0 to ``test -e``) ⇒ True.
    * overlay sentinel path: mtime ≥ StartedAt - 1s ⇒ True (small
      slack absorbs clock skew between the addon and the container).
    * missing / stat failure: False with a diagnostic reason.
    * container missing or StartedAt unreadable: ``None``.
    """
    started = await container_started_at(docker, container)
    if started is None:
        return None
    quoted = shlex.quote(sentinel_path)
    if await sentinel_is_tmpfs(docker, container, sentinel_path):
        rc, _ = await _exec_output(docker, container, ["test", "-e", sentinel_path])
        if rc == 0:
            return HealthResult(True, f"tmpfs sentinel {sentinel_path} present")
        return HealthResult(False, f"tmpfs sentinel {sentinel_path} missing")
    # Non-tmpfs (overlay): check mtime.
    rc, out = await _exec_output(
        docker,
        container,
        ["sh", "-c", f"stat -c %Y {quoted} 2>/dev/null || echo MISSING"],
    )
    if rc != 0 or not out or out == "MISSING":
        return HealthResult(False, f"sentinel {sentinel_path} missing")
    try:
        mtime = float(out.strip())
    except ValueError:
        return HealthResult(False, f"sentinel {sentinel_path} unparsable mtime")
    if mtime + _SENTINEL_SLACK_SECONDS < started:
        return HealthResult(
            False,
            f"sentinel mtime {started - mtime:.1f}s before container start",
        )
    return HealthResult(True, f"sentinel {sentinel_path} fresh")


def render_binary_state(result: HealthResult | None) -> str:
    """Map a HealthResult to the payload we publish to the state topic."""
    if result is None:
        return "unknown"
    return "ON" if result.value else "OFF"


def render_reason(result: HealthResult | None) -> dict[str, Any]:
    """Attributes block published alongside a state — the "why" for the UI."""
    if result is None:
        return {"reason": "no data"}
    return {"reason": result.reason}
