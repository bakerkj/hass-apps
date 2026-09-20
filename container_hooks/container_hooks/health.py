# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Two per-lifecycle health checks. Both return ``None`` on unreadable state.

* ``check_applied`` — did container_hooks fire the pre-start hook for the
  current container lifecycle? Compares pre-start.log's newest put_archive
  entry to Container.Created (put_archive fires on the create event).
* ``check_sentinel`` — did the staged payload actually run inside the target
  on the current lifecycle? Checks a path the payload is expected to touch;
  tmpfs paths are freshness-by-existence, overlay paths compare mtime to
  StartedAt (sentinel is touched during target startup).
"""

import asyncio
import datetime
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiodocker

# Tail window when scanning ``pre-start.log`` for the newest put_archive line.
_LAST_LINE_SCAN_BYTES = 1_048_576
# Clock-skew slack when comparing put_archive log entry to Container.Created.
_APPLIED_SLACK_SECONDS = 30.0
_SENTINEL_SLACK_SECONDS = 30.0
# Ceiling on a single docker-API call so a hung daemon can't stall polling.
_DOCKER_CHECK_TIMEOUT = 10.0


@dataclass(frozen=True)
class HealthResult:
    """One check's outcome: bool + short "why" for the sensor's ``reason`` attr."""

    value: bool
    reason: str


async def _fetch_show(
    docker: aiodocker.Docker, container: str
) -> dict[str, Any] | None:
    """Bounded ``docker inspect``; ``None`` on missing/timeout/daemon error."""
    try:
        c = await docker.containers.get(container)
        return await asyncio.wait_for(c.show(), timeout=_DOCKER_CHECK_TIMEOUT)
    except TimeoutError:
        return None
    except aiodocker.exceptions.DockerError:
        return None
    except Exception:  # noqa: BLE001 -- daemon returned nonsense; bail
        return None


async def container_started_at(
    docker: aiodocker.Docker, container: str
) -> float | None:
    """``StartedAt`` (used by the sentinel check, which anchors on start time)."""
    info = await _fetch_show(docker, container)
    if info is None:
        return None
    raw = (info.get("State") or {}).get("StartedAt")
    if not raw:
        return None
    return _parse_docker_ts(raw)


async def container_created_at(
    docker: aiodocker.Docker, container: str
) -> float | None:
    """``Created`` (used by check_applied — put_archive fires on the create event,
    so this anchors freshness on container-instance identity, not start time)."""
    info = await _fetch_show(docker, container)
    if info is None:
        return None
    raw = info.get("Created")
    if not raw:
        return None
    return _parse_docker_ts(raw)


def _parse_docker_ts(raw: str) -> float | None:
    """Parse Docker's ISO-8601 timestamp (nanosecond precision, trailing Z) to a
    unix float. Returns None for unparsable input, the zero-time, or any
    timezone-naive value (comparisons against timezone-aware Created/StartedAt
    would silently become wall-clock-local otherwise)."""
    s = raw.strip()
    if not s or s.startswith("0001-"):
        return None
    # Truncate ns precision to μs since fromisoformat only handles μs.
    if "." in s:
        head, _, tail = s.partition(".")
        m = re.match(r"(\d+)(.*)$", tail)
        if m:
            frac, rest = m.group(1), m.group(2)
            frac = (frac + "000000")[:6]
            s = f"{head}.{frac}{rest}"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(s)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.timestamp()


_PUT_ARCHIVE_LOG_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T[^\]]+)\]\s+put_archive ok:"
)


def last_put_archive_ts(log_path: Path) -> float | None:
    """Newest ``put_archive ok`` timestamp in the log's tail
    (``_LAST_LINE_SCAN_BYTES``); None on missing/empty/no-match."""
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
    """Compare pre-start.log's newest entry to Container.Created (symmetric slack).

    Returns None on missing container. False on stale/missing log entry (log
    predates Created by more than slack, or log entry is > slack in the future
    — clock-skew corruption). True otherwise.
    """
    created = await container_created_at(docker, container)
    if created is None:
        return None
    last = last_put_archive_ts(pre_start_log_path)
    if last is None:
        return HealthResult(False, "no successful put_archive in pre-start.log")
    delta = last - created
    if abs(delta) > _APPLIED_SLACK_SECONDS:
        side = "before" if delta < 0 else "after"
        return HealthResult(
            False,
            f"last put_archive {abs(delta):.1f}s {side} container create",
        )
    return HealthResult(True, "put_archive within slack of Created")


async def _exec_output(
    docker: aiodocker.Docker,
    container: str,
    cmd: list[str],
) -> tuple[int, str]:
    """Run one short command in the target, bounded by _DOCKER_CHECK_TIMEOUT.

    Returns (rc, stdout+stderr); rc=127 for any timeout/docker/exec failure
    (callers treat non-zero as "check unavailable")."""

    async def _run() -> tuple[int, str]:
        c = await docker.containers.get(container)
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

    try:
        return await asyncio.wait_for(_run(), timeout=_DOCKER_CHECK_TIMEOUT)
    except TimeoutError:
        return 127, f"exec timeout after {_DOCKER_CHECK_TIMEOUT:.0f}s"
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

    tmpfs paths: presence alone ⇒ True (tmpfs remounts on every start).
    overlay paths: mtime within ``_SENTINEL_SLACK_SECONDS`` of StartedAt ⇒ True.
    ``None`` when the container is missing or StartedAt is unreadable.
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
