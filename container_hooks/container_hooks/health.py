# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Two per-lifecycle health checks. Both return ``None`` on unreadable state.

* ``check_applied`` — did container_hooks fire the pre-start hook for the
  current container lifecycle? Compares pre-start.log's newest put_archive
  entry to Container.Created (put_archive fires on the create event).
* ``check_sentinel`` — did the staged payload actually run inside the target
  on the current lifecycle? Checks a path the payload is expected to touch;
  tmpfs paths are freshness-by-existence, overlay paths compare mtime to
  StartedAt. When the sentinel body is a JSON object carrying identity
  fields (``boot_id`` and ``pid1_start_ticks_since_boot``), those are also
  compared against the live host boot and target pid1 start ticks — a
  stale sentinel from a prior container lifecycle or a prior host boot is
  caught even when its mtime looks fresh.
"""

import asyncio
import datetime
import json
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
    """Bounded ``docker inspect``; ``None`` on missing/timeout/daemon error.

    ``wait_for`` covers both ``containers.get`` and ``.show`` — both round-trip
    to the daemon, and a hung daemon can stall either half."""

    async def _run() -> dict[str, Any]:
        c = await docker.containers.get(container)
        return await c.show()

    try:
        return await asyncio.wait_for(_run(), timeout=_DOCKER_CHECK_TIMEOUT)
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


SENTINEL_MODE_PRESENCE = "presence"
SENTINEL_MODE_CONTENT = "content"


async def check_sentinel(
    docker: aiodocker.Docker,
    container: str,
    sentinel_path: str,
    *,
    mode: str = SENTINEL_MODE_PRESENCE,
) -> HealthResult | None:
    """Verify the sentinel file confirms the payload ran on this lifecycle.

    Two modes, opt-in per-container via ``success_sentinel_mode``:

    * ``presence`` (default): a payload that just ``touch``es a file
      is sufficient. tmpfs paths use presence-only (tmpfs remounts on
      every start); overlay paths compare mtime to StartedAt with
      ``_SENTINEL_SLACK_SECONDS`` slack.
    * ``content``: the payload writes a JSON body carrying identity
      fields (``boot_id``, ``pid1_start_ticks_since_boot``). A single
      bounded read of the sentinel yields both the presence signal
      (rc≠0 → missing) and the body. The body's identity fields are
      compared against the live host boot id and the target's pid1
      start ticks — those are strictly stronger than mtime, so no
      separate presence/mtime pass is done. Missing file, empty or
      non-JSON body, body without both fields, or a mismatch → OFF.

    Returns ``None`` when the container is missing or StartedAt is
    unreadable — an unreachable target lands on per-slug availability
    offline rather than a false "sentinel missing" OFF.
    """
    # ``container_started_at`` is load-bearing in both modes: it doubles
    # as the reachability probe. When it returns None the target is gone
    # or the daemon is hung, and the publisher flips per-slug
    # availability offline. Content mode discards the returned value
    # itself; that's the intended trade for the availability behavior.
    started = await container_started_at(docker, container)
    if started is None:
        return None
    # Fail-closed: only literal ``content`` triggers the identity path.
    # Any other value (default ``presence``, typo, unknown-future value
    # reaching a stale caller) uses presence/mtime.
    if mode == SENTINEL_MODE_CONTENT:
        return await _check_identity(docker, container, sentinel_path)
    return await _check_presence(docker, container, sentinel_path, started)


async def _check_presence(
    docker: aiodocker.Docker,
    container: str,
    sentinel_path: str,
    started: float,
) -> HealthResult:
    """The presence half of ``check_sentinel``: tmpfs → ``test -e``,
    overlay → mtime vs StartedAt."""
    if await sentinel_is_tmpfs(docker, container, sentinel_path):
        rc, _ = await _exec_output(docker, container, ["test", "-e", sentinel_path])
        if rc == 0:
            return HealthResult(True, f"tmpfs sentinel {sentinel_path} present")
        return HealthResult(False, f"tmpfs sentinel {sentinel_path} missing")
    quoted = shlex.quote(sentinel_path)
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


# Cap on how much of the sentinel body we ingest before parsing. A
# broken or hostile payload could otherwise write a multi-GB blob and
# OOM the poll cycle. 64 KiB is well above any legitimate identity
# payload (ours is < 1 KiB).
_SENTINEL_BODY_MAX_BYTES = 65536


async def _check_identity(
    docker: aiodocker.Docker,
    container: str,
    sentinel_path: str,
) -> HealthResult:
    """The content half of ``check_sentinel`` (``mode='content'``): read
    the sentinel body, require both ``boot_id`` and
    ``pid1_start_ticks_since_boot``, and match them against live values.
    Any deviation is OFF — the whole point of opting in to content mode
    is that the payload promised to write the identity fields."""
    # ``head -c`` bounds the read so a huge sentinel doesn't OOM the
    # poll cycle. If a legitimate payload exceeds the cap, that's
    # already non-conformant.
    quoted = shlex.quote(sentinel_path)
    rc, out = await _exec_output(
        docker,
        container,
        ["sh", "-c", f"head -c {_SENTINEL_BODY_MAX_BYTES} {quoted}"],
    )
    if rc != 0:
        return HealthResult(
            False, f"sentinel {sentinel_path} missing or unreadable (content mode)"
        )
    if not out:
        return HealthResult(
            False, f"sentinel {sentinel_path} body empty (content mode)"
        )
    try:
        body = json.loads(out)
    except ValueError:
        return HealthResult(
            False, f"sentinel {sentinel_path} body is not JSON (content mode)"
        )
    if not isinstance(body, dict):
        return HealthResult(
            False, f"sentinel {sentinel_path} body is not a JSON object (content mode)"
        )
    seen_boot = body.get("boot_id")
    seen_ticks = body.get("pid1_start_ticks_since_boot")
    if not isinstance(seen_boot, str) or not seen_boot:
        return HealthResult(
            False, f"sentinel {sentinel_path} missing boot_id field (content mode)"
        )
    # ``bool`` is a subclass of ``int`` in Python; reject it explicitly
    # so ``{"pid1_start_ticks_since_boot": true}`` doesn't slip through
    # the field-present gate and compare truthy against a real tick.
    if not isinstance(seen_ticks, int) or isinstance(seen_ticks, bool):
        return HealthResult(
            False,
            f"sentinel {sentinel_path} missing pid1_start_ticks_since_boot "
            f"(content mode)",
        )
    expected_boot = _host_boot_id()
    if expected_boot and expected_boot != seen_boot:
        return HealthResult(
            False,
            f"sentinel from prior host boot "
            f"(sentinel boot_id={seen_boot!r}, host={expected_boot!r})",
        )
    current_ticks = await _target_pid1_start_ticks(docker, container)
    if current_ticks is None:
        return HealthResult(
            False,
            f"sentinel {sentinel_path} identity check: target pid1 ticks unreadable",
        )
    if current_ticks != seen_ticks:
        return HealthResult(
            False,
            f"sentinel from prior container lifecycle "
            f"(sentinel pid1_ticks={seen_ticks}, current={current_ticks})",
        )
    return HealthResult(True, f"sentinel {sentinel_path} identity verified")


def _host_boot_id() -> str | None:
    """Read the kernel's boot id. Shared with every container on this
    host (same kernel), so we can compare directly against a value the
    payload captured from inside its own target container."""
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip() or None
    except OSError:
        return None


async def _target_pid1_start_ticks(
    docker: aiodocker.Docker, container: str
) -> int | None:
    """Field 22 of ``/proc/1/stat`` for the target container's pid 1
    (starttime in jiffies since host boot). Uniquely identifies this
    container lifecycle when combined with the host boot id.

    ``/proc/1/stat`` is kernel-bounded to a few hundred bytes, but we
    still cap the read to match the ceiling used for the sentinel body
    (defense in depth against a docker exec that misbehaves)."""
    rc, out = await _exec_output(
        docker, container, ["sh", "-c", "head -c 4096 /proc/1/stat"]
    )
    if rc != 0 or not out:
        return None
    # comm can contain spaces and parens; slice past the LAST ')' before
    # splitting so we land on stat field 3 (state) at tail idx 0.
    # Field 22 (starttime) is then tail idx 19.
    tail = out.rsplit(")", 1)[-1].split()
    try:
        return int(tail[19])
    except IndexError, ValueError:
        return None


def render_binary_state(result: HealthResult | None) -> str:
    """Map a HealthResult to the payload we publish to the state topic."""
    if result is None:
        return "unknown"
    return "ON" if result.value else "OFF"
