# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Docker integration: events + hooks via aiodocker (async).

The events stream uses ``aiodocker``; post-start hooks ship the script
into the target with ``put_archive`` and run it via ``container.exec``.
Pre-start scripts run in the addon container via
``asyncio.create_subprocess_exec`` so user bash can call the ``docker``
CLI directly against the target before its entrypoint starts.
"""

import asyncio
import datetime
import io
import logging
import os
import re
import tarfile
import tempfile
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiodocker
import patch_ng
from aiodocker.exceptions import DockerError

# Backoff between reconnect attempts when the events stream exits —
# keeps the loop from spinning at full speed when the daemon socket is
# missing or unreachable. Doubles on consecutive failures up to the cap.
_RECONNECT_BACKOFF_INITIAL_SECONDS = 1.0
_RECONNECT_BACKOFF_MAX_SECONDS = 30.0


@dataclass(frozen=True)
class HookExecution:
    container: str
    # The thing that was executed/applied: a hook script, a pre-start
    # file tree's root, or a patch file. Different stages set different
    # kinds of paths here, so the field is named generically.
    source: Path
    log_path: Path
    returncode: int
    duration_ms: int


def _sync_append(log_path: Path, text: str) -> None:
    """Plain ``open(..., 'a').write``; runs on a thread, see ``_async_append``."""
    with log_path.open("a") as fh:
        fh.write(text)


async def _async_append(log_path: Path, text: str) -> None:
    """Append ``text`` to ``log_path`` without blocking the event loop."""
    await asyncio.to_thread(_sync_append, log_path, text)


# ``/proc/self/mountinfo`` carries docker's bind mounts for
# ``/etc/hostname``, ``/etc/hosts``, ``/etc/resolv.conf``, each with a
# source path of the shape ``.../containers/<64-hex-id>/...``. That ID
# survives every layout that matters here: cgroup v1, cgroup v2,
# cgroup-namespaced (HAOS and GHA's ubuntu-24.04 runner), systemd or
# cgroupfs driver — verified inline on both. Anchoring on
# ``/containers/<id>/`` is what skips the overlay2 ``lowerdir`` /
# ``upperdir`` layer IDs that appear in the file's first (root) line.
_CONTAINER_ID_RE = re.compile(r"/containers/([0-9a-f]{64})/")


def _own_container_id() -> str:
    """Scrape our container ID out of ``/proc/self/mountinfo``.

    Returns ``""`` if the file can't be read or no docker bind-mount
    line is present; the caller surfaces that via the self-skip warning.
    """
    try:
        text = Path("/proc/self/mountinfo").read_text()
    except OSError:
        return ""
    m = _CONTAINER_ID_RE.search(text)
    return m.group(1) if m else ""


async def self_container_name(docker: aiodocker.Docker) -> str:
    """Resolve our own full docker name (e.g. ``addon_<slug>_container_hooks``).

    Pulls the container ID from ``/proc/self/mountinfo`` (see
    ``_own_container_id``), then asks the docker API for the canonical
    Name — that's what the events stream and ``docker_ps_running``
    report, so it's what ``skip_containers`` has to match.

    Returns ``""`` on any failure (no ID in mountinfo, docker lookup
    fails). The caller is expected to warn the operator when this
    returns empty; falling back to a guess would silently add a
    never-matching value to ``skip_containers``, defeating self-skip
    during ``initial_sweep``.
    """
    container_id = _own_container_id()
    if not container_id:
        return ""
    try:
        info = await (await docker.containers.get(container_id)).show()
        return str(info.get("Name", "")).lstrip("/")
    except DockerError, KeyError, AttributeError:
        return ""


def docker_url() -> str:
    """Return the unix-socket URL for the docker daemon.

    HA addons see the daemon socket at ``/run/docker.sock`` (the bind
    mount Supervisor sets up when ``docker_api: true`` and protection
    mode is off). Falls back to ``/var/run/docker.sock`` in case the
    base image symlinks the other way.
    """
    for candidate in ("/run/docker.sock", "/var/run/docker.sock"):
        if Path(candidate).exists():
            return f"unix://{candidate}"
    return "unix:///run/docker.sock"


async def docker_events(
    docker: aiodocker.Docker,
    log: logging.Logger,
    events: tuple[str, ...] = ("start",),
) -> AsyncIterator[dict]:
    """Async iterator yielding matching container lifecycle events.

    Filters by Action in Python because docker treats multiple
    ``event=`` filter values as a logical AND, not OR. Auto-reconnects
    with exponential backoff on socket / daemon hiccups. The caller is
    expected to handle its own SIGTERM and stop awaiting.
    """
    wanted = set(events)
    backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
    while True:
        saw_event = False
        subscriber = None
        try:
            subscriber = docker.events.subscribe()
            while True:
                event = await subscriber.get()
                if event is None:
                    break
                if event.get("Type") != "container":
                    continue
                saw_event = True
                # Every container event is logged at DEBUG so an operator
                # who wants to see what the daemon is emitting (including
                # the ~10/s of healthcheck exec_* lines across all addons)
                # can flip ``log_level: DEBUG``. The default INFO keeps
                # this quiet and surfaces only the dispatch log lines below.
                action = event.get("Action")
                log.debug(
                    "rx event Action=%s name=%s",
                    action,
                    event.get("Actor", {}).get("Attributes", {}).get("name", ""),
                )
                if action in wanted:
                    yield event
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — aiohttp.ClientError et al. must also retry
            log.warning(
                "docker events stream error: %s; reconnecting in %.1fs",
                e,
                backoff,
            )
        else:
            log.warning("docker events stream ended; reconnecting in %.1fs", backoff)
        # Drop the subscriber before the backoff sleep so aiodocker's
        # ``ChannelSubscriber.__del__`` removes its queue from
        # ``channel.queues`` right now. The reassignment on the next
        # iteration would trigger the same refcount-GC in CPython, but
        # doing it here means no daemon events accumulate in the
        # abandoned queue during the (potentially 30-second) sleep.
        # aiodocker 0.27.0 has no ``unsubscribe()`` / ``__aexit__``
        # surface on the subscriber, so ``del`` is the documented exit.
        if subscriber is not None:
            del subscriber
        if saw_event:
            backoff = _RECONNECT_BACKOFF_INITIAL_SECONDS
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SECONDS)


async def docker_ps_running(
    docker: aiodocker.Docker,
    log: logging.Logger | None = None,
) -> list[str]:
    """Names of currently-running containers, read from the ``list()`` response.

    Prefers each container's canonical name (the single ``/<name>`` entry)
    over its link aliases (``/other/<name>`` entries) — docker returns
    both in ``Names`` and ``raw[0]`` is not guaranteed to be the canonical.
    """
    try:
        containers = await docker.containers.list()
    except DockerError as e:
        if log is not None:
            log.warning(
                "docker_ps_running: list() failed (%s); initial sweep will be empty",
                e,
            )
        return []
    names: list[str] = []
    for c in containers:
        try:
            raw = c["Names"]
        except KeyError, TypeError:
            continue
        if not raw:
            continue
        canonical = next(
            (str(n) for n in raw if isinstance(n, str) and n.count("/") == 1),
            str(raw[0]),
        )
        name = canonical.lstrip("/")
        if name:
            names.append(name)
    return names


def _build_single_file_tar(local: Path, arcname: str) -> bytes:
    """Tar bytes carrying ``local`` as ``arcname`` at the archive root."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        tf.add(str(local), arcname=arcname)
    return buf.getvalue()


def _build_dir_tree_tar(src_dir: Path) -> bytes:
    """Tar bytes for everything under ``src_dir``, paths relative to it.

    ``src_dir/etc/cont-init.d/foo`` ends up at ``etc/cont-init.d/foo``
    in the archive, so extracting at the container's root lands the
    file at ``/etc/cont-init.d/foo``.

    Uses ``tarfile.add``'s default ``dereference=False`` — a symlink in
    ``src_dir`` is shipped to the target as a symlink, not as the file
    it points to. This is deliberate so an operator can stage a symlink
    farm (e.g. ``/etc/cont-init.d/00-init`` pointing at a shared script)
    without flattening it on the way in.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for child in sorted(src_dir.iterdir()):
            tf.add(str(child), arcname=child.name, recursive=True)
    return buf.getvalue()


def _strip_diff_prefix(raw: bytes | None) -> str:
    """Strip ``a/`` / ``b/`` from a patch_ng source/target path.

    Rejects ``..`` components so a hand-edited (or typo'd) patch can't
    cause ``apply_patch`` to read or write outside its working tempdir
    when paths are joined to it.
    """
    if raw is None:
        return ""
    s = raw.decode("utf-8", errors="replace").split("\t", 1)[0].strip()
    if s.startswith(("a/", "b/")):
        s = s[2:]
    if ".." in s.split("/"):
        raise ValueError(f"patch path escapes its root: {s!r}")
    return s


def _categorize_patch_items(
    patch_set: Any,
) -> tuple[list[str], list[str], list[str]]:
    """Bucket each patch item into fetch / putback / deletion lists.

    Returns ``(fetch_paths, putback_targets, deletion_sources)``:

    * ``fetch_paths`` — absolute paths to ``get_archive`` from the
      target before applying (modify + delete sources).
    * ``putback_targets`` — relative paths to ``put_archive`` after
      apply (modify + create targets).
    * ``deletion_sources`` — relative paths the patch attempts to
      delete; recorded for a warning since docker ``put_archive``
      cannot remove files.
    """
    fetch_paths: list[str] = []
    putback_targets: list[str] = []
    deletion_sources: list[str] = []

    for item in patch_set.items:
        source_raw = item.source
        target_raw = item.target
        is_create = source_raw == b"/dev/null"
        is_delete = target_raw == b"/dev/null"
        src_rel = _strip_diff_prefix(source_raw)
        tgt_rel = _strip_diff_prefix(target_raw)

        if is_create:
            if not tgt_rel:
                raise ValueError(
                    "patch creates a file with no target path "
                    f"(source={source_raw!r}, target={target_raw!r})"
                )
            putback_targets.append(tgt_rel.lstrip("/"))
        elif is_delete:
            if src_rel:
                fetch_paths.append("/" + src_rel.lstrip("/"))
                deletion_sources.append(src_rel.lstrip("/"))
        else:
            if src_rel:
                fetch_paths.append("/" + src_rel.lstrip("/"))
            if tgt_rel:
                putback_targets.append(tgt_rel.lstrip("/"))

    return fetch_paths, putback_targets, deletion_sources


async def _container_get_file(
    ctr: aiodocker.containers.DockerContainer,
    path_in_container: str,
) -> tuple[bytes, int]:
    """Pull a single file's content + mode out of the target container.

    Returns ``(content_bytes, mode)``. The mode is preserved so a
    subsequent ``put_archive`` round-trip can land the file with its
    original permissions — losing the executable bit on a script in
    ``/etc/cont-init.d/`` would silently make s6 skip it.
    """
    tf = await ctr.get_archive(path_in_container)
    try:
        files = [m for m in tf.getmembers() if m.isfile()]
        if not files:
            raise FileNotFoundError(f"no file in archive for {path_in_container}")
        # docker's get_archive of a single file usually returns a one-entry
        # archive, but get_archive of a directory (or a symlink that
        # resolves to one) can bring siblings along. Prefer the entry
        # whose basename matches the requested path; fall back to the
        # first regular file only if nothing matches.
        wanted = os.path.basename(path_in_container.rstrip("/"))
        member = next(
            (m for m in files if os.path.basename(m.name) == wanted),
            files[0],
        )
        extracted = tf.extractfile(member)
        if extracted is None:
            raise FileNotFoundError(f"cannot extract file for {path_in_container}")
        return extracted.read(), member.mode
    finally:
        tf.close()


async def run_hook(
    docker: aiodocker.Docker,
    container: str,
    script: Path,
    log_path: Path,
    log: logging.Logger,
    env: dict[str, str] | None = None,
) -> HookExecution:
    """Put ``script`` into ``container``:/tmp and exec it via aiodocker.

    The script is shipped as a single-file tar via ``put_archive`` and
    executed with ``env`` exported. Output (stdout + stderr) is appended
    to ``log_path``.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    # Include the script stem so two concurrent ``_dispatch`` tasks for
    # the same container (debounce=0, or a slow hook outlasting the
    # debounce window) can't stomp on each other's remote copy in
    # ``/tmp``. ``rocs.`` prefix matches the existing tempdir naming.
    remote_name = f"rocs.{container}.{script.stem}.sh"
    remote_path = f"/tmp/{remote_name}"
    env = env or {}

    started = time.monotonic()
    rc = 0
    output_text = ""
    # ``out_chunks`` lives at function scope so a DockerError raised
    # mid-stream in the read loop below still gets its partial output
    # decoded and logged — that partial output is usually the most
    # useful diagnostic an operator has.
    out_chunks: list[bytes] = []
    try:
        ctr = await docker.containers.get(container)
        await ctr.put_archive(
            path="/tmp",
            data=_build_single_file_tar(script, remote_name),
        )
        # chmod via exec — put_archive preserves mode but a 644 source
        # would land non-exec; force +x explicitly.
        chmod = await ctr.exec(["chmod", "755", remote_path])
        chmod_chunks: list[bytes] = []
        async with chmod.start(detach=False) as stream:
            while True:
                msg = await stream.read_out()
                if msg is None:
                    break
                if isinstance(msg.data, bytes):
                    chmod_chunks.append(msg.data)
        # Non-zero chmod exit (e.g. read-only fs) means the script exec
        # below would fail with "permission denied" anyway; surface the
        # real reason and bail.
        chmod_info = await chmod.inspect()
        chmod_rc = int(chmod_info.get("ExitCode") or 0)
        if chmod_rc != 0:
            chmod_stderr = (
                b"".join(chmod_chunks).decode("utf-8", errors="replace").strip()
            )
            output_text += (
                f"[container_hooks] chmod 755 {remote_path} exited "
                f"{chmod_rc}; not running script"
                + (f": {chmod_stderr}" if chmod_stderr else "")
                + "\n"
            )
            rc = chmod_rc
            await _async_append(log_path, output_text)
            duration_ms = int((time.monotonic() - started) * 1000)
            return HookExecution(
                container=container,
                source=script,
                log_path=log_path,
                returncode=rc,
                duration_ms=duration_ms,
            )
        # aiodocker's ``Stream.read_out()`` returns demultiplexed Messages
        # from BOTH stdout (Message.stream=1) and stderr (Message.stream=2)
        # — the parser feeds both into the same queue (despite the method
        # name). So a single read loop captures stderr too; we keep the
        # bytes interleaved and don't try to tag them, which would split
        # across docker's chunk boundaries.
        exec_inst = await ctr.exec(
            [remote_path],
            environment=[f"{k}={v}" for k, v in env.items()],
        )
        async with exec_inst.start(detach=False) as stream:
            while True:
                msg = await stream.read_out()
                if msg is None:
                    break
                if isinstance(msg.data, bytes):
                    out_chunks.append(msg.data)
        output_text = b"".join(out_chunks).decode("utf-8", errors="replace")
        info = await exec_inst.inspect()
        rc = int(info.get("ExitCode") or 0)
    except DockerError as e:
        output_text = b"".join(out_chunks).decode("utf-8", errors="replace")
        output_text += f"[container_hooks] docker error: {e}\n"
        rc = -1

    await _async_append(log_path, output_text)

    duration_ms = int((time.monotonic() - started) * 1000)
    return HookExecution(
        container=container,
        source=script,
        log_path=log_path,
        returncode=rc,
        duration_ms=duration_ms,
    )


async def run_pre_start_hook(
    container: str,
    script: Path,
    log_path: Path,
    log: logging.Logger,
    env: dict[str, str] | None = None,
) -> HookExecution:
    """Run ``script`` in the addon container for a ``create`` event.

    The target hasn't started yet, so we can't ``docker exec`` against
    it; the pre-start script runs locally with the addon's docker CLI
    available. Output → ``log_path``.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = env or {}
    merged_env = {**os.environ, **env}

    started = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_exec(
            str(script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=merged_env,
        )
        stdout, _ = await proc.communicate()
        rc = proc.returncode if proc.returncode is not None else -1
        await _async_append(log_path, stdout.decode("utf-8", errors="replace"))
    except OSError as e:
        # Most common: script not executable (forgot ``chmod +x``) or
        # not found. Surface the script name and the OS error in the
        # per-container log so the operator doesn't get only a generic
        # "DISPATCH FAILED" from the parent dispatcher.
        await _async_append(
            log_path,
            f"[container_hooks] could not exec {script.name}: {type(e).__name__}: {e}\n",
        )
        rc = -1

    duration_ms = int((time.monotonic() - started) * 1000)
    return HookExecution(
        container=container,
        source=script,
        log_path=log_path,
        returncode=rc,
        duration_ms=duration_ms,
    )


async def put_archive_dir(
    docker: aiodocker.Docker,
    container: str,
    src_dir: Path,
    log_path: Path,
    log: logging.Logger,
) -> HookExecution:
    """Tar ``src_dir``'s tree and put_archive it into ``container``:/.

    Treats the tree rooted at ``src_dir`` as if it were the root of the
    target container — so ``src_dir/etc/cont-init.d/foo`` lands at
    ``container:/etc/cont-init.d/foo``. This is the fast path: one
    HTTP PUT against the docker socket vs the ~100 ms of bash + docker
    CLI overhead a script-based hook would pay.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    rc = 0
    payload_size = 0
    file_count = 0
    error: str | None = None
    try:
        files = [p for p in src_dir.rglob("*") if p.is_file()]
        file_count = len(files)
        # Bytes of actual content, not len(tar). A tar stream is padded to a
        # multiple of 10240 and Python's default PAX format adds an extended
        # header per member, so the stream length is roughly a record count:
        # one small file and nine members both logged "20480 bytes".
        #
        # lstat, not stat: _build_dir_tree_tar ships symlinks as symlinks
        # (dereference=False), so a link's tar member carries no content.
        # Following it here would bill the target's size against a hook that
        # never sends those bytes -- the same lie in the other direction.
        payload_size = sum(p.lstat().st_size for p in files)
        archive = _build_dir_tree_tar(src_dir)
        ctr = await docker.containers.get(container)
        await ctr.put_archive(path="/", data=archive)
    except DockerError as e:
        error = f"docker error: {e}"
        rc = -1
    except OSError as e:
        error = f"OS error: {e}"
        rc = -1

    duration_ms = int((time.monotonic() - started) * 1000)
    ts = datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")
    if error is None:
        line = (
            f"[{ts}] put_archive ok: {file_count} files, "
            f"{payload_size} bytes → {container} in {duration_ms}ms\n"
        )
    else:
        line = f"[{ts}] put_archive FAILED ({error}) after {duration_ms}ms\n"

    await _async_append(log_path, line)

    return HookExecution(
        container=container,
        source=src_dir,
        log_path=log_path,
        returncode=rc,
        duration_ms=duration_ms,
    )


async def apply_patch(
    docker: aiodocker.Docker,
    container: str,
    patch_file: Path,
    log_path: Path,
    log: logging.Logger,
) -> HookExecution:
    """Apply a unified diff against files inside ``container``.

    Steps, all async:
    1. Parse ``patch_file`` to learn which files inside the container
       it touches.
    2. ``get_archive`` each affected file from the target.
    3. Lay the files out at their relative paths inside a tmpdir,
       apply the patch in memory via ``patch_ng``.
    4. Tar the patched files up and ``put_archive`` them back into
       the target's ``/``.

    The whole operation runs in one event-loop pass, with no
    subprocess on the hot path.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    rc = 0
    error: str | None = None
    patched_count = 0
    payload_size = 0

    try:
        patch_bytes = patch_file.read_bytes()
        patch_set = patch_ng.fromstring(patch_bytes)
        if not patch_set:
            raise RuntimeError("patch_ng failed to parse the patch")

        fetch_paths, putback_targets, deletion_sources = _categorize_patch_items(
            patch_set
        )
        if not fetch_paths and not putback_targets:
            raise RuntimeError("patch references no destination paths")
        if deletion_sources:
            # docker put_archive can write or overwrite but cannot
            # remove; surface the limitation rather than silently
            # leaving the files in place.
            log.warning(
                "apply_patch: %s attempts to delete %d file(s); docker "
                "put_archive cannot remove files, leaving in place: %s. "
                "Use a post-start script with `rm` if you need to delete.",
                patch_file.name,
                len(deletion_sources),
                deletion_sources,
            )

        ctr = await docker.containers.get(container)

        with tempfile.TemporaryDirectory(prefix="rocs-patch-") as td_str:
            td = Path(td_str)
            modes: dict[str, int] = {}

            for path_in_container in fetch_paths:
                rel = path_in_container.lstrip("/")
                dest = td / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                content, mode = await _container_get_file(ctr, path_in_container)
                dest.write_bytes(content)
                # Preserve the source mode so the put_archive round-trip
                # doesn't strip the executable bit off scripts.
                dest.chmod(mode)
                modes[rel] = mode

            # patch_ng.PatchSet.apply runs synchronously and reads / writes
            # against the filesystem. Push it to a thread so the event
            # loop stays responsive. ``strip=0`` because patch_ng's
            # ``findfiles`` already strips the git-style ``a/`` / ``b/``
            # prefix; ``strip=1`` would drop an additional component and
            # land creation-targets at the wrong path.
            applied = await asyncio.get_running_loop().run_in_executor(
                None, lambda: patch_set.apply(strip=0, root=str(td))
            )
            if not applied:
                raise RuntimeError("patch_ng.apply reported failure")

            # Re-apply the original mode: patch_ng may write-and-rename
            # which loses the executable bit even though we chmod'd before.
            for rel, mode in modes.items():
                local = td / rel
                if local.is_file():
                    local.chmod(mode)

            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tf:
                for rel in putback_targets:
                    local_src = td / rel
                    if local_src.is_file():
                        tf.add(str(local_src), arcname=rel)
                        patched_count += 1
                        # Content bytes, not len(tar) — see put_archive_dir.
                        payload_size += local_src.stat().st_size

            if patched_count:
                await ctr.put_archive(path="/", data=buf.getvalue())
    except Exception as e:  # noqa: BLE001 — best-effort hook
        error = str(e)
        rc = -1

    duration_ms = int((time.monotonic() - started) * 1000)
    ts = datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")
    if error is None:
        line = (
            f"[{ts}] apply_patch ok: {patched_count} files, "
            f"{payload_size} bytes → {container} in {duration_ms}ms "
            f"(patch={patch_file.name})\n"
        )
    else:
        line = (
            f"[{ts}] apply_patch FAILED ({error}) after {duration_ms}ms "
            f"(patch={patch_file.name})\n"
        )

    await _async_append(log_path, line)

    return HookExecution(
        container=container,
        source=patch_file,
        log_path=log_path,
        returncode=rc,
        duration_ms=duration_ms,
    )
