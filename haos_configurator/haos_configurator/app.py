# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Top-level orchestration: read options, parse manifest, install
changed files, fire on_change actions.

The manifest and source files are read locally from ``/config`` (plain
Python file IO + PyYAML + hashlib).  ``nsenter -t 1 -m -u -i`` is used
only for genuinely host-side ops:

* hashing existing destination files on the host,
* streaming source bytes into HAOS-persistent paths via ``tee``,
* running user-supplied ``on_change`` actions.

Modeled on adamoutler's HassOsEnableSSH for the host-namespace pattern:

    https://github.com/adamoutler/HassOSConfigurator/tree/main/HassOsEnableSSH
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from typing import Any

import yaml

from . import __version__
from .host import host_run, host_sha256, run_user_action
from .manifest import (
    CONFIG_DIR,
    default_mode_for,
    load_manifest,
    local_sha256,
    validate_manifest,
)

log = logging.getLogger(__name__)


def read_options() -> dict[str, Any]:
    try:
        with open("/data/options.json") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def configure_logging(level: str) -> None:
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    logging.basicConfig(
        level=levels.get(level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def install_file(src_local: str, dst: str, mode: str, *, dry_run: bool) -> None:
    """Atomically write ``src_local``'s bytes to ``dst`` on the host
    with permissions ``mode``.  Streams the source fd via stdin into
    ``tee`` host-side, so the install doesn't slurp the whole file into
    memory."""
    if dry_run:
        log.info(
            "[dry-run] would install %s -> host:%s (mode %s)",
            src_local,
            dst,
            mode,
        )
        return
    tmp = f"{dst}.tmp"
    host_run(["mkdir", "-p", os.path.dirname(dst)])
    with open(src_local, "rb") as f:
        host_run(["tee", tmp], stdin=f, stdout=subprocess.DEVNULL)
    host_run(["chmod", mode, tmp])
    host_run(["mv", tmp, dst])
    log.info("Installed host:%s (mode %s)", dst, mode)


def process_files(manifest: dict[str, Any], *, dry_run: bool) -> tuple[bool, set[str]]:
    """Walk ``files[]``, install anything whose sha256 differs from the
    host's copy.  Returns ``(any_changed, fired_actions)`` so callers
    can tell apart "nothing to do" from "files written but the entries
    declared no on_change actions"."""
    fired: set[str] = set()
    any_changed = False
    for entry in manifest.get("files") or []:
        src = entry["src"]
        dst = entry["dst"]
        mode = entry.get("mode") or default_mode_for(src)
        on_change = entry.get("on_change") or []

        src_local = f"{CONFIG_DIR}/{src}"
        if not os.path.isfile(src_local):
            log.error("Manifest references missing source file: %s", src)
            sys.exit(1)

        s_hash = local_sha256(src_local)
        d_hash = host_sha256(dst)
        log.debug("src=%s hash=%s", src_local, s_hash)
        log.debug("dst=%s hash=%s", dst, d_hash)
        if s_hash == d_hash:
            log.info("unchanged: host:%s (sha256 match)", dst)
            continue

        install_file(src_local, dst, mode, dry_run=dry_run)
        any_changed = True
        fired.update(on_change)
    return any_changed, fired


def run_actions(manifest: dict[str, Any], fired: set[str], *, dry_run: bool) -> None:
    """Walk ``actions`` in declaration order; fire each action whose
    name appears in ``fired`` exactly once."""
    for name, action in (manifest.get("actions") or {}).items():
        if name not in fired:
            continue
        cmd = (action or {}).get("run") or ""
        if not cmd:
            log.warning("Action '%s' has no 'run' command; skipping.", name)
            continue
        if dry_run:
            log.info("[dry-run] would run action '%s': %s", name, cmd)
            continue
        log.info("Action '%s': %s", name, cmd)
        rc = run_user_action(cmd)
        if rc != 0:
            log.warning("Action '%s' returned non-zero (%d).", name, rc)


def main() -> int:
    opts = read_options()
    log_level = str(opts.get("log_level") or "INFO")
    dry_run = bool(opts.get("dry_run", False))
    apply_post_actions = bool(opts.get("apply_post_actions", True))

    configure_logging(log_level)
    log.info("HAOS Configurator v%s starting", __version__)
    # Dump Options as INFO so each line gets the timestamp+level prefix.
    # logging treats the message as one record, so embedded "\n" produce
    # bare-content lines without the formatter prefix — which is harder
    # to grep / read.  Splitting into per-line calls fixes that.
    log.info("Options:")
    for line in json.dumps(opts, indent=2, sort_keys=True).splitlines():
        log.info("  %s", line)

    # Surface security-context info at DEBUG so failures of nsenter-based
    # host operations are diagnosable from the log alone.
    try:
        with open("/proc/self/attr/current") as f:
            log.debug("AppArmor label: %s", f.read().strip())
    except OSError as exc:
        log.debug("AppArmor: %s", exc)
    log.debug("os.getpid()=%d", os.getpid())
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(
                    ("Uid", "CapEff", "CapBnd", "Seccomp", "NoNewPrivs")
                ):
                    log.debug("/proc/self/status: %s", line.strip())
    except OSError as exc:
        log.debug("/proc/self/status: %s", exc)

    # Detect HA's Protection mode silently downgrading ``host_pid: true``.
    #
    # When Protection mode is ON in the add-on's Info tab, Supervisor
    # ignores our ``host_pid: true`` and starts the container with its
    # own PID namespace — so our process is PID 1 inside that ns.
    # ``nsenter -t 1`` then enters our *own* (container) namespace, not
    # the host's, and every read of a host path silently fails or
    # returns the wrong content.  The previous ``nsenter true`` probe
    # passed through this case happily, so the add-on would run end-to
    # -end and falsely report every host file as "needs install".
    #
    # When Protection mode is OFF, Supervisor honors ``host_pid`` and
    # the container shares the host's PID namespace — our process gets
    # a real host PID (not 1), and ``nsenter -t 1`` actually targets
    # the host's init.
    #
    # ``os.getpid() == 1`` is the cleanest distinguisher: True means we
    # have our own PID namespace; False means we share the host's.
    if os.getpid() == 1:
        log.error(
            "This add-on is PID 1 inside its container — host_pid: true"
            " was not honored, so nsenter cannot reach the host."
        )
        log.error("")
        log.error(
            "Almost always this means **Home Assistant's Protection"
            " mode is ON** for this add-on."
        )
        log.error("")
        log.error(
            "Fix: open the add-on's Info tab in the HA UI and toggle"
            " Protection mode OFF, then restart the add-on."
        )
        return 1

    try:
        manifest = load_manifest()
        log.info("Manifest:")
        for line in yaml.safe_dump(manifest, sort_keys=False).rstrip().splitlines():
            log.info("  %s", line)
        if not manifest.get("files"):
            log.warning("Manifest declares no files; nothing to do.")
            return 0

        validate_manifest(manifest)
        any_changed, fired = process_files(manifest, dry_run=dry_run)
        if not any_changed:
            log.info("All files already match host content; no actions to run.")
            return 0
        if not apply_post_actions:
            log.info(
                "Files %s, but apply_post_actions=false; skipping on_change actions.",
                "would change" if dry_run else "changed",
            )
            return 0
        if not fired:
            log.info(
                "Files %s, but no on_change actions are configured for"
                " those entries; nothing to fire.",
                "would change" if dry_run else "changed",
            )
            return 0

        run_actions(manifest, fired, dry_run=dry_run)
    except subprocess.CalledProcessError as exc:
        log.error("Host command failed: %s", " ".join(exc.cmd))
        if exc.stderr:
            log.error("stderr: %s", exc.stderr.decode(errors="replace").strip())
        return 1

    log.info("Done.  The add-on can be stopped now.")
    return 0
