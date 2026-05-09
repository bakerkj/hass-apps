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

    https://github.com/adamoutler/HAOSConfigurator/tree/main/HassOsEnableSSH
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
        format="[%(levelname)s] %(message)s",
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


def process_files(manifest: dict[str, Any], *, dry_run: bool) -> set[str]:
    """Walk ``files[]``, install anything whose sha256 differs from the
    host's copy, and return the set of action names to fire."""
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
        if s_hash == d_hash:
            log.debug("unchanged: host:%s (sha256 match)", dst)
            continue

        install_file(src_local, dst, mode, dry_run=dry_run)
        any_changed = True
        fired.update(on_change)
    return fired if any_changed else set()


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
    log.debug("Options:\n%s", json.dumps(opts, indent=2, sort_keys=True))

    if host_run(["true"], check=False).returncode != 0:
        log.error(
            "Cannot enter host mount namespace.  Need full_access + "
            "host_pid + SYS_ADMIN, and Protection mode must be OFF."
        )
        return 1

    try:
        manifest = load_manifest()
        log.debug(
            "Manifest:\n%s",
            yaml.safe_dump(manifest, sort_keys=False).rstrip(),
        )
        if not manifest.get("files"):
            log.warning("Manifest declares no files; nothing to do.")
            return 0

        validate_manifest(manifest)
        fired = process_files(manifest, dry_run=dry_run)
        if not fired:
            log.info("All files already match host content; no actions to run.")
            return 0
        if not apply_post_actions:
            log.info(
                "Files changed, but apply_post_actions=false; skipping"
                " on_change actions."
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
