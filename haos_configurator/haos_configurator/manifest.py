# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Read, validate, and hash manifest entries.

The manifest and source files live at ``/config/`` inside the
container, which Supervisor bind-mounts from
``/addon_configs/<repo_id>_haos_configurator/`` on the host (for
github.com/bakerkj/hass-apps the repo_id is ``0f7b38ce``).  We never
write to ``/config`` — the map in :file:`config.json` is
``addon_config:ro``.
"""

import hashlib
import logging
import os
import sys
from typing import Any

import yaml

CONFIG_DIR = "/config"

log = logging.getLogger(__name__)


def load_manifest() -> dict[str, Any]:
    """Read and parse ``/config/manifest.yaml``."""
    path = f"{CONFIG_DIR}/manifest.yaml"
    if not os.path.isfile(path):
        log.error("No manifest at %s.", path)
        log.error("Drop a manifest.yaml (and any source files it references) into")
        log.error("the add-on's config directory")
        log.error("(/addon_configs/<repo_id>_haos_configurator/ on the host).")
        log.error("See examples/ in the source repo for a starting point:")
        log.error(
            "  https://github.com/bakerkj/hass-apps/tree/main/haos_configurator/examples"
        )
        sys.exit(1)
    try:
        with open(path, "rb") as f:
            manifest = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        log.error("Could not parse %s as YAML: %s", path, exc)
        sys.exit(1)
    if not isinstance(manifest, dict):
        log.error("Manifest at %s is not a YAML mapping.", path)
        sys.exit(1)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    """Check that every ``on_change`` name resolves to an action and
    that every ``dst`` is an absolute path."""
    actions = manifest.get("actions") or {}
    referenced = {
        name
        for entry in (manifest.get("files") or [])
        for name in (entry.get("on_change") or [])
    }
    missing = sorted(referenced - actions.keys())
    if missing:
        log.error("on_change references undefined action(s): %s", " ".join(missing))
        sys.exit(1)
    for entry in manifest.get("files") or []:
        dst = entry.get("dst", "")
        if not isinstance(dst, str) or not dst.startswith("/"):
            log.error("Manifest dst must be an absolute path: %r", dst)
            sys.exit(1)
    for name, action in actions.items():
        if not isinstance(action, dict):
            continue  # missing-run is warned at run time, not a manifest error
        cmd = action.get("run")
        if cmd is None:
            continue
        if isinstance(cmd, list):
            if not all(isinstance(x, str) for x in cmd):
                log.error("Action '%s' run list contains non-string elements", name)
                sys.exit(1)
        elif not isinstance(cmd, str):
            log.error("Action '%s' run must be a string or list of strings", name)
            sys.exit(1)


def default_mode_for(name: str) -> str:
    return "0755" if name.endswith(".sh") else "0644"


def local_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
