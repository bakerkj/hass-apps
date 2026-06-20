# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Container Hooks package."""

import os

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .docker import (  # noqa: E402,F401 — tests reference these as ``rocs.<name>``.
    HookExecution,
    apply_patch,
    docker_events,
    docker_ps_running,
    docker_url,
    put_archive_dir,
    run_hook,
    run_pre_start_hook,
    self_container_name,
)
