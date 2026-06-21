# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""System Resource Tuner package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os  # noqa: F401 — exposed so tests can patch via system_resource_tuner.os

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main, main_async  # noqa: E402,F401
from .config import (  # noqa: E402,F401
    ProcessTuning,
    Target,
    cpuset_matches,
    load_options,
    parse_bool,
    parse_cpuset_expression,
    parse_host_process_targets,
    parse_process_targets,
    parse_process_tuning,
    parse_targets,
)
from .docker import (  # noqa: E402,F401
    apply_all,
    apply_target,
    desired_update_kwargs,
    docker_inspect_limits,
    docker_top_processes,
    docker_url,
)
from .process import (  # noqa: E402,F401
    apply_process_cpuset,
    apply_process_nice,
    apply_process_tuning,
    apply_process_tunings,
    apply_tuning_to_pid,
    find_matching_host_pids,
    find_matching_pids,
    host_top_processes,
    list_process_threads,
    read_process_nice,
    read_task_cpuset,
)
