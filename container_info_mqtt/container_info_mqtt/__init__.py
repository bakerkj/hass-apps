# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Container Info → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.  ``time`` and ``subprocess`` are re-exported so tests
that patch ``container_info_mqtt.subprocess.run`` etc. keep working.
"""

import os
import subprocess  # noqa: F401 — exposed for tests
import time  # noqa: F401 — exposed for tests

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: E402,F401
from .config import OPTION_KEYS, load_options_file  # noqa: E402,F401
from .docker import (  # noqa: E402,F401
    UnixSocketHTTPConnection,
    _fetch_stats_by_id_async,
    _fetch_stats_for_container,
    docker_api_get_json,
    fetch_containers,
    fetch_inspect_by_id,
    fetch_ps_containers,
    fetch_stats_by_id,
    run_cmd,
)
from .metrics import (  # noqa: E402,F401
    METRIC_DEFS,
    RATE_METRICS,
    RATE_SOURCE_METRICS,
    compute_rate_metrics,
    cpu_percent_from_stats,
    metric_value,
    parse_docker_timestamp,
    parse_include_metrics,
    sum_blkio_totals,
    sum_network_totals,
)
from .mqtt import (  # noqa: E402,F401
    MqttHealth,
    clear_discovery,
    prune_stale_discovery,
    publish_discovery,
    publish_summary_discovery,
)
from .util import (  # noqa: E402,F401
    DOCKER_SOCKET_PATH,
    SENSITIVE_OPTION_KEYS,
    cmd_error,
    container_display_name,
    deep_get,
    safe_float,
    safe_int,
    safe_text,
    slugify,
)

# redact_options_for_log lives in config
from .config import redact_options_for_log  # noqa: E402,F401
