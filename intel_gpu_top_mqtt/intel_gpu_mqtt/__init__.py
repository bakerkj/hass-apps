# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Intel GPU Top → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os
import subprocess  # noqa: F401 — tests patch via intel_gpu_mqtt.subprocess
import time  # noqa: F401 — exposed so tests can patch via intel_gpu_mqtt.time

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: F401
from .config import Options, from_sources, parse_bool  # noqa: F401
from .device import (  # noqa: F401
    auto_select_device_arg,
    list_intel_gpu_top_devices,
    start_intel_gpu_top,
)
from .metrics import build_metrics  # noqa: F401
from .mqtt import MqttHealth, discovery_payloads  # noqa: F401
from .publisher import Fault, Publisher  # noqa: F401
from .util import (  # noqa: F401
    dig,
    extract_latest_json_object,
    find_engine_field,
    safe_float,
)
