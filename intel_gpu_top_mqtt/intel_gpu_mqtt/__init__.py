# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Intel GPU Top → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os
import subprocess  # noqa: F401 — exposed so tests can patch via intel_gpu_mqtt.subprocess
import time  # noqa: F401 — exposed so tests can patch via intel_gpu_mqtt.time

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: E402,F401
from .device import (  # noqa: E402,F401
    auto_select_device_arg,
    list_intel_gpu_top_devices,
    start_intel_gpu_top,
)
from .metrics import build_metrics  # noqa: E402,F401
from .mqtt import MqttHealth, publish_discovery  # noqa: E402,F401
from .util import (  # noqa: E402,F401
    dig,
    extract_latest_json_object,
    find_engine_field,
    safe_float,
)
