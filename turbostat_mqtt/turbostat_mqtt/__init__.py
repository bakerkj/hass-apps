# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Turbostat → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os
import subprocess  # noqa: F401 — exposed so tests can patch via turbostat_mqtt.subprocess
import time  # noqa: F401 — exposed so tests can patch via turbostat_mqtt.time

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: E402,F401
from .mqtt import (  # noqa: E402,F401
    MqttHealth,
    build_discovery_payloads,
    connect_mqtt_with_retry,
    mqtt_publish,
)
from .metadata import friendly_name, guess_meta  # noqa: E402,F401
from .parser import TurbostatParser, start_turbostat  # noqa: E402,F401
from .util import log, sanitize_key  # noqa: E402,F401
