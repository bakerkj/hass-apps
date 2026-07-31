# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Direwolf IGate → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os
import time  # noqa: F401 — exposed so tests can patch via direwolf_igate.time

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main, tee_only  # noqa: F401
from .config import Options, enabled, from_mapping, read  # noqa: F401
from .mqtt import (  # noqa: F401
    BINARY_SENSORS,
    SENSORS,
    MqttHealth,
    SensorMeta,
    build_discovery_payloads,
    connect_mqtt_with_retry,
    heartbeat_payload,
    mqtt_publish,
)
from .parser import DirewolfParser, IGateStats  # noqa: F401
from .publisher import Publisher, overdue, state_values  # noqa: F401
from .rates import MAX_SAMPLES, RATE_WINDOW_S, RateSample, RateTracker  # noqa: F401
from .util import log  # noqa: F401
