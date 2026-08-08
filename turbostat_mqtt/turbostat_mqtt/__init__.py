# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Turbostat → MQTT add-on package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os
import time  # noqa: F401 — exposed so tests can patch via turbostat_mqtt.time

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: F401
from .config import Options, from_mapping, read  # noqa: F401
from .metadata import (  # noqa: F401
    COLUMNS,
    COUNT_COLS,
    DIAGNOSTIC_COLS,
    EXPECTED_COLS,
    ColMeta,
    friendly_name,
    guess_meta,
    missing_expected_columns,
)
from .mqtt import MqttHealth, build_discovery_payloads  # noqa: F401
from .parser import TurbostatParser, start_turbostat  # noqa: F401
from .publisher import Fault, Publisher, coerce, map_columns  # noqa: F401
from .util import log, sanitize_key  # noqa: F401
