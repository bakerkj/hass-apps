# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""HALPI2 Power add-on package.

Publishes power controller telemetry read from halpid's UNIX-socket API as
Home Assistant MQTT Discovery entities. Nothing here touches I2C directly --
halpid owns the bus.
"""

import os

__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import main  # noqa: F401
from .config import (  # noqa: F401
    OPTION_KEYS,
    SENSITIVE_OPTION_KEYS,
    load_options_file,
    redact_options_for_log,
    resolve_mqtt_config,
)
from .halpid_client import (  # noqa: F401
    HalpidError,
    UnixSocketHTTPConnection,
    get_values,
    get_version,
)
from .mqtt import (  # noqa: F401
    MqttHealth,
    availability_topic,
    clear_discovery,
    publish_all_discovery,
    publish_discovery,
    state_topic,
)
from .sensors import (  # noqa: F401
    DEVICE_INFO_KEYS,
    POWER_STATE_DEF,
    POWER_STATE_KEY,
    SENSOR_DEFS,
)
