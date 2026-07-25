# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Signal K -> Home Assistant MQTT bridge."""

import os

__version__ = os.environ.get("ADDON_VERSION", "dev")

from . import auth  # noqa: F401
from .app import main, resolve_entities, slugify  # noqa: F401
from .client import (  # noqa: F401
    SignalKAuthError,
    SignalKError,
    get_self,
    get_server_info,
)
from .config import (  # noqa: F401
    OPTION_KEYS,
    load_options_file,
    redact_options_for_log,
    resolve_mqtt_config,
)
from .paths import PATH_MAP, flatten, match_path, resolve_group  # noqa: F401
