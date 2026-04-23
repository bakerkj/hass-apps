# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""FFmpeg Snapshotter package.

Re-exports the symbols tests and external callers reference at the
package level.
"""

import os

# Version comes from the HA Supervisor at build time (ADDON_VERSION env).
__version__ = os.environ.get("ADDON_VERSION", "dev")

from .app import _compute_stream_offsets, main  # noqa: E402,F401
from .config import (  # noqa: E402,F401
    MqttConfig,
    MqttHealth,
    StreamCfg,
    load_mqtt_config,
)
from .mqtt import MqttPublisher, _slugify_camera  # noqa: E402,F401
from .retention import apply_retention_count, apply_retention_days  # noqa: E402,F401
from .stats import SnapshotStats, SnapshotView  # noqa: E402,F401
from .util import (  # noqa: E402,F401
    ensure_media_path,
    log,
    redact_url,
    set_latest_symlink,
)
from .worker import Worker  # noqa: E402,F401
