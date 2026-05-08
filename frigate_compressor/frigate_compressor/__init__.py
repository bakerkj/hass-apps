# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Frigate Compressor package.

The package is split into focused submodules; this module re-exports the
symbols that tests and external callers reference at the package level.
"""

import os
import time  # noqa: F401  — exposed as ``frigate_compressor.time`` for tests

# Version is supplied at build time by the HA Supervisor (BUILD_VERSION build
# arg, sourced from config.json). config.json is the single source of truth.
__version__ = os.environ.get("ADDON_VERSION", "dev")

from .compressor import (  # noqa: E402,F401
    _compress_one_inner,
    compress_one,
)
from .config import (  # noqa: E402,F401
    CameraConfig,
    Config,
    MqttConfig,
    MqttHealth,
    TierConfig,
    TypeSettings,
    _fmt_type,
    _merge_defaults,
    load_config,
)
from .context import CompressorContext  # noqa: E402,F401
from .database import (  # noqa: E402,F401
    STATUS_ERROR,
    STATUS_DIRECT,
    STATUS_OK,
    STATUS_SEGMENT_UPDATE_FAILED,
    _attach_frigate,
    _backfill_files_stats,
    _record,
    _recording_type,
    check_frigate_schema,
    check_frigate_schema_attached,
    open_compress_db,
    verify_files_stats,
)
from .eligibility import (  # noqa: E402,F401
    _build_eligible_where,
    _ELIGIBLE_BATCH_SIZE,
    get_eligible_recordings,
    time_until_next_eligible,
)
from .ffmpeg import (  # noqa: E402,F401
    FFMPEG_STDERR_MAX_LEN,
    FFMPEG_TIMEOUT_SEC,
    _build_fps_filter,
    _build_scale_filter,
    _parse_fps,
    _probe,
    _probe_dims,
    _TEMP_GLOB,
    _TEMP_PREFIX,
    build_ffmpeg_cmd,
    check_encoder_works,
    detect_encoder,
)
from .housekeeping import run_housekeeping  # noqa: E402,F401
from .paths import (  # noqa: E402,F401
    T2_INFIX,
    delete_sibling,
    is_sibling_path,
    primary_from_sibling,
    sibling_exists,
    sibling_path,
)
from .mqtt import (  # noqa: E402,F401
    MqttPublisher,
    RateTracker,
    _slugify_camera,
)
from .mqtt_stats import (  # noqa: E402,F401
    CameraStats,
    FrigateStats,
    _MB_BYTES,
    collect_frigate_stats,
)
from .probe_loop import (  # noqa: E402,F401
    PROBE_SLEEP_SEC,
    _get_unprobed_recordings,
    _store_probe,
    run_probe_loop,
)
from .throttle import (  # noqa: E402,F401
    MAX_SLEEP_SEC,
    RateLimiter,
    _THROTTLE_WINDOW_SEC,
)
from .util import _display_path, _fmt, log, set_log_level  # noqa: E402,F401

# app must be imported last (it pulls in everything else).
from .app import (  # noqa: E402,F401
    _pace_then_compress,
    _warn_qsv_fps_conflicts,
    main,
    run_main_loop,
)
