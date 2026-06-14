# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Dashboard Entity Proxy.

A reverse proxy between browser clients and Home Assistant that intercepts the
/api/websocket subscribe_entities subscription and serves each client only the
entities its current dashboard/view needs.
"""

import os

__version__ = os.environ.get("ADDON_VERSION", "dev")
