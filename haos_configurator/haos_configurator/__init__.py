# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""HAOS Configurator add-on package.

Installs persistent host files into HAOS according to a user-supplied
manifest in the add-on's config directory.  See :mod:`.app` for the
top-level orchestration.
"""

import os

__version__ = os.environ.get("ADDON_VERSION", "dev")
