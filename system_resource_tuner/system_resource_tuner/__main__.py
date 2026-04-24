# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Module entry point — ``python -m system_resource_tuner``."""

from .app import main

if __name__ == "__main__":
    raise SystemExit(main())
