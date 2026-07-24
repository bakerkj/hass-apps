# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""
Shared pytest configuration: paho.mqtt stub.
"""

import logging
import sys
import types
from unittest.mock import MagicMock

logging.basicConfig(level=logging.WARNING)


# ---------------------------------------------------------------------------
# Stub out paho.mqtt so the source modules can be imported on machines that
# do not have paho-mqtt installed (or where we want to avoid network side
# effects during import).
# ---------------------------------------------------------------------------


def _make_paho_stub() -> None:
    paho = types.ModuleType("paho")
    paho_mqtt = types.ModuleType("paho.mqtt")
    paho_mqtt_client = types.ModuleType("paho.mqtt.client")

    # Minimal constants / class needed by the source modules at import time.
    paho_mqtt_client.MQTT_ERR_SUCCESS = 0  # type: ignore[attr-defined]

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        def publish(self, *args, **kwargs):
            m = MagicMock()
            m.rc = 0
            return m

        def connect(self, *args, **kwargs):
            pass

        def loop_start(self):
            pass

        def loop_stop(self):
            pass

        def disconnect(self):
            pass

        def username_pw_set(self, *args, **kwargs):
            pass

        def will_set(self, *args, **kwargs):
            pass

        def reconnect_delay_set(self, *args, **kwargs):
            pass

    paho_mqtt_client.Client = _Client  # type: ignore[attr-defined]

    paho.mqtt = paho_mqtt  # type: ignore[attr-defined]
    paho_mqtt.client = paho_mqtt_client  # type: ignore[attr-defined]

    sys.modules.setdefault("paho", paho)
    sys.modules.setdefault("paho.mqtt", paho_mqtt)
    sys.modules.setdefault("paho.mqtt.client", paho_mqtt_client)


_make_paho_stub()
