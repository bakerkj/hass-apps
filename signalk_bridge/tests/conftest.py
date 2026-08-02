# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Synthetic Signal K vessel tree that exercises the path mapping.

Deliberately not modelled on any one boat. It carries twin engines, several
battery banks under differing instance ids, multiple tank types, solar, a
charger, switch banks, notifications and a GPS fix, so the wildcard capture and
grouping logic is exercised across varied instance names rather than a fixed
inventory. The source families below are illustrative of the kind of gear each
Signal K path comes from, not a specific product list:

* MFD / GPS         -> navigation.*
* Wind / depth / log-> environment.*
* Engine bridge     -> propulsion.*
* Battery monitor   -> electrical.batteries.*, electrical.solar.*, electrical.chargers.*
* Digital switching -> electrical.switches.*
* Tank senders      -> tanks.*

Values are in Signal K's SI units (radians, kelvin, m/s, Pa, 0..1 ratios, Hz),
which is the whole reason the conversions exist.
"""

from typing import Any

import pytest


def leaf(value: Any, source: str = "n2k.1") -> dict[str, Any]:
    """A Signal K leaf: a value plus the usual metadata siblings."""
    return {
        "value": value,
        "$source": source,
        "timestamp": "2026-07-22T04:00:00.000Z",
        "pgn": 130306,
    }


@pytest.fixture
def vessel_tree() -> dict[str, Any]:
    return {
        "name": "Test Vessel",
        "mmsi": "000000000",
        # --- MFD / GPS ---
        "navigation": {
            "position": leaf({"latitude": 40.0, "longitude": -70.0}),
            "speedOverGround": leaf(3.086),  # m/s  = 6.0 kn
            "speedThroughWater": leaf(2.983),  # m/s  = 5.8 kn
            "courseOverGroundTrue": leaf(1.5708),  # rad  = 90 deg
            "headingMagnetic": leaf(1.6057),  # rad  = 92 deg
            "attitude": {
                "roll": leaf(0.0873),  # rad  = 5 deg
                "pitch": leaf(-0.0349),  # rad  = -2 deg
            },
            "log": leaf(1852000.0),  # m    = 1000 NM
            "trip": {"log": leaf(18520.0)},  # m    = 10 NM
            "magneticVariation": leaf(-0.2618),  # rad  = -15 deg
            "gnss": {
                "satellites": leaf(9),
                "antennaAltitude": leaf(3.2),
                "horizontalDilution": leaf(0.8),
                "methodQuality": leaf("DGNSS fix"),  # enum -> text sensor
                "type": leaf("GPS+SBAS/WAAS"),
            },
            # Active GOTO/route -- the MFD only emits these while navigating.
            "courseGreatCircle": {
                "crossTrackError": leaf(28.81),  # m (starboard of track)
                "bearingTrackTrue": leaf(1.1589),  # rad  = 66.4 deg
                "nextPoint": {
                    "distance": leaf(233.0),  # m
                    "bearingTrue": leaf(5.592),  # rad  = 320.4 deg
                    "velocityMadeGood": leaf(0.06),  # m/s
                    "timeToGo": leaf(3881.839),  # s
                    "name": leaf("GOTO CURSOR"),  # enum -> text sensor
                },
                "activeRoute": {"name": leaf("Route")},  # enum -> text sensor
            },
        },
        # --- autopilot / rudder ---
        "steering": {
            "rudderAngle": leaf(-0.0872665),  # rad  = -5 deg
            "autopilot": {
                "state": leaf("standby"),  # enum -> text sensor
                "target": {"headingMagnetic": leaf(1.0825)},  # rad = 62 deg
            },
        },
        # --- wind / depth ---
        "environment": {
            "depth": {
                "belowTransducer": leaf(12.4),
                "belowKeel": leaf(10.9),
            },
            "wind": {
                "speedApparent": leaf(6.173),  # m/s  = 12 kn
                "angleApparent": leaf(-0.7854),  # rad  = -45 deg (port)
                "speedTrue": leaf(7.716),
                "angleTrueWater": leaf(-1.0472),  # rad  = -60 deg
            },
            "water": {"temperature": leaf(288.15)},  # K    = 15 C
            # Water set & drift -- one composite leaf, split into two sensors.
            "current": leaf({"setTrue": 1.5708, "drift": 0.5}),  # 90 deg, 0.5 m/s
            "outside": {
                "temperature": leaf(293.15),  # K    = 20 C
                "pressure": leaf(101325.0),  # Pa   = 1013.25 hPa
            },
        },
        # --- engine gateway ---
        "propulsion": {
            "port": {
                "revolutions": leaf(29.17),  # Hz   = 1750 rpm
                "temperature": leaf(355.37),  # K    = 82.2 C
                "oilPressure": leaf(310264.0),  # Pa   = 310.26 kPa
                "alternatorVoltage": leaf(14.2),
                "runTime": leaf(4_320_000.0),  # s    = 1200 h
            },
            # A second engine under a different instance id exercises the
            # wildcard grouping for more than one of the same subsystem.
            "starboard": {
                "revolutions": leaf(25.0),  # Hz   = 1500 rpm
                "temperature": leaf(350.0),  # K    = 76.85 C
            },
        },
        # --- battery monitor / solar / charger ---
        "electrical": {
            "batteries": {
                "house": {
                    "voltage": leaf(12.84),
                    "current": leaf(-8.2),
                    "stateOfCharge": leaf(0.87),  # ratio = 87 %
                    "temperature": leaf(295.15),  # K     = 22 C
                    "capacity": {"timeRemaining": leaf(36000.0)},  # s = 10 h
                },
                "start": {
                    "voltage": leaf(12.61),
                },
                # N2K style: SoC nested under capacity.
                "1": {
                    "voltage": leaf(13.2),
                    "capacity": {"stateOfCharge": leaf(0.55)},  # ratio = 55 %
                },
            },
            # Digital switching.
            "switches": {
                "bank": {
                    "0": {
                        "1": {"state": leaf(1), "order": leaf(1)},
                        "2": {"state": leaf(0), "order": leaf(2)},
                    },
                },
            },
            "solar": {
                "mppt1": {
                    "voltage": leaf(18.9),
                    "current": leaf(4.1),
                    "panelPower": leaf(77.5),
                },
            },
            "chargers": {
                "ac1": {"chargingMode": leaf("bulk")},  # enum -> text sensor
            },
        },
        # --- tank senders ---
        "tanks": {
            "freshWater": {"0": {"currentLevel": leaf(0.62)}},
            "fuel": {"0": {"currentLevel": leaf(0.45)}},
            "blackWater": {"0": {"currentLevel": leaf(0.13)}},
        },
        # --- notifications / alarms ---
        "notifications": {
            "instrument": {
                "PilotOffCourse": leaf(
                    {
                        "state": "alarm",
                        "message": "Pilot Off Course",
                        "method": ["visual"],
                    }
                ),
            },
            "navigation": {
                "anchor": leaf(
                    {"state": "normal", "message": "Anchor watch", "method": []}
                ),
            },
        },
    }
