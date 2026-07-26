# Signal K to Home Assistant

Publishes Signal K marine data as Home Assistant MQTT Discovery sensors, so NMEA
2000 instrument data appears as normal HA entities.

## How it works

Signal K normalises everything on the NMEA 2000 bus into a single data model.
This add-on polls that model and maps known paths onto HA entities, converting
from Signal K's SI units into something readable.

**Only paths actually present are published.** The mapping table covers more
equipment than any one boat carries, so absent gear simply produces no entities
rather than phantom sensors.

## Unit conversions

Signal K is strictly SI, which is unreadable on a dashboard. The bridge
converts:

| Quantity                     | Signal K   | Published               |
| ---------------------------- | ---------- | ----------------------- |
| Engine speed                 | Hz         | rpm (x60)               |
| Temperatures                 | kelvin     | °C                      |
| Angles / bearings            | radians    | degrees                 |
| State of charge, tank level  | 0..1 ratio | %                       |
| Barometric pressure          | Pa         | hPa                     |
| Oil / coolant pressure       | Pa         | kPa                     |
| Engine hours, time remaining | seconds    | hours                   |
| Speeds                       | m/s        | m/s (HA converts to kn) |

Bearings (course, heading, wind direction) wrap to 0–360°, while wind **angle**
stays signed — negative is to port, which is what a wind gauge should read.

## Devices

Entities are grouped into one HA device per boat subsystem rather than a single
flat list:

- **Navigation** — SOG, STW, COG, heading, attitude, log
- **Environment** — depth, wind, sea/air temperature, barometric pressure
- **Engine `<id>`** — rpm, coolant, oil pressure, alternator, engine hours
- **Battery `<id>`** — voltage, current, SoC, temperature, time remaining
- **Solar `<id>`**, **Charger `<id>`**, **Tank `<type>` `<id>`**
- **GPS**, **Steering**, **Digital switches bank `<id>`**, **Alarms**
- **Vessel** — position, shown on the HA map as a device tracker
- **NMEA 2000 Bus** — frame rate, receive errors, device counts, link status

## Requirements

Signal K must be running with a configured NMEA 2000 connection, and the bus
must actually be carrying traffic:

```
cat /sys/class/net/can0/statistics/rx_packets
```

If that stays at `0`, no data exists to bridge and the add-on will say so in its
log rather than failing silently.

## Authentication

Signal K runs with security enabled. If it refuses anonymous reads, the add-on
logs an explicit 401/403 message — create a device token under **Security →
Devices** and set `signalk_token`, or allow read-only access for unauthenticated
clients in Signal K's settings.

## Why polling rather than the delta websocket

Marine data reaches Home Assistant at a human timescale, a REST snapshot is
internally consistent, and there is no reconnect/backfill state machine to get
wrong. The websocket stream is the upgrade path if sub-second latency is ever
wanted.

## Coverage

The mapping targets common NMEA 2000 instrument categories: MFD/GPS and GNSS,
wind and depth instruments, engine gateways, battery/solar/charger monitors,
tank senders, and digital switching. Because it maps standardised Signal K
paths, any device that publishes them works — it is not tied to specific
vendors. It is validated by tests against a synthetic vessel tree — see
`tests/conftest.py`.

Beyond numeric sensors, the bridge also maps the vessel **position** (as a
device tracker on the HA map), **digital switch** banks and **notification
alarms** (as binary sensors), and enum/text states such as autopilot mode, GPS
fix quality and charger mode. Paths with no meaningful HA representation (e.g.
AIS target lists) are skipped rather than published in a misleading form.
