# Signal K to Home Assistant

Publishes Signal K marine data as Home Assistant MQTT Discovery sensors, so NMEA
2000 instrument data appears as normal HA entities.

## How it works

Signal K normalises everything on the NMEA 2000 bus into a single data model.
This add-on subscribes to that model's delta stream over Signal K's websocket,
maintains a live mirror of `vessels/self`, and maps known paths onto HA entities
— converting from Signal K's SI units into something readable.

**Only paths actually present are published.** The mapping table covers more
equipment than any one boat carries, so absent gear simply produces no entities
rather than phantom sensors.

**Per-path publish rate limiting** keeps busy sources (position, wind, RPM) from
flooding MQTT. Each SK path has a minimum interval between MQTT publishes; a
value that arrives inside the cap window is remembered and emitted as soon as
the window opens (the latest value always wins).

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

## Publish rate limiting

Signal K deltas arrive at the source's own cadence (sub-second for GPS, wind,
RPM). Publishing every one of them to MQTT would flood the broker and
pointlessly churn Home Assistant's state machine. The bridge caps how often each
SK path may republish; deltas that land inside the cap window are held (latest
wins) and emitted as soon as the window opens.

Defaults:

```yaml
publish_min_interval_seconds: 1.0
publish_path_overrides:
  navigation.position: 0.5
  environment.wind.*: 1.0
  propulsion.*.revolutions: 1.0
  electrical.batteries.*.voltage: 5.0
  electrical.batteries.*.stateOfCharge: 30.0
```

Overrides use fnmatch-style patterns against Signal K paths; the most specific
(longest) matching pattern wins. Discovery topics are **not** rate-limited —
they publish immediately on first sight of an entity so HA can create it.

### Pairing with `recorder_downsampler` for graph history

At source-rate publish, the recorder database grows fast. The companion
[`ha-recorder-downsampler`](https://github.com/bakerkj/ha-recorder-downsampler)
integration mirrors fast sources into 1/min aggregated siblings so the recorder
sees one row per minute per source, while HA's live state still updates on every
delta.

```yaml
# configuration.yaml
recorder_downsampler:
  interval: "00:01:00"
  method: auto
  rules: !include recorder_downsampler.yaml

recorder:
  exclude:
    entity_globs:
      - sensor.signalk_*_voltage
      - sensor.signalk_*_current
      - sensor.signalk_*_power
      - sensor.signalk_*_rpm
      - sensor.signalk_*_wind_*
      - sensor.signalk_navigation_*
```

```yaml
# recorder_downsampler.yaml
- name: Fast signalk sources
  entity_regex_include: ["^sensor\\.signalk_"]
  interval: "00:01:00"
  method: auto
```

The `interval_seconds` option still governs how often the resolver ticks (and
thus the maximum rate any published entity can reach given a matching
`publish_path_overrides` entry). Set it to `1` if you want the rate limiter to
actually control fine-grained cadence — leaving it at the historical default of
`10` effectively floors every publish at 10s regardless of the limiter.

## N2K fleet health

Every N2K device on the bus (Cerbo GX, YDNG-03 gateway, Actisense EMU-1, the AIS
receiver, MFDs, Victron shunts/chargers, ...) gets one `binary_sensor` per
device grouped under a single **N2K Fleet** device card. The state is `ON` when
the device has emitted any PGN within `fleet_health_stale_seconds`, `OFF`
otherwise -- a fast way to spot that the YDNG dropped off the bus (taking the
ICOM's DSC path with it) or that an MFD is powered down.

```yaml
fleet_health_enabled: true # default on -- one binary_sensor per bus device
fleet_health_stale_seconds: 90 # freshness window
```

Entities are keyed by canName (the address-claim identity, stable across bus
resets) rather than address, so a device that renumbers doesn't collide with its
old slot. The mutable address, model, manufacturer, deviceClass, last-seen PGN,
and freshness age all land in the attributes for at-a-glance diagnosis.

## Safety-critical notifications (DSC / MOB / distress)

Notification paths under DSC, MOB, and distress-relay branches are surfaced as
`binary_sensor` alarms with `device_class: safety` (not the generic `problem`
class every other notification uses). HA's mobile app and dashboards treat
`safety` alerts distinctly -- notifications lock the screen on Android and
render with a red banner -- so a DSC distress call or MOB event won't get lost
in the same lane as an engine over-temperature warning.

Covered path prefixes (matched by prefix so canboatjs's exact per-call sub-path
shape works either way):

- `notifications.mob*`
- `notifications.dsc.*`, `notifications.communications.dsc.*`,
  `notifications.communication.dsc.*`
- `notifications.communications.distress.*`,
  `notifications.communication.distress.*`

Own-vessel VHF callsign (`communication.callsignVhf`) surfaces as a plain-text
sensor when present.

## AIS targets

Opt-in via `ais_enabled: true`. When set, the bridge widens its Signal K
subscription to include all `vessels.*` contexts and publishes one HA
`device_tracker` per AIS-detected vessel on top of the boat's own instruments.
Each tracker's attributes carry position, SoG, CoG, heading, plus static data
(name, ship type, callsign, IMO, dimensions) as SK receives them.

```yaml
ais_enabled: false # off by default
ais_expire_seconds: 900 # target dropped after this long with no delta
ais_max_targets: 200 # entity-registry cap (sticky targets don't count)
ais_always_retain: # MMSIs whose tracker never expires
  - "367674550"
```

**Cleanup semantics.** Targets that fall silent past `ais_expire_seconds` have
their discovery + attributes topics cleared with empty-retained payloads so HA
unregisters the entity, not left as ghost dots on the map. Sticky MMSIs in
`ais_always_retain` never expire once first observed — the tracker keeps its
last-known position with a stale `last_seen` attribute.

**Cold-start orphan reap.** On startup, the bridge subscribes to
`homeassistant/device_tracker/signalk/ais_+/config` for
`ais_reap_window_seconds` (default 15) to catalogue whatever AIS trackers HA
still remembers from a prior run. Any MMSI observed in that window but not seen
by the live registry gets empty-retained on both its config and attributes
topics -- so orphans from a bridge crash / config rename / target permanently
gone don't linger forever.

There is also a `sensor.signalk_ais_inventory` whose state is the current
tracked-target count. Its `targets` attribute carries a sorted (most-recent
first) list of `{mmsi, name, lat, lon, sog, cog, last_seen}` summaries,
truncated with a `truncated: true` flag if the batch would exceed HA's
per-attribute size budget.

Filter noisy AIS entities out of your recorder so long-running captures don't
bloat the database:

```yaml
# configuration.yaml
recorder:
  exclude:
    entity_globs:
      - device_tracker.signalk_ais_*
```

## Staleness detection

Signal K keeps a value after its source stops broadcasting — it only freezes the
`timestamp`. Left alone, a powered-down instrument shows a **frozen reading that
looks live** in Home Assistant. With `stale_after_seconds` set (0 = off), the
bridge instead learns each path's normal refresh cadence and stops republishing
a path once its value is far older than that cadence; Home Assistant's
`expire_after` then marks the entity **unavailable**, and it recovers
automatically when fresh data returns.

Detection is adaptive per path, so a fast depth sounder and a slow battery
monitor are each judged against their own rhythm — a slow-_changing_ value is
not a slow-_broadcast_ value, since N2K rebroadcasts on a fixed schedule.
Multi-source (fanned-out) entities go stale independently, so one dead battery
monitor doesn't take its neighbour with it. `notifications.*` are exempt (a
quiescent alarm must stay OFF, not flip to unavailable). `stale_after_seconds`
is the ceiling on detection latency — a path is judged against its own learned
cadence, clamped to at most this value — so set it above your slowest device's
broadcast interval.

`stale_learning_max_age` (default 1800s, 0 = in-memory only) persists the
learned cadences to `/data` and reloads them **only if the snapshot is newer
than that window** — so a brief restart keeps its learning (catching a device
that died during the downtime on the first poll), while a long outage safely
cold-starts.

Caveats: the vessel **position** is a device tracker (which has no
`expire_after`), so it is left alone rather than withheld — it keeps its last
fix instead of being cleared. An entity goes unavailable at roughly the
detection threshold **plus** one `expire_after` window (the bridge stops
publishing, then HA's timer elapses), not at `stale_after_seconds` exactly.
Leaves with no parseable timestamp aren't tracked. Finally, if `signalk_url`
points at a **remote** Signal K server, keep both clocks NTP-synced; on the
default localhost server the timestamps and the bridge share one clock, so
freshness is exact regardless of the absolute time.

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
