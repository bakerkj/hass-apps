# Turbostat MQTT (Home Assistant Add-on)

This add-on runs `turbostat --Summary` and publishes parsed metrics to
MQTT with Home Assistant discovery.

## What It Does

- Parses turbostat summary output dynamically from headers.
- Creates MQTT discovery sensors from detected columns.
- Publishes:
  - a combined JSON state payload
  - per-sensor state topics
  - per-sensor availability topics
  - heartbeat topic
- Optional raw sample payload fields (`publish_raw_sample`).

## MQTT Topics

Default base topic: `turbostat`

- Availability: `turbostat/availability`
- Heartbeat: `turbostat/heartbeat`
- Combined state JSON: `turbostat/state`
- Per-sensor state: `turbostat/<sensor_key>/state`
- Per-sensor availability: `turbostat/<sensor_key>/availability`

## Notes

- Sensor set depends on turbostat columns available on your host.
- A few noisy columns are intentionally skipped (`IRQ`, `NMI`, `SMI`,
  selected package/system residency fields).
- Values are retained in MQTT for stable dashboard/history behavior.
