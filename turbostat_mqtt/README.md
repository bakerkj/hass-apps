# Turbostat MQTT (Home Assistant Add-on)

This add-on runs `turbostat --Summary` and publishes parsed metrics to MQTT with
Home Assistant discovery.

## What It Does

- Parses turbostat summary output dynamically from headers.
- Creates MQTT discovery sensors from detected columns.
- Publishes:
  - per-sensor state topics
  - add-on availability topic
  - heartbeat topic
- Optionally publishes one consolidated raw turbostat sample to a dedicated
  debug topic (`publish_raw_sample`, disabled by default). The raw sample is
  never attached to the individual sensor entities.

## MQTT Topics

Default base topic: `turbostat`

- Availability: `turbostat/availability`
- Heartbeat: `turbostat/heartbeat`
- Per-sensor state: `turbostat/<sensor_key>/state`
- Raw sample (only when `publish_raw_sample` is enabled): `turbostat/raw_sample`

## Availability and Stale Data

- The add-on publishes `online`/`offline` to `turbostat/availability`.
- MQTT Last Will marks `turbostat/availability` as `offline` on unexpected
  disconnects.
- Discovery entities include `expire_after` based on `sample_timeout_seconds`,
  so entities become unavailable if updates stop.
- Uses the same direct MQTT publish + fail-fast disconnect watchdog model as
  `intel_gpu_top_mqtt` (exits for supervisor restart on prolonged
  publish/disconnect failure).
- Default cutoffs are significantly longer: `sample_timeout_seconds=180` and
  `mqtt_disconnect_timeout_seconds=300`.

## Notes

- Sensor set depends on turbostat columns available on your host.
- A few noisy columns are intentionally skipped (`IRQ`, `NMI`, `SMI`, selected
  package/system residency fields).
- Values are retained in MQTT for stable dashboard/history behavior.
