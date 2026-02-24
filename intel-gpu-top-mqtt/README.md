# Intel GPU Top MQTT (Home Assistant Add-on)

This add-on runs `intel_gpu_top` inside the add-on container and publishes Intel GPU metrics to MQTT using **Home Assistant MQTT Discovery**.

## Features

- `protection_mode: false`
- Maps `/dev/dri` into the container
- Auto-selects GPU device using `intel_gpu_top -L` (prefers first `/dev/dri/renderD*`)
  - Optionally set `preferred_device_regex` to pick a specific device if you have multiple Intel GPUs.
- Publishes **separate MQTT topics per sensor**
- Publishes **attributes** per sensor (`json_attributes_topic`)
- Publishes an availability topic
- Optional `publish_raw_sample` for debugging

## MQTT Topics

Base topic (default): `intel_gpu`

- Availability: `intel_gpu/availability`
- Per-sensor state: `intel_gpu/<sensor_key>/state`
- Per-sensor attributes: `intel_gpu/<sensor_key>/attributes`
- Optional debug: `intel_gpu/raw_sample`

## Notes

- Ensure your host exposes Intel GPU nodes at `/dev/dri` (typical).
- If metrics are missing/zero, the host kernel/perf settings may restrict access.
