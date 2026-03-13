# Intel GPU Top MQTT (Home Assistant Add-on)

This add-on runs `intel_gpu_top` and publishes Intel GPU metrics to MQTT
using Home Assistant MQTT Discovery.

## What It Publishes

Default base topic: `intel_gpu_top`

- Availability: `intel_gpu_top/availability`
- Heartbeat: `intel_gpu_top/heartbeat`
- Optional raw sample: `intel_gpu_top/raw_sample`
- Per-sensor state: `intel_gpu_top/<sensor_key>/state`
- Per-sensor attributes: `intel_gpu_top/<sensor_key>/attributes`

Published sensor keys:

- `rc6_percent`
- `freq_mhz`
- `freq_requested_mhz`
- `interrupts_per_s`
- `power_gpu_w`
- `power_pkg_w`
- `engine_render_3d_busy_percent`
- `engine_video_busy_percent`
- `engine_videoenhance_busy_percent`
- `engine_blitter_busy_percent`

Notes:

- Wait/semaphore engine metrics are intentionally not published.
- Deprecated discovery entries for old wait/semaphore sensors are cleared
  automatically.

## Runtime Behavior

- Auto-selects a render node using `intel_gpu_top -L`.
- Optional `preferred_device_regex` allows selecting a specific GPU.
- Publishes MQTT discovery once the first valid sample is parsed.
- Includes watchdogs for:
  - sample timeout
  - render node disappearance
  - prolonged MQTT disconnect

## Requirements

- `/dev/dri` must be available in the add-on container.
- Host/kernel permissions must allow `intel_gpu_top` to collect metrics.
