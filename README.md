# Ken's Home Assistant Apps

A monorepo of [Home Assistant](https://www.home-assistant.io/) add-ons I run on
my own setup. Each subdirectory is a self-contained add-on with its own
`Dockerfile`, `config.json`, and `README.md`.

## Installing as a Home Assistant add-on repository

In Home Assistant, go to **Settings → Add-ons → Add-on Store**, open the
three-dot menu, choose **Repositories**, and add:

```
https://github.com/bakerkj/hass-apps
```

Each add-on below will then appear in the store.

## Add-ons

| Add-on                                           | What it does                                                                                                                                                                                                                                |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [birdnet_audio_stream/](birdnet_audio_stream/)   | Streams ALSA/Pulse microphone audio over RTSP using go2rtc + ffmpeg. Intended as an audio source for BirdNET-style consumers. Supports `opus`, `flac`, and `pcm` codecs and optional mixer control.                                         |
| [container_info_mqtt/](container_info_mqtt/)     | Publishes per-container Docker metrics (CPU %, memory, network/IO rates, uptime) and limits (cpuset, CPU shares, blkio weight) as Home Assistant MQTT Discovery sensors.                                                                    |
| [ffmpeg_snapshotter/](ffmpeg_snapshotter/)       | Runs ffmpeg on demand to capture timestamped JPEG snapshots from RTSP streams, maintains `latest.jpg` symlinks, and applies retention by count/age. Supports VAAPI hardware acceleration and per-stream MQTT status.                        |
| [frigate_compressor/](frigate_compressor/)       | Long-running daemon that re-encodes older Frigate NVR recordings using Intel QSV (with VAAPI/NVENC/CPU fallback). Two-tier age-based compression, parallel jobs, MQTT status, and per-camera config. State is tracked in a local SQLite DB. |
| [haos_configurator/](haos_configurator/)         | Manifest-driven one-shot installer for persistent HAOS host files (udev rules, init scripts, etc.). Sha256-compares each file to the host destination via `nsenter`, atomically installs differences, and fires per-file on-change actions. |
| [intel_gpu_top_mqtt/](intel_gpu_top_mqtt/)       | Wraps `intel_gpu_top` and publishes Intel GPU engine/power/frequency metrics to MQTT with Home Assistant discovery.                                                                                                                         |
| [system_resource_tuner/](system_resource_tuner/) | Applies and reconciles container resource tuning (cpuset, CPU shares, blkio weight) plus per-process `nice`/cpuset for both in-container and host processes.                                                                                |
| [turbostat_mqtt/](turbostat_mqtt/)               | Runs `turbostat` and publishes CPU package/core power, frequency, and C-state metrics to MQTT with Home Assistant discovery.                                                                                                                |

See each add-on's own `README.md` for configuration options and operational
notes.

## License

Each add-on directory contains its own `LICENSE.md`.
