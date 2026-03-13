# BirdNET Audio Stream (Home Assistant Add-on)

This add-on uses go2rtc + ffmpeg to stream microphone audio over RTSP for
BirdNET-style consumers.

## What It Does

- Captures audio from ALSA or Pulse input.
- Publishes an RTSP audio stream using go2rtc.
- Supports codecs:
  - `opus`
  - `flac`
  - `pcm` (copy)
- Optional mixer control/volume setup via `amixer`.
- Optional go2rtc API auth (`username` / `password`).

## Ports

- `8554/tcp`: RTSP stream
- `1984/tcp`: go2rtc API

RTSP URL format:

- `rtsp://<host>:8554/<stream_name>`

## Notes

- Uses add-on options to generate go2rtc config at runtime.
- `bitrate` is used for Opus when configured.
- `ffmpeg_volume`, channel count, and sample rate are applied in ffmpeg input
  template.
