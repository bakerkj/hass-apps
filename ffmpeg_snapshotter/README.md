# FFmpeg Snapshotter (Home Assistant Add-on)

This add-on takes periodic JPEG snapshots from configured RTSP streams using ffmpeg.

## What It Does

- Runs one worker per configured stream.
- Captures single-frame snapshots on each stream interval.
- Writes snapshots under `/media/...` (or prefixes `/media` automatically).
- Maintains a `latest.jpg` symlink per stream output directory.
- Supports retention policies:
  - by age (`retain_days`)
  - by count (`retain_count`)
- Supports global and per-stream ffmpeg argument overrides.

## Scheduling Behavior

- Streams sharing the same interval are evenly staggered to reduce burst load.
- Failed snapshot runs use exponential backoff.

## Notes

- VAAPI hwaccel arguments are supported; when used, output filter includes `hwdownload,format=nv12`.
- If no streams are configured, the add-on exits cleanly.
