# Frigate Compressor

A Home Assistant add-on that runs as a long-running daemon to compress old
Frigate NVR recordings using Intel QSV hardware acceleration (with NVENC and CPU
fallback).

## How it works

- Reads Frigate's own SQLite database (`frigate.db`) to discover recordings
- Compresses files as soon as they age past the configured thresholds
- Wakes up precisely when the next file becomes eligible — no fixed polling
- Tracks compression state in its own SQLite database (`/data/compress.db`)
- Updates `segment_size` in Frigate's DB after compression so storage UI stays
  accurate
- Runs parallel compression jobs (configurable) for faster throughput
- Periodic housekeeping prunes stale DB entries and logs a savings summary

## Two-tier compression

Recordings are compressed in two passes based on age. Each tier has independent
quality, scale, and FPS settings for three recording types:

| Recording type | When                                                 |
| -------------- | ---------------------------------------------------- |
| `object`       | At least one object detected — highest value footage |
| `motion`       | Motion detected but no object hit                    |
| `continuous`   | No motion, no objects — lowest priority              |

Tier 1 applies first (`tier1.min_days` days old). Tier 2 applies later
(`tier2.min_days` days old) and typically uses harder compression.

## Configuration

| Option                       | Default                     | Description                                                                      |
| ---------------------------- | --------------------------- | -------------------------------------------------------------------------------- |
| `encoder`                    | `qsv`                       | `qsv` (Intel libmfx), `vaapi` (Intel/AMD direct VA-API), `nvenc` (NVIDIA), `cpu` |
| `max_parallel_jobs`          | `2`                         | Concurrent ffmpeg processes                                                      |
| `housekeeping_interval_days` | `7`                         | How often to prune DB and log summary                                            |
| `frigate_db`                 | `/config/frigate.db`        | Path to Frigate's SQLite DB                                                      |
| `recordings_dir`             | `/media/frigate/recordings` | Path to Frigate recordings                                                       |
| `compress_db`                | `/data/compress.db`         | Path to compression tracking DB                                                  |
| `log_level`                  | `INFO`                      | `DEBUG`, `INFO`, `WARNING`, `ERROR`                                              |
| `dry_run`                    | `true`                      | Log actions only — no files or DB writes                                         |

### First-run safety: `dry_run`

`dry_run` defaults to **`true`**. In this mode the add-on will:

- Scan Frigate's DB and identify every recording that would be compressed
- Log the exact ffmpeg command, target tier, and expected savings for each one
- **Not** invoke ffmpeg, write to `compress.db`, modify any recording file, or
  update `segment_size` in Frigate's DB

This lets you confirm the encoder, tier thresholds, and per-camera overrides
behave the way you expect before letting the add-on touch real recordings. Once
you're happy with what the logs show, set `dry_run: false` to enable
compression.

### Per-tier, per-type settings

Both `tier1` and `tier2` accept the same nested structure:

```yaml
tier1:
  min_days: 7
  continuous:
    quality: 28 # CQ/CRF (0-51, lower = better)
    scale_mode: none # none | halve | fixed | fraction
    scale_value: "" # used by fixed ("1280:720") and fraction ("0.5")
    fps_mode: none # none | cap | fraction
    fps_value: 1.0 # cap=max fps, fraction=multiplier vs. source fps
  motion:
    quality: 26
    scale_mode: halve
    fps_mode: none
  object:
    quality: 22
    scale_mode: none
    fps_mode: none

tier2:
  min_days: 30
  continuous:
    quality: 34
    scale_mode: halve
    fps_mode: cap
    fps_value: 4.0
  motion:
    quality: 30
    scale_mode: halve
    fps_mode: cap
    fps_value: 8.0
  object:
    quality: 26
    scale_mode: halve
    fps_mode: cap
    fps_value: 8.0
```

#### Scale modes

| Mode       | Effect                                                |
| ---------- | ----------------------------------------------------- |
| `none`     | Keep original resolution                              |
| `halve`    | Half width, half height (e.g. 4K→1080p, 1080p→720p)   |
| `fixed`    | Exact dimensions from `scale_value` e.g. `"1280:720"` |
| `fraction` | Multiply source dimensions by `scale_value` (0.0–1.0) |

#### FPS modes

| Mode       | Effect                                               |
| ---------- | ---------------------------------------------------- |
| `none`     | Keep original framerate                              |
| `cap`      | Hard cap at `fps_value` fps                          |
| `fraction` | Multiply source fps by `fps_value` (e.g. 0.5 = half) |

### Per-camera overrides

Any quality, scale, or fps setting can be overridden per camera, per tier, and
per recording type. Each entry specifies the camera name, which tier it applies
to, which recording type, and whichever fields to override. Unspecified fields
fall back to the global tier settings.

```yaml
camera_overrides:
  # 4K cam: keep full resolution for object clips in both tiers
  - name: front_door
    tier: 1
    recording_type: object
    scale_mode: fixed
    scale_value: "1920:1080"
  - name: front_door
    tier: 2
    recording_type: object
    scale_mode: fixed
    scale_value: "1920:1080"
    quality: 24

  # Low-res doorbell: never downscale, use higher quality
  - name: doorbell
    tier: 1
    recording_type: continuous
    scale_mode: none
    quality: 26
  - name: doorbell
    tier: 2
    recording_type: continuous
    scale_mode: none
    fps_mode: none
```

Each entry must have `name`, `tier` (1 or 2), and `recording_type`
(`continuous`, `motion`, or `object`). All other fields are optional.

## Choosing the encoder

| `encoder` | When to use                                                                                         |
| --------- | --------------------------------------------------------------------------------------------------- |
| `qsv`     | Intel iGPU via Intel's libmfx/Media SDK. Most mature path on Intel.                                 |
| `vaapi`   | Intel iGPU (or AMD GPU with Mesa) via direct VA-API. More reliable on Linux when libmfx misbehaves. |
| `nvenc`   | NVIDIA discrete GPU via NVENC.                                                                      |
| `cpu`     | Software libx264. Fallback when no GPU is available.                                                |

Switching is just a config change — no other changes needed. Already-compressed
files are not re-processed.

At startup the add-on runs a 1-second synthetic encode against the chosen
encoder and aborts (in non-`dry_run` mode) if the GPU/driver/cgroup is not
reachable, so misconfiguration shows up immediately rather than as a flood of
per-file errors.

## Inspecting the compression database

The tracking database lives at `/data/compress.db` inside the add-on's data
directory. Query it with any SQLite tool:

```bash
sqlite3 /data/compress.db "SELECT * FROM savings_by_camera;"
sqlite3 /data/compress.db "SELECT * FROM recent_errors;"
```
