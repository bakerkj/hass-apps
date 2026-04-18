# Frigate Compressor

A Home Assistant add-on that runs as a long-running daemon to compress old
Frigate NVR recordings using Intel QSV hardware acceleration (with NVENC and CPU
fallback).

## How it works

- Reads Frigate's own SQLite database (`frigate.db`) to discover recordings
- Compresses files as soon as they age past the configured thresholds
- Wakes up precisely when the next file becomes eligible — no fixed polling
- Tracks compression state in its own SQLite database (`/config/compress.db`)
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

### HAOS options (options.json)

| Option                       | Default                                         | Description                                                                      |
| ---------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------- |
| `encoder`                    | `qsv`                                           | `qsv` (Intel libmfx), `vaapi` (Intel/AMD direct VA-API), `nvenc` (NVIDIA), `cpu` |
| `max_parallel_jobs`          | `2`                                             | Concurrent ffmpeg processes                                                      |
| `housekeeping_interval_days` | `7`                                             | How often to prune DB and log summary                                            |
| `frigate_db`                 | `/addon_configs/ccab4aaf_frigate-fa/frigate.db` | Path to Frigate's SQLite DB                                                      |
| `recordings_dir`             | `/media/frigate/recordings`                     | Path to Frigate recordings                                                       |
| `compress_db`                | `/config/compress.db`                           | Path to compression tracking DB                                                  |
| `config_path`                | `/config/config.yaml`                           | Path to YAML camera config file                                                  |
| `log_level`                  | `INFO`                                          | `DEBUG`, `INFO`, `WARNING`, `ERROR`                                              |

### YAML config file (`/config/config.yaml`)

Camera-centric compression settings live in a separate YAML file. This file has
a `defaults` block and a `cameras` block. See `config.yaml.example` for a
working template.

`dry_run` can be set per-camera or globally in the YAML defaults block. It
defaults to `false` in the built-in defaults but `config.yaml.example` ships
with `dry_run: true` so first-time installs log-only until you're ready.

### Per-tier, per-type settings

Both `tier1` and `tier2` accept the same nested structure. These are set in the
YAML config file under the `defaults` block (or per-camera):

```yaml
defaults:
  tier1:
    min_days: 7
    quality: 28 # CQ/CRF (0-51, lower = better)
    scale_mode: none # none | halve | fixed | fraction
    scale_value: "" # used by fixed ("1280:720") and fraction ("0.5")
    fps_mode: none # none | cap | fraction
    fps_value: 1.0 # cap=max fps, fraction=multiplier vs. source fps
    motion:
      quality: 26
      scale_mode: halve
    object:
      quality: 22
  tier2:
    min_days: 30
    quality: 34
    scale_mode: halve
    fps_mode: cap
    fps_value: 4.0
    motion:
      quality: 30
      fps_value: 8.0
    object:
      quality: 26
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

Camera-specific settings are configured in the YAML config file
(`/config/config.yaml`) under the `cameras` block. Each camera can override any
setting from the `defaults` block at the camera, tier, or type level. Only
specify what differs — everything else inherits from defaults.

```yaml
defaults:
  tier1:
    quality: 28
    motion:
      scale_mode: halve
  tier2:
    quality: 34
    scale_mode: halve

cameras:
  front_door:
    tier1:
      quality: 24 # camera tier base override
      object:
        quality: 18 # camera tier per-type override
    tier2:
      scale_mode: none # keep full resolution in tier 2
  doorbell:
    tier1:
      quality: 26
    tier2:
      fps_mode: none
  garage:
    enabled: false # skip this camera entirely
```

Resolution order (later overrides earlier):

1. Built-in defaults
2. YAML `defaults` block (base + per-type)
3. Camera tier base fields
4. Camera tier per-type fields

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

The tracking database lives at `/config/compress.db` inside the add-on's config
directory (persists across reinstalls). Query it with any SQLite tool:

```bash
sqlite3 /config/compress.db "SELECT * FROM savings_by_camera;"
sqlite3 /config/compress.db "SELECT * FROM recent_errors;"
```
