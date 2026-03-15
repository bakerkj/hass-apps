# System Resource Tuner

This add-on applies system resource tuning for containers and keeps those
settings reconciled over time.

## What It Tunes

Per configured container target, it can apply:

- `cpuset_cpus` (Docker `--cpuset-cpus`)
- `cpu_shares` (relative CPU priority via Docker `--cpu-shares`)
- `blkio_weight` (Docker `--blkio-weight`)

## How It Works

- Reads targets from add-on options.
- Inspects each target container's current HostConfig limits.
- Applies `docker update` only for values that differ.
- Re-checks on a configurable interval to re-apply if containers restart.

## Requirements

- Add-on uses `docker_api: true` to access the Docker API.
- Add-on requests privileged capabilities: `NET_ADMIN`, `SYS_ADMIN`,
  `SYS_RAWIO`, `SYS_TIME`, `SYS_NICE`.
- Add-on runs with `full_access: true`.
- AppArmor is disabled (`apparmor: false`) to match elevated-control addons like
  Advanced SSH.
- Protection mode must be OFF for write operations (container resource changes).
- Container names/IDs in `targets` must exist on the host.

## Example Options

```json
{
  "interval_seconds": 60,
  "apply_on_start": true,
  "dry_run": false,
  "log_level": "INFO",
  "targets": [
    {
      "container": "addon_core_mosquitto",
      "cpuset_cpus": "0-1",
      "cpu_shares": 1024,
      "blkio_weight": 500
    }
  ]
}
```
