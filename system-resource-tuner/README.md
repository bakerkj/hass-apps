# System Resource Tuner

This add-on applies system resource tuning for containers and keeps those
settings reconciled over time.

## What It Tunes

Per configured container target, it can apply:

- `cpuset_cpus` (Docker `--cpuset-cpus`)
- `cpu_shares` (relative CPU priority via Docker `--cpu-shares`)
- `blkio_weight` (Docker `--blkio-weight`)

It can also tune the Home Assistant process (inside a target container):

- Process niceness (`renice`)
- Process CPU affinity for all threads (`taskset -apc`)

## How It Works

- Reads targets from add-on options.
- Inspects each target container's current HostConfig limits.
- Applies `docker update` only for values that differ.
- Optionally finds a Home Assistant process by regex and applies process-level
  niceness/affinity.
- Re-checks on a configurable interval to re-apply if containers/processes
  restart.

## Requirements

- Add-on uses `docker_api: true` to access the Docker API.
- Add-on requests privileged capabilities: `NET_ADMIN`, `SYS_ADMIN`,
  `SYS_RAWIO`, `SYS_TIME`, `SYS_NICE`.
- Add-on runs with `full_access: true`.
- AppArmor is disabled (`apparmor: false`) to match elevated-control addons like
  Advanced SSH.
- Protection mode must be OFF for write operations (container resource changes).
- Container names/IDs in `targets` must exist on the host.
- For process affinity tuning, the target container must have `taskset`
  available.

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
  ],
  "homeassistant_process": {
    "container": "homeassistant",
    "process_match_regex": "python3 .*homeassistant|homeassistant",
    "nice": -5,
    "cpuset_cpus": "2-3"
  }
}
```

If `targets` is empty and no `homeassistant_process` tuning values are set, the
add-on stays running in idle mode.
