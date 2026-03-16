# System Resource Tuner

This add-on applies system resource tuning for containers and keeps those
settings reconciled over time.

## What It Tunes

Per configured container target, it can apply:

- `cpuset_cpus` (Docker `--cpuset-cpus`)
- `cpu_shares` (relative CPU priority via Docker `--cpu-shares`)
- `blkio_weight` (Docker `--blkio-weight`)

It can also tune selected processes:

- Container process tuning via `process_targets`
- Host process tuning via `host_process_targets`
- Process niceness (`renice`)
- Process CPU affinity for all threads (`add-on python3` +
  `os.sched_setaffinity`)

## How It Works

- Reads targets from add-on options.
- Inspects each target container's current HostConfig limits.
- Applies `docker update` only for values that differ.
- Optionally finds configured container and host processes by regex and applies
  process-level niceness/affinity.
- Re-checks on a configurable interval to re-apply if containers/processes
  restart.

## Requirements

- Add-on uses `docker_api: true` to access the Docker API.
- Add-on uses `host_pid: true` so process tuning can resolve and tune host PIDs
  directly.
- Because `host_pid: true` is incompatible with S6 overlay startup, this add-on
  bypasses `/init` and starts directly via `/run.sh`.
- Add-on requests privileged capabilities: `NET_ADMIN`, `SYS_ADMIN`,
  `SYS_RAWIO`, `SYS_TIME`, `SYS_NICE`.
- Add-on runs with `full_access: true`.
- AppArmor is disabled (`apparmor: false`) to match elevated-control addons like
  Advanced SSH.
- Protection mode must be OFF for write operations (container resource changes).
- Container names/IDs in `targets` must exist on the host.
- Target containers do not need `python3` or `taskset` for process tuning.
- `host_process_targets` applies to all host processes that match each regex.

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
  "process_targets": [
    {
      "container": "homeassistant",
      "process_match_regex": "python3 .*homeassistant|homeassistant",
      "nice": -5,
      "cpuset_cpus": "0-5"
    },
    {
      "container": "addon_core_mariadb",
      "process_match_regex": "mariadbd|mysqld",
      "nice": -3,
      "cpuset_cpus": "2,3,6,7"
    },
    {
      "container": "addon_ccab4aaf_frigate-fa",
      "process_match_regex": "go2rtc|ffmpeg",
      "nice": -2
    }
  ],
  "host_process_targets": [
    {
      "process_match_regex": "dockerd",
      "nice": -4,
      "cpuset_cpus": "0-1"
    },
    {
      "process_match_regex": "containerd",
      "nice": -4,
      "cpuset_cpus": "0-1"
    },
    {
      "process_match_regex": "containerd-shim-runc-v2",
      "nice": -2
    }
  ]
}
```

If `targets` is empty and no `process_targets` or `host_process_targets` tuning
values are set, the add-on stays running in idle mode.
