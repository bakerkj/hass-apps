# Container Info MQTT

This add-on polls Docker directly and publishes Home Assistant MQTT Discovery
sensors for each container.

## What It Does

- Creates one Home Assistant device per container.
- Creates separate sensors per metric per container.
- Publishes container runtime metrics from Docker Engine API stats.
- Publishes container limits from `docker inspect`:
  - `cpuset_cpus`
  - `cpu_shares`
  - `blkio_weight`
- Supports container include/exclude regex filters.
- Supports metric include-list selection.
- Uses stable, name-based entity IDs across container restarts.

## Data Sources

- `docker ps` for running container inventory
- Docker Engine API `/containers/<id>/stats?stream=false` for raw
  CPU/memory/network/disk counters
- `docker inspect` for status and host config limits

## Default Metrics

- `cpu_percent`
- `memory_usage`
- `network_rx_total`
- `network_tx_total`
- `io_read_total`
- `io_write_total`
- `network_rx_rate`
- `network_tx_rate`
- `io_read_rate`
- `io_write_rate`
- `status`
- `cpuset_cpus`
- `cpu_shares`
- `blkio_weight`

Rate metrics are computed from cumulative counter deltas each poll interval.

All totals are consumed as raw counts from Docker Engine (bytes/integers), so no
human-unit parsing is required.

## MQTT Topics

Default base topic: `container_info`

- Availability: `container_info/availability`
- Heartbeat: `container_info/heartbeat`
- Per-sensor state: `container_info/<container_slug>/<metric_key>/state`
