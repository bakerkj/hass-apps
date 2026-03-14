# Glances Container Stats MQTT

This add-on polls Glances container metrics and publishes MQTT Discovery sensors
to Home Assistant.

## What It Does

- Creates one Home Assistant device per container.
- Creates separate sensors per metric per container.
- Supports container include/exclude regex filters.
- Supports metric include-list selection.

## Networking

This add-on uses `host_network: true` so it can reach the official Glances
add-on when Glances is running on host network.

Default Glances target: `http://localhost:61209` with endpoint
`/api/3/containers`.

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

Metric sources are aligned with Glances container payloads, preferring
cumulative counters:

- `network.cumulative_rx` / `network.cumulative_tx`
- `io.cumulative_ior` / `io.cumulative_iow`

## MQTT Topics

Default base topic: `glances_containers`

- Availability: `glances_containers/availability`
- Heartbeat: `glances_containers/heartbeat`
- Per-sensor state: `glances_containers/<container_slug>/<metric_key>/state`

Rate metrics are computed directly from cumulative counters per container on
each poll interval.
