#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="/data/options.json"

interval_seconds="$(jq -r '.interval_seconds' "$CONFIG_PATH")"
mqtt_host="$(jq -r '.mqtt_host' "$CONFIG_PATH")"
mqtt_port="$(jq -r '.mqtt_port' "$CONFIG_PATH")"
mqtt_username="$(jq -r '.mqtt_username' "$CONFIG_PATH")"
mqtt_password="$(jq -r '.mqtt_password' "$CONFIG_PATH")"
mqtt_discovery_prefix="$(jq -r '.mqtt_discovery_prefix' "$CONFIG_PATH")"
mqtt_base_topic="$(jq -r '.mqtt_base_topic' "$CONFIG_PATH")"
client_id="$(jq -r '.client_id' "$CONFIG_PATH")"
preferred_device_regex="$(jq -r '.preferred_device_regex' "$CONFIG_PATH")"
log_level="$(jq -r '.log_level' "$CONFIG_PATH")"
publish_raw_sample="$(jq -r '.publish_raw_sample' "$CONFIG_PATH")"

exec python3 /intel_gpu_mqtt.py   --interval-seconds "$interval_seconds"   --mqtt-host "$mqtt_host"   --mqtt-port "$mqtt_port"   --mqtt-username "$mqtt_username"   --mqtt-password "$mqtt_password"   --mqtt-discovery-prefix "$mqtt_discovery_prefix"   --mqtt-base-topic "$mqtt_base_topic"   --client-id "$client_id"   --preferred-device-regex "$preferred_device_regex"   --log-level "$log_level"   --publish-raw-sample "$publish_raw_sample"
