#!/usr/bin/with-contenv bash
# shellcheck shell=bash
set -euo pipefail

OPTIONS="/data/options.json"
if [[ ! -f "$OPTIONS" ]]; then
  echo "Missing $OPTIONS (add-on options)."
  exit 1
fi

export PYTHONPATH=/opt
exec python3 -m container_info_mqtt --options "$OPTIONS"
