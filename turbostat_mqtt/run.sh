#!/usr/bin/with-contenv bash
set -euo pipefail

OPTIONS="/data/options.json"
if [[ ! -f "$OPTIONS" ]]; then
  echo "Missing $OPTIONS (add-on options)."
  exit 1
fi

exec python3 /turbostat_mqtt.py --options "$OPTIONS"
