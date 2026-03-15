#!/usr/bin/with-contenv bash
# shellcheck shell=bash
set -euo pipefail

OPTIONS="/data/options.json"
if [[ ! -f "$OPTIONS" ]]; then
  echo "Missing $OPTIONS (add-on options)."
  exit 1
fi

exec python3 /system_resource_tuner.py --options "$OPTIONS"
