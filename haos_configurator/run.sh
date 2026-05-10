#!/bin/bash
# shellcheck shell=bash
#
# Plain bash — no `with-contenv` and no s6-overlay involvement.  s6's
# pid-1 check fires before our code under Protection mode and would
# hide the actual cause of failure, so the add-on's Dockerfile sets
# ENTRYPOINT [] to bypass /init entirely.  All this script needs to do
# is sanity-check that Supervisor mounted /data/options.json and then
# exec the Python module.
set -euo pipefail

OPTIONS="/data/options.json"
if [[ ! -f "$OPTIONS" ]]; then
  echo "Missing $OPTIONS (add-on options)."
  exit 1
fi

export PYTHONPATH=/opt
exec python3 -m haos_configurator
