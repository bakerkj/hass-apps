#!/usr/bin/env sh
# Signal K server add-on entrypoint.
#
# Signal K keeps its entire configuration -- connections, plugins, security,
# vessel identity -- in one directory. Pointing that at /data is what makes the
# configuration survive add-on updates, which replace the container.
set -eu

OPTIONS="/data/options.json"
CONFIG_DIR="/data/signalk"

# /data is a runtime-mounted Supervisor volume (root-owned), so this can't be
# done in the Dockerfile -- the mount shadows anything baked into the image.
# Create the config dir and hand it to `node` here, at start; the server drops
# to node via su-exec below, matching how the upstream image runs it. Only
# recurse the chown when ownership is actually wrong (fresh volume, or a restore
# left root-owned files) -- on a normal restart the tree is already node-owned
# (signalk runs as node), so we skip walking a large config dir every time.
mkdir -p "${CONFIG_DIR}"
owner="$(stat -c '%U' "${CONFIG_DIR}")"
if [ "${owner}" != "node" ]; then
  chown -R node:node "${CONFIG_DIR}"
fi

get() {
  [ -f "${OPTIONS}" ] || {
    echo "$2"
    return
  }
  node -e '
    const fs = require("fs");
    let o = {};
    try { o = JSON.parse(fs.readFileSync(process.argv[1], "utf8")); } catch (e) {}
    const v = o[process.argv[2]];
    process.stdout.write(String(v === undefined || v === null ? process.argv[3] : v));
  ' "${OPTIONS}" "$1" "$2"
}

PORT="$(get port 3000)"
LOG_LEVEL="$(get log_level info)"

export SIGNALK_NODE_CONFIG_DIR="${CONFIG_DIR}"
export PORT
export DEBUG="${DEBUG:-}"

case "${LOG_LEVEL}" in
  debug) export DEBUG="signalk-server:*,@canboat/*" ;;
  *) : ;;
esac

echo "[signalk] config dir: ${CONFIG_DIR}"
echo "[signalk] listening on port ${PORT} (host network)"

# can0 lives in the host network namespace, which host_network: true gives us.
# Report what we can see so a missing interface is obvious in the log rather
# than surfacing later as a silent absence of data.
if [ -d /sys/class/net/can0 ]; then
  STATE="$(cat /sys/class/net/can0/operstate 2>/dev/null || echo unknown)"
  RX="$(cat /sys/class/net/can0/statistics/rx_packets 2>/dev/null || echo '?')"
  echo "[signalk] can0 present (operstate=${STATE}, rx_packets=${RX})"
  if [ "${RX}" = "0" ]; then
    echo "[signalk] NOTE: can0 has received 0 frames -- is the NMEA 2000 backbone connected and powered?"
  fi
else
  echo "[signalk] WARNING: can0 not found. NMEA 2000 input will not work."
  echo "[signalk] Check the haos_configurator overlays and that host_network is enabled."
fi

# Hand off to the image's own startup script as `node`, the upstream default
# user: it sets IS_IN_DOCKER and execs signalk-server --securityenabled. Its
# attempt to start dbus/avahi (for mDNS) fails harmlessly on this Alpine image
# -- see the README's log-noise note -- without affecting the server or decode.
exec su-exec node /home/node/signalk/startup.sh
