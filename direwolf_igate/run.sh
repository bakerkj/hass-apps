#!/usr/bin/with-contenv bash
# shellcheck shell=bash
set -euo pipefail

source /usr/lib/bashio/bashio.sh

bashio::log.info "Direwolf APRS IGate v${ADDON_VERSION:-dev} starting"

MYCALL="$(bashio::config 'mycall')"
PASSCODE="$(bashio::config 'iglogin_passcode')"
IGSERVER="$(bashio::config 'igserver')"
FREQUENCY="$(bashio::config 'frequency')"
RTL_DEVICE="$(bashio::config 'rtl_device')"
RTL_GAIN="$(bashio::config 'rtl_gain')"
RTL_PPM="$(bashio::config 'rtl_ppm')"
LATITUDE="$(bashio::config 'latitude')"
LONGITUDE="$(bashio::config 'longitude')"
BEACON_COMMENT="$(bashio::config 'beacon_comment')"
BEACON_SYMBOL="$(bashio::config 'beacon_symbol')"
BEACON_OVERLAY="$(bashio::config 'beacon_overlay')"
BEACON_DELAY="$(bashio::config 'beacon_delay')"
BEACON_EVERY="$(bashio::config 'beacon_every')"
SEND_STATUS_BEACON="$(bashio::config 'send_status_beacon')"

# rtl_fm's default demod output rate and direwolf's ARATE are both 24000 Hz;
# both sides must agree or AFSK decode silently fails.
AUDIO_RATE=24000

# Seconds to wait for direwolf/rtl_fm to exit on SIGTERM before SIGKILL.
# Direwolf flushes APRS-IS in well under a second; the HA supervisor's own
# SIGTERM→SIGKILL deadline is ~10 s so longer values get preempted upstream.
GRACE_SECONDS=5

[[ -n "${MYCALL}" && "${MYCALL}" != "null" ]] ||
  bashio::exit.nok "'mycall' is required"
[[ -n "${PASSCODE}" && "${PASSCODE}" != "null" ]] ||
  bashio::exit.nok "'iglogin_passcode' is required"

# Direwolf's config has no escape syntax, so strip anything that could close a
# token or start a new directive (an injected IGTXLIMIT would enable transmit).
for v in MYCALL PASSCODE IGSERVER \
  BEACON_COMMENT BEACON_SYMBOL BEACON_OVERLAY BEACON_DELAY BEACON_EVERY; do
  declare -n ref="${v}"
  ref="${ref//\"/}"
  ref="${ref//\\/}"
  ref="${ref//$'\n'/}"
  ref="${ref//$'\r'/}"
done
unset -n ref

# bashio::config returns the literal "null" for an unset str? option.
[[ "${BEACON_COMMENT}" == "null" ]] && BEACON_COMMENT=""

# Reject Null Island (0,0) — the schema defaults make it the silent fallback
# on a fresh install, which would broadcast a bogus position into APRS-IS.
# Numeric compare via awk so 0, 0.0, -0, -0.0, 0e0 all collapse to zero.
if awk -v lat="${LATITUDE}" -v lon="${LONGITUDE}" \
  'BEGIN { exit !(lat == 0 && lon == 0) }'; then
  bashio::exit.nok \
    "'latitude' and 'longitude' must be set to your station coordinates"
fi

# Derived from the callsign. -1 authenticates a read-only feed and cannot
# inject, so an IGate needs a real one or the beacon is dropped by the server.
# Trim paste artifacts and reject anything else so a malformed value can't
# silently break the IGLOGIN line.
PASSCODE="${PASSCODE//[[:space:]]/}"
[[ "${PASSCODE}" =~ ^-?[0-9]+$ ]] ||
  bashio::exit.nok \
    "'iglogin_passcode' must be the numeric APRS-IS passcode for your callsign"

# Direwolf takes MYCALL as a bare token without validating it, so stray text
# renders a malformed MYCALL/IGLOGIN pair and fails at login obscurely.
# Uppercased because direwolf emits it verbatim and APRS-IS wants upper case.
MYCALL="${MYCALL//[[:space:]]/}"
MYCALL="${MYCALL^^}"
[[ "${MYCALL}" =~ ^[A-Z0-9]{1,6}(-[0-9]{1,2})?$ ]] ||
  bashio::exit.nok "'mycall' must be a callsign with an optional SSID (e.g. N0CALL-10)"

# Unquoted on the PBEACON line, so a space appends further keywords -- enough to
# override lat=/long= and slip past the Null Island guard above.
[[ "${BEACON_DELAY}" =~ ^[0-9]{1,3}(:[0-9]{2})?$ ]] ||
  bashio::exit.nok "'beacon_delay' must be mm:ss or a whole number of minutes"
[[ "${BEACON_EVERY}" =~ ^[0-9]{1,3}(:[0-9]{2})?$ ]] ||
  bashio::exit.nok "'beacon_every' must be mm:ss or a whole number of minutes"
[[ "${BEACON_OVERLAY}" =~ ^[A-Z0-9]$ ]] ||
  bashio::exit.nok "'beacon_overlay' must be a single character A-Z or 0-9"

# Shape-checked so a typo fails at startup rather than as a direwolf warning.
# Printable, no space -- a space would append further PBEACON keywords. Not
# alphanumeric: most symbol codes are a table char plus punctuation ("I&",
# "/#"). [[:graph:]] not a range like [!-~], which follows locale collation.
[[ "${BEACON_SYMBOL}" =~ ^[[:graph:]]{1,20}$ ]] ||
  bashio::exit.nok "'beacon_symbol' must be a symbol name or code (e.g. igate)"
[[ "${IGSERVER}" =~ ^[A-Za-z0-9.-]+(:[0-9]{1,5})?$ ]] ||
  bashio::exit.nok "'igserver' must be a hostname, optionally with :port"

# Hand the Supervisor's broker to the publisher when one is offered, so a user
# running the Mosquitto add-on needs no credentials of their own. Explicit
# options still win; see config.py.
if bashio::services.available "mqtt"; then
  export SVC_MQTT_HOST SVC_MQTT_PORT SVC_MQTT_USERNAME SVC_MQTT_PASSWORD
  SVC_MQTT_HOST="$(bashio::services mqtt 'host')"
  SVC_MQTT_PORT="$(bashio::services mqtt 'port')"
  SVC_MQTT_USERNAME="$(bashio::services mqtt 'username')"
  SVC_MQTT_PASSWORD="$(bashio::services mqtt 'password')"
  bashio::log.info "Using the Supervisor-provided MQTT broker at ${SVC_MQTT_HOST}"
fi

# Not /tmp: that is 1777, so the unprivileged direwolf account could pre-create
# sdr.conf as a symlink for root's `>` and chmod to follow. 0750 root:direwolf
# gives that account traverse and read but not write, so it cannot plant one.
CONF_DIR="/run/direwolf"
install -d -m 750 -o root -g direwolf "${CONF_DIR}"
CONF_PATH="${CONF_DIR}/sdr.conf"

{
  echo "ADEVICE null null"
  echo "CHANNEL 0"
  echo "ARATE ${AUDIO_RATE}"
  echo
  # Both listen by default and accept frames to transmit; add-ons share the
  # Supervisor network. Close them on a receive-only gate.
  echo "AGWPORT 0"
  echo "KISSPORT 0"
  echo
  echo "MYCALL ${MYCALL}"
  echo
  echo "IGSERVER ${IGSERVER}"
  echo "IGLOGIN ${MYCALL} ${PASSCODE}"
  echo
  pbeacon="PBEACON sendto=IG delay=${BEACON_DELAY} every=${BEACON_EVERY}"
  pbeacon+=" symbol=\"${BEACON_SYMBOL}\" overlay=${BEACON_OVERLAY}"
  pbeacon+=" lat=${LATITUDE} long=${LONGITUDE}"
  [[ -n "${BEACON_COMMENT}" ]] && pbeacon+=" COMMENT=\"${BEACON_COMMENT}\""
  echo "${pbeacon}"
  if [[ "${SEND_STATUS_BEACON}" == "true" ]]; then
    # sendto=IG is direwolf's default for IBEACON, but every= is not -- it would
    # fall back to 600s. State both so the status beacon, which carries the
    # counters, follows the same schedule as the position beacon.
    echo "IBEACON sendto=IG every=${BEACON_EVERY}"
  fi
} >"${CONF_PATH}"

# Ahead of the log line: an inline substitution masks its exit status (SC2312).
if [[ -n "${PASSCODE}" ]]; then
  PASSCODE_STATE="(set)"
else
  PASSCODE_STATE="(none)"
fi

bashio::log.info "Configuration:
  mycall:             ${MYCALL}
  igserver:           ${IGSERVER}
  frequency:          ${FREQUENCY}
  rtl_device:         ${RTL_DEVICE}
  rtl_gain:           ${RTL_GAIN:-(auto)}
  rtl_ppm:            ${RTL_PPM}
  latitude:           ${LATITUDE}
  longitude:          ${LONGITUDE}
  beacon_comment:     ${BEACON_COMMENT:-(none)}
  beacon_symbol:      ${BEACON_SYMBOL}
  beacon_overlay:     ${BEACON_OVERLAY}
  beacon_delay:       ${BEACON_DELAY}
  beacon_every:       ${BEACON_EVERY}
  send_status_beacon: ${SEND_STATUS_BEACON}
  passcode:           ${PASSCODE_STATE}"

# sdr.conf holds the passcode in clear text; give it to direwolf and nobody else.
chown direwolf:direwolf "${CONF_PATH}"
chmod 400 "${CONF_PATH}"

bashio::log.info "Rendered sdr.conf:"
# `[^[:space:]]`, not `\S`: the latter is a GNU extension, and a silent
# non-match here would print the passcode to the log in clear.
sed -E "s/^(IGLOGIN[[:space:]]+[^[:space:]]+[[:space:]]+).*/\1[redacted]/" \
  "${CONF_PATH}" >&2

RTL_ARGS=(-d "${RTL_DEVICE}" -f "${FREQUENCY}" -p "${RTL_PPM}")
if [[ -n "${RTL_GAIN}" && "${RTL_GAIN}" != "null" ]]; then
  RTL_ARGS+=(-g "${RTL_GAIN}")
fi
RTL_ARGS+=(-)

# In CONF_DIR, not /tmp: that is 1777, so a name there can be raced. Nothing
# but root can create in CONF_DIR, and the shell opens this before dropping
# privilege, so direwolf needs no access of its own.
PIPE="${CONF_DIR}/audio.fifo"
rm -f "${PIPE}"
mkfifo -m 600 "${PIPE}"
trap 'rm -f "${PIPE}"' EXIT

RTL=""
DW=""

# `jobs -p` rather than $RTL/$DW: correct in the µs window between `&` and the
# `$!` assignment. SC2329 is a false positive -- the `trap` below invokes this.
# shellcheck disable=SC2329
cleanup() {
  bashio::log.info "Shutdown requested; terminating children"
  # shellcheck disable=SC2046
  kill -TERM $(jobs -p) 2>/dev/null || true
  local deadline=$((SECONDS + GRACE_SECONDS))
  local running
  while ((SECONDS < deadline)); do
    # Captured first so the substitution's status is not masked (SC2312).
    running="$(jobs -rp)"
    if [[ -z "${running}" ]]; then
      exit 0
    fi
    sleep 1
  done
  bashio::log.warning "Children did not exit within ${GRACE_SECONDS}s; sending KILL"
  # shellcheck disable=SC2046
  kill -KILL $(jobs -p) 2>/dev/null || true
  exit 0
}
trap cleanup TERM INT

# Piped through the stats publisher, which tees every line back to stdout. $!
# after a pipeline is its LAST process, so DW is the publisher -- still a valid
# sentinel, since the two die together (EOF one way, SIGPIPE the other).
bashio::log.info "Starting direwolf"
su-exec direwolf:direwolf /usr/bin/direwolf -t 0 -c "${CONF_PATH}" - <"${PIPE}" 2>&1 |
  PYTHONPATH=/opt python3 -m direwolf_igate \
    --options /data/options.json --mycall "${MYCALL}" &
DW=$!
# direwolf's own pid, needed because the teardown must signal it directly --
# otherwise it dies only on a delayed SIGPIPE, or never if it is hung.
DIREWOLF_PID=$(jobs -p %%)
# Every kill below is `|| true`, so an empty pid would silently no-op.
[[ -n "${DIREWOLF_PID}" ]] ||
  bashio::log.warning "could not determine direwolf's pid; shutdown may be slow"

bashio::log.info "Starting rtl_fm ${RTL_ARGS[*]}"
/usr/bin/rtl_fm "${RTL_ARGS[@]}" >"${PIPE}" </dev/null &
RTL=$!

wait -n "${RTL}" "${DW}" || true

if ! kill -0 "${RTL}" 2>/dev/null; then
  bashio::log.warning "rtl_fm exited; terminating direwolf"
else
  bashio::log.warning "direwolf exited; terminating rtl_fm"
fi

# direwolf included: signalling it gives the publisher EOF, which is what runs
# its shutdown path and publishes the retained "offline".
kill -TERM "${RTL}" "${DIREWOLF_PID}" "${DW}" 2>/dev/null || true
# Only wait if something is still up, rather than adding a flat GRACE_SECONDS to
# every restart.
for _ in $(seq "${GRACE_SECONDS}"); do
  # Captured first so the substitution's status is not masked (SC2312).
  remaining="$(jobs -rp)"
  [[ -n "${remaining}" ]] || break
  sleep 1
done
kill -KILL "${RTL}" "${DIREWOLF_PID}" "${DW}" 2>/dev/null || true
wait "${RTL}" 2>/dev/null || true
wait "${DW}" 2>/dev/null || true

bashio::log.warning "Exiting with non-zero status to trigger supervisor restart"
exit 1
