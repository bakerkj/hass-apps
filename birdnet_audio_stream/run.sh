#!/usr/bin/with-contenv bash
# shellcheck shell=bash
set -euo pipefail

source /usr/lib/bashio/bashio.sh

bashio::log.info "BirdNET Audio Stream v${ADDON_VERSION:-dev} starting"

CONFIG_PATH="/tmp/go2rtc.yaml"
STREAM_NAME="$(bashio::config 'stream_name')"
AUDIO_DEVICE="$(bashio::config 'alsa_device')"
SAMPLE_RATE="$(bashio::config 'sample_rate')"
CHANNELS="$(bashio::config 'channels')"
CODEC="$(bashio::config 'codec')"
BITRATE_RAW="$(bashio::config 'bitrate')"
INPUT_FORMAT="$(bashio::config 'input_format')"
GO2RTC_LOG_LEVEL="$(bashio::config 'go2rtc_log_level')"
FFMPEG_LOG_LEVEL="$(bashio::config 'ffmpeg_log_level')"
FFMPEG_VOLUME="$(bashio::config 'ffmpeg_volume')"
MIXER_CONTROL="$(bashio::config 'mixer_control')"
MIXER_VOLUME="$(bashio::config 'mixer_volume')"
RTSP_PORT="8554"
API_PORT="1984"
USERNAME="$(bashio::config 'username')"
PASSWORD="$(bashio::config 'password')"

if [[ -z "${BITRATE_RAW}" || "${BITRATE_RAW}" == "null" ]]; then
  case "${CODEC}" in
    opus) BITRATE="96k" ;;
    *) BITRATE="" ;;
  esac
else
  BITRATE="${BITRATE_RAW}"
fi

if [[ -n "${MIXER_CONTROL}" && -n "${MIXER_VOLUME}" ]]; then
  bashio::log.info "Setting mixer card control ${MIXER_CONTROL} to ${MIXER_VOLUME}"
  amixer sset "${MIXER_CONTROL}" "${MIXER_VOLUME}" || bashio::log.warning "Failed to set mixer control ${MIXER_CONTROL}"
fi

case "${INPUT_FORMAT}" in
  alsa)
    INPUT_TEMPLATE="-hide_banner -loglevel ${FFMPEG_LOG_LEVEL} -fflags nobuffer -flags low_delay -thread_queue_size 512 -f alsa -channels ${CHANNELS} -sample_rate ${SAMPLE_RATE} -i {input} -vn -af volume=${FFMPEG_VOLUME} -ac ${CHANNELS} -ar ${SAMPLE_RATE}"
    ;;
  pulse)
    INPUT_TEMPLATE="-hide_banner -loglevel ${FFMPEG_LOG_LEVEL} -fflags nobuffer -flags low_delay -thread_queue_size 512 -f pulse -channels ${CHANNELS} -sample_rate ${SAMPLE_RATE} -i {input} -vn -af volume=${FFMPEG_VOLUME} -ac ${CHANNELS} -ar ${SAMPLE_RATE}"
    ;;
  *)
    bashio::log.fatal "Unsupported input_format: ${INPUT_FORMAT}"
    exit 1
    ;;
esac

case "${CODEC}" in
  opus)
    STREAM_SPEC="ffmpeg:${AUDIO_DEVICE}#input=birdalsa#audio=opus"
    if [[ -n "${BITRATE}" ]]; then
      STREAM_SPEC="${STREAM_SPEC}#audio_bitrate=${BITRATE}"
    fi
    ;;
  flac)
    STREAM_SPEC="ffmpeg:${AUDIO_DEVICE}#input=birdalsa#audio=flac"
    ;;
  pcm)
    STREAM_SPEC="ffmpeg:${AUDIO_DEVICE}#input=birdalsa#audio=copy"
    ;;
  *)
    bashio::log.fatal "Unsupported codec: ${CODEC}"
    exit 1
    ;;
esac

{
  echo "log:"
  echo "  level: ${GO2RTC_LOG_LEVEL}"
  echo "api:"
  echo "  listen: :${API_PORT}"
  if [[ -n "${USERNAME}" && -n "${PASSWORD}" ]]; then
    echo "  username: '${USERNAME}'"
    echo "  password: '${PASSWORD}'"
  fi
  echo "rtsp:"
  echo "  listen: :${RTSP_PORT}"
  echo "ffmpeg:"
  echo "  birdalsa: ${INPUT_TEMPLATE}"
  echo "streams:"
  echo "  ${STREAM_NAME}: '${STREAM_SPEC}'"
} >"${CONFIG_PATH}"

bashio::log.info "Configuration:
  alsa_device:         ${AUDIO_DEVICE}
  api_port:            ${API_PORT}
  bitrate:             ${BITRATE:-(none)}
  channels:            ${CHANNELS}
  codec:               ${CODEC}
  ffmpeg_log_level:    ${FFMPEG_LOG_LEVEL}
  ffmpeg_volume:       ${FFMPEG_VOLUME}
  go2rtc_log_level:    ${GO2RTC_LOG_LEVEL}
  input_format:        ${INPUT_FORMAT}
  mixer_control:       ${MIXER_CONTROL:-(none)}
  mixer_volume:        ${MIXER_VOLUME:-(none)}
  password:            $([ -n "${PASSWORD}" ] && echo '(set)' || echo '(none)')
  rtsp_port:           ${RTSP_PORT}
  rtsp_url:            rtsp://<host>:${RTSP_PORT}/${STREAM_NAME}
  sample_rate:         ${SAMPLE_RATE} Hz
  stream_name:         ${STREAM_NAME}
  username:            ${USERNAME:-(none)}"
bashio::log.info "Generated go2rtc config:"
sed 's/password: .*/password: [redacted]/' "${CONFIG_PATH}" >&2
bashio::log.info "Starting go2rtc"

exec /usr/bin/go2rtc -config "${CONFIG_PATH}"
