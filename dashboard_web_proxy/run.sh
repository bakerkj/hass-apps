#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
set -euo pipefail

OPTIONS="/data/options.json"
CONF="/etc/nginx/http.d/proxy.conf"

if [[ ! -f "${OPTIONS}" ]]; then
  bashio::log.fatal "Options file not found: ${OPTIONS}"
  bashio::exit.nok
fi

# Apply the configured log level to bashio's own logging and, below, to nginx.
LOG_LEVEL="$(jq -r '.log_level // "info"' "${OPTIONS}")"
bashio::log.level "${LOG_LEVEL}"
case "${LOG_LEVEL}" in
  trace | debug) NGINX_LEVEL="debug" ;;
  info) NGINX_LEVEL="info" ;;
  notice) NGINX_LEVEL="notice" ;;
  warning) NGINX_LEVEL="warn" ;;
  error) NGINX_LEVEL="error" ;;
  fatal) NGINX_LEVEL="crit" ;;
  *) NGINX_LEVEL="info" ;;
esac
# Ports statically published in config.json; a site must use one of these or it
# would bind inside the container but be unreachable from the host/Tailscale.
ALLOWED_PORTS="18800 18801 18802 18803 18804 18805 18806 18807 18808 18809 18810 18811 18812 18813 18814 18815 18816 18817 18818 18819"

mkdir -p /run/nginx /var/log/nginx
# Drop the stock server that listens on :80; we only serve the configured ports.
rm -f /etc/nginx/http.d/default.conf

# error_log (needs ${NGINX_LEVEL} expanded) + WebSocket upgrade map. Both are
# http-context directives; this file is included inside http{}.
printf 'error_log /dev/stderr %s;\n' "${NGINX_LEVEL}" >"${CONF}"
cat >>"${CONF}" <<'EOF'
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
EOF

count="$(jq '.sites | length' "${OPTIONS}")"
if [[ "${count}" -eq 0 ]]; then
  bashio::log.warning "No sites configured; nothing to proxy."
fi

used_ports=""
for ((i = 0; i < count; i++)); do
  name="$(jq -r ".sites[${i}].name" "${OPTIONS}")"
  upstream="$(jq -r ".sites[${i}].upstream" "${OPTIONS}")"
  uport="$(jq -r ".sites[${i}].upstream_port // 80" "${OPTIONS}")"
  lport="$(jq -r ".sites[${i}].listen_port" "${OPTIONS}")"

  # Reject anything that isn't a bare host/IP -- prevents config injection into
  # the generated nginx blocks.
  if ! [[ "${upstream}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    bashio::log.fatal "site '${name}': invalid upstream '${upstream}' (host/IP only)"
    bashio::exit.nok
  fi
  # upstream_port is also interpolated into the nginx block; keep it numeric so a
  # crafted value can't inject config even if it bypasses the Supervisor schema.
  if ! [[ "${uport}" =~ ^[0-9]+$ ]] || [[ "${uport}" -lt 1 ]] || [[ "${uport}" -gt 65535 ]]; then
    bashio::log.fatal "site '${name}': invalid upstream_port '${uport}' (1-65535)"
    bashio::exit.nok
  fi
  if [[ " ${ALLOWED_PORTS} " != *" ${lport} "* ]]; then
    bashio::log.fatal "site '${name}': listen_port ${lport} must be one of: ${ALLOWED_PORTS}"
    bashio::exit.nok
  fi
  if [[ " ${used_ports} " == *" ${lport} "* ]]; then
    bashio::log.fatal "site '${name}': listen_port ${lport} already used by another site"
    bashio::exit.nok
  fi
  used_ports="${used_ports} ${lport}"

  bashio::log.info "Proxying '${name}': :${lport} -> ${upstream}:${uport}"

  # Relay to the device and, on the way back, make the page embeddable:
  #   - drop X-Frame-Options / CSP so HA can iframe it
  #   - rewrite the session-cookie domain so login sticks through the proxy host
  #   - rewrite absolute redirects back to the proxy host
  cat >>"${CONF}" <<EOF
server {
    listen ${lport};
    server_name _;

    location / {
        proxy_pass http://${upstream}:${uport};
        proxy_set_header Host ${upstream};
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \$connection_upgrade;

        proxy_hide_header X-Frame-Options;
        proxy_hide_header Content-Security-Policy;
        proxy_cookie_domain ${upstream} \$host;
        proxy_redirect http://${upstream}:${uport}/ /;
        proxy_redirect http://${upstream}/ /;

        proxy_buffering off;
    }
}
EOF
done

nginx -t
bashio::log.info "Starting nginx"
exec nginx -g 'daemon off;'
