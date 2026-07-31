# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

# nginx stops promptly; shorten s6's teardown grace from the 3000ms default.
ENV S6_KILL_GRACETIME=500

LABEL \
    org.opencontainers.image.title="Dashboard Web Proxy" \
    org.opencontainers.image.description="Reverse-proxies LAN device web UIs for embedding in Home Assistant dashboards." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  nginx=1.30.4-r1 \
  ca-certificates=20260611-r0

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
