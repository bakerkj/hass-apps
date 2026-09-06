# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Container Info MQTT" \
    org.opencontainers.image.description="Collects Docker container stats and config limits (cpuset, CPU shares, blkio weight) and publishes Home Assistant MQTT Discovery sensors." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  docker-cli=29.5.3-r1 \
  py3-paho-mqtt=1.6.1-r4 \
  python3=3.14.7-r1

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY container_info_mqtt /opt/container_info_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/container_info_mqtt

CMD ["/run.sh"]
