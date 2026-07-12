# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Turbostat to MQTT" \
    org.opencontainers.image.description="Runs turbostat and publishes to MQTT." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# turbostat is part of linux-tools on Alpine
RUN --mount=type=cache,target=/var/cache/apk apk add \
      cpupower=7.1.3-r0 \
      linux-tools=7.1.3-r0 \
      py3-paho-mqtt=1.6.1-r4 \
      python3=3.14.5-r0 \
      util-linux=2.42.1-r0

COPY run.sh /run.sh

COPY turbostat_mqtt /opt/turbostat_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/turbostat_mqtt

CMD ["/run.sh"]
