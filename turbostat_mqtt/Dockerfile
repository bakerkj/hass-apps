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
      cpupower=7.1.5-r0 \
      linux-tools=7.1.5-r0 \
      python3=3.14.7-r1 \
      util-linux=2.42.3-r1 \
      uv=0.11.19-r0

# aiomqtt is not packaged in any Alpine branch, so it comes in via uv. It needs
# paho 2.x, which is likewise newer than the py3-paho-mqtt apk.
RUN --mount=type=cache,target=/root/.cache/uv uv pip install \
        --system --no-cache --break-system-packages \
        aiomqtt==2.5.1 \
        paho-mqtt==2.1.0

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh

COPY turbostat_mqtt /opt/turbostat_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/turbostat_mqtt

CMD ["/run.sh"]
