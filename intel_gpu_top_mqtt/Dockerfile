# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Intel GPU Top MQTT (Discovery)" \
    org.opencontainers.image.description="Runs intel_gpu_top and publishes Intel GPU metrics to MQTT (separate topics per sensor) with Home Assistant discovery + attributes." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Two apk calls in one RUN: the first uses Alpine 3.24 stable repos; the
# second pulls igt-gpu-tools (provides intel_gpu_top) from Alpine
# edge/testing because Alpine 3.24 stable doesn't package it.
# --repository scopes the alternate repo to the second apk call only, so
# future stable updates can't accidentally come from edge.
RUN --mount=type=cache,target=/var/cache/apk apk add \
      python3=3.14.7-r1 \
      uv=0.11.19-r0 \
 && apk add \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing \
      igt-gpu-tools=2.3-r0

# aiomqtt is not packaged in any Alpine branch, so it comes in via uv. It needs
# paho 2.x, which is likewise newer than the py3-paho-mqtt apk.
RUN --mount=type=cache,target=/root/.cache/uv uv pip install \
        --system --no-cache --break-system-packages \
        aiomqtt==2.5.1 \
        paho-mqtt==2.1.0

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY intel_gpu_mqtt /opt/intel_gpu_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/intel_gpu_mqtt

CMD ["/run.sh"]
