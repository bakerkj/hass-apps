ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Intel GPU Top MQTT (Discovery)" \
    org.opencontainers.image.description="Runs intel_gpu_top and publishes Intel GPU metrics to MQTT (separate topics per sensor) with Home Assistant discovery + attributes." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Two apk calls in one RUN: the first uses Alpine 3.23 stable repos; the
# second pulls igt-gpu-tools (provides intel_gpu_top) from Alpine
# edge/testing because Alpine 3.23 stable doesn't package it.
# --repository scopes the alternate repo to the second apk call only, so
# future stable updates can't accidentally come from edge.
RUN apk add --no-cache \
      python3=3.12.13-r0 \
      py3-paho-mqtt=1.6.1-r3 \
 && apk add --no-cache \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing \
      igt-gpu-tools=2.3-r0

COPY run.sh /run.sh
COPY intel_gpu_mqtt /opt/intel_gpu_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/intel_gpu_mqtt

CMD ["/run.sh"]
