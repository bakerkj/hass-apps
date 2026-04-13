ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.23
FROM ${BUILD_FROM}

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
COPY intel_gpu_mqtt.py /intel_gpu_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
