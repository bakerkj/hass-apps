ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

LABEL \
    org.opencontainers.image.title="Container Info MQTT" \
    org.opencontainers.image.description="Collects Docker container stats and config limits (cpuset, CPU shares, blkio weight) and publishes Home Assistant MQTT Discovery sensors." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache \
  python3=3.12.13-r0 \
  py3-paho-mqtt=1.6.1-r3 \
  docker-cli=29.1.3-r4

COPY run.sh /run.sh
COPY container_info_mqtt.py /container_info_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
