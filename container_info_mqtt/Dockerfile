ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

LABEL \
    org.opencontainers.image.title="Container Info MQTT" \
    org.opencontainers.image.description="Collects Docker container stats and config limits (cpuset, CPU shares, blkio weight) and publishes Home Assistant MQTT Discovery sensors." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache \
  docker-cli=29.1.3-r4 \
  py3-paho-mqtt=1.6.1-r3 \
  python3=3.12.13-r0

COPY run.sh /run.sh
COPY container_info_mqtt /opt/container_info_mqtt

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/container_info_mqtt

CMD ["/run.sh"]
