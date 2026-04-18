ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

LABEL \
    org.opencontainers.image.title="Turbostat to MQTT" \
    org.opencontainers.image.description="Runs turbostat and publishes to MQTT." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# turbostat is part of linux-tools on Alpine
RUN apk add --no-cache \
      python3=3.12.13-r0 \
      linux-tools=6.19.8-r0 \
      util-linux=2.41.4-r0 \
      py3-paho-mqtt=1.6.1-r3 \
      cpupower=6.19.8-r0

COPY run.sh /run.sh

COPY turbostat_mqtt.py /turbostat_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
