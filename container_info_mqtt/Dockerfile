ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.23
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3=3.12.13-r0 \
  py3-paho-mqtt=1.6.1-r3 \
  docker-cli=29.1.3-r4

COPY run.sh /run.sh
COPY container_info_mqtt.py /container_info_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
