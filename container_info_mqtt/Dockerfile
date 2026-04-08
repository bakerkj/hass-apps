ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3=3.12.12-r0 \
  py3-paho-mqtt=1.6.1-r3 \
  docker-cli=29.1.3-r3

COPY run.sh /run.sh
COPY container_info_mqtt.py /container_info_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
