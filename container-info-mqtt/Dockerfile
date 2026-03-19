ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3 \
  py3-paho-mqtt \
  docker-cli \
  bash

COPY run.sh /run.sh
COPY container_info_mqtt.py /container_info_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
