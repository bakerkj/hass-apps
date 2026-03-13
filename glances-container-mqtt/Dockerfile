ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3 \
  py3-paho-mqtt \
  py3-requests \
  bash

COPY run.sh /run.sh
COPY glances_container_mqtt.py /glances_container_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
