ARG BUILD_FROM
FROM ${BUILD_FROM}

# turbostat is part of linux-tools on Alpine
RUN apk add --no-cache \
      python3 py3-pip \
      linux-tools \
      util-linux \
    && pip3 install --no-cache-dir paho-mqtt

COPY run.sh /run.sh

COPY turbostat_mqtt.py /turbostat_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
