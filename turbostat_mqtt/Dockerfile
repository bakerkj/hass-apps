ARG BUILD_FROM
FROM ${BUILD_FROM}

# turbostat is part of linux-tools on Alpine
RUN apk add --no-cache \
      python3 \
      linux-tools \
      util-linux \
      py3-paho-mqtt \
      cpupower

COPY run.sh /run.sh

COPY turbostat_mqtt.py /turbostat_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
