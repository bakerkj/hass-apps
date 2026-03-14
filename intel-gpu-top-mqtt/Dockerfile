ARG BUILD_FROM
FROM ${BUILD_FROM}


RUN apk update && \
  apk add --no-cache \
  python3 \
  py3-paho-mqtt \
  bash

# Home Assistant app base images are Alpine-based.
# igt-gpu-tools provides intel_gpu_top
RUN echo "https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
  apk update && \
  apk add --no-cache \
  igt-gpu-tools

COPY run.sh /run.sh
COPY intel_gpu_mqtt.py /intel_gpu_mqtt.py

RUN chmod +x /run.sh

CMD [ "/run.sh" ]
