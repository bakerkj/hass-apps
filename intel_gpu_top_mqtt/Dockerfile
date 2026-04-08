ARG BUILD_FROM
FROM ${BUILD_FROM}


RUN apk add --no-cache \
  python3=3.12.12-r0 \
  py3-paho-mqtt=1.6.1-r3

# igt-gpu-tools provides intel_gpu_top (Alpine edge/testing repo)
RUN echo "https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
  apk add --no-cache --update \
  igt-gpu-tools=2.3-r0

COPY run.sh /run.sh
COPY intel_gpu_mqtt.py /intel_gpu_mqtt.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
