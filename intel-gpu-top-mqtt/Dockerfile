ARG BUILD_FROM
FROM ${BUILD_FROM}

# Home Assistant app base images are Alpine-based.
# intel-gpu-tools provides intel_gpu_top
RUN apk add --no-cache \
  igt-gpu-tools \
  python3 \
  py3-pip \
  bash \
  jq

RUN pip3 install --no-cache-dir paho-mqtt

COPY run.sh /run.sh
COPY intel_gpu_mqtt.py /intel_gpu_mqtt.py

RUN chmod +x /run.sh

CMD [ "/run.sh" ]
