ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3 \
  docker-cli \
  bash

COPY run.sh /run.sh
COPY system_resource_tuner.py /system_resource_tuner.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
