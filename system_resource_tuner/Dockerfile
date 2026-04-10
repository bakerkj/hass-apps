ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3=3.12.13-r0 \
  docker-cli=29.1.3-r3

COPY run.sh /run.sh
COPY system_resource_tuner.py /system_resource_tuner.py

RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]
