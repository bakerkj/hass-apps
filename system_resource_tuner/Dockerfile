ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="System Resource Tuner" \
    org.opencontainers.image.description="Applies system resource tuning for containers (cpuset, CPU shares priority, blkio weight) and keeps it reconciled." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache \
  python3=3.12.13-r0 \
  docker-cli=29.1.3-r4

COPY run.sh /run.sh
COPY system_resource_tuner /opt/system_resource_tuner

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/system_resource_tuner

ENTRYPOINT ["/run.sh"]
