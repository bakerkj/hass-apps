# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="System Resource Tuner" \
    org.opencontainers.image.description="Applies system resource tuning for containers (cpuset, CPU shares priority, blkio weight) and keeps it reconciled." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  docker-cli=29.5.3-r0 \
  python3=3.14.5-r0

COPY run.sh /run.sh
COPY system_resource_tuner /opt/system_resource_tuner

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/system_resource_tuner

ENTRYPOINT ["/run.sh"]
