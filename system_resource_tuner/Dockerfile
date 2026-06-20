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
  python3=3.14.5-r0 \
  py3-pip=26.1.2-r0 \
  uv=0.11.19-r0
RUN --mount=type=cache,target=/root/.cache/uv \
  uv pip install --system --break-system-packages \
    aiodocker==0.27.0

COPY run.sh /run.sh
COPY system_resource_tuner /opt/system_resource_tuner

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/system_resource_tuner

ENTRYPOINT ["/run.sh"]
