# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Container Hooks" \
    org.opencontainers.image.description="Per-container docker hooks: stage files, apply unified-diff patches, or run scripts at container create or start events — without forking the target image." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  docker-cli=29.5.3-r1 \
  python3=3.14.7-r1 \
  py3-pip=26.1.2-r0 \
  uv=0.11.19-r0
# aiomqtt is not packaged in any Alpine branch, so it comes in via uv.
# aiomqtt 2.x wraps paho-mqtt 2.x -- pin both explicitly for reproducibility.
RUN --mount=type=cache,target=/root/.cache/uv \
  uv pip install --system --break-system-packages \
    aiodocker==0.27.0 \
    aiomqtt==2.5.1 \
    paho-mqtt==2.1.0 \
    patch-ng==1.19.1

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY container_hooks /opt/container_hooks

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/container_hooks

ENTRYPOINT ["/run.sh"]
