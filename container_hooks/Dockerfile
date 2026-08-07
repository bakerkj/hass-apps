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
  docker-cli=29.5.3-r0 \
  python3=3.14.5-r0 \
  py3-pip=26.1.2-r0 \
  uv=0.11.19-r0
RUN --mount=type=cache,target=/root/.cache/uv \
  uv pip install --system --break-system-packages \
    aiodocker==0.27.0 \
    patch-ng==1.19.1

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY container_hooks /opt/container_hooks

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/container_hooks

ENTRYPOINT ["/run.sh"]
