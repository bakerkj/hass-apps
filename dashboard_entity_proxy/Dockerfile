# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
  org.opencontainers.image.title="Dashboard Entity Proxy" \
  org.opencontainers.image.description="Filters Home Assistant WebSocket entity subscriptions per dashboard." \
  org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
  org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  nginx=1.30.2-r1 \
  nginx-mod-http-brotli=1.30.2-r1 \
  nginx-mod-http-zstd=1.30.2-r1 \
  python3=3.14.5-r0 \
  py3-aiohttp=3.13.5-r0 \
  py3-humanfriendly=10.0-r7 \
  py3-voluptuous=0.15.2-r1 \
  py3-yaml=6.0.3-r2

COPY rootfs/ /
COPY dashboard_entity_proxy /opt/dashboard_entity_proxy
# Ship the customization example alongside the package so operators can
# ``cat /opt/customization.example.yaml`` inside the container without
# rummaging through the source tree.
COPY customization.example.yaml /opt/customization.example.yaml

RUN python3 -m compileall -q /opt/dashboard_entity_proxy
