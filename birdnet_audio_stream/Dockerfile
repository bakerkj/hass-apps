# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="BirdNET Audio Stream" \
    org.opencontainers.image.description="Stream ALSA microphone audio over RTSP for BirdNET using go2rtc" \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk set -eux; \
    apk add \
        alsa-lib=1.2.15.3-r0 \
        alsa-plugins-pulse=1.2.12-r2 \
        alsa-utils=1.2.15.2-r1 \
        ffmpeg=8.1.2-r0 \
        go2rtc=1.9.14-r4

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
