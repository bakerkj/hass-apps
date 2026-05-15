# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
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
        alsa-lib=1.2.14-r2 \
        alsa-plugins-pulse=1.2.12-r2 \
        alsa-utils=1.2.14-r4 \
        ffmpeg=8.0.1-r1 \
        go2rtc=1.9.9-r12

COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
