# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Direwolf APRS IGate" \
    org.opencontainers.image.description="Receive-only APRS IGate using Direwolf with an RTL-SDR dongle." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk set -eux; \
    apk add \
        direwolf=1.8.1-r1 \
        py3-paho-mqtt=1.6.1-r4 \
        python3=3.14.5-r0 \
        rtl-sdr=2.0.2-r1 \
        su-exec=0.3-r0

# direwolf needs no privilege: it reads a pipe and opens an outbound socket.
# Only it drops -- rtl_fm still needs /dev/bus/usb.
RUN addgroup -S direwolf && adduser -S -D -H -G direwolf direwolf

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY direwolf_igate /opt/direwolf_igate
RUN chmod +x /run.sh

CMD ["/run.sh"]
