# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Frigate Compressor" \
    org.opencontainers.image.description="Compresses old Frigate NVR recordings using Intel QSV (or NVENC/CPU fallback). Two-tier compression based on age." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Two apk calls in one RUN: the first uses Alpine 3.23 stable repos; the
# second pulls onevpl-intel-gpu from Alpine edge because Alpine 3.23 stable
# only ships the libvpl dispatcher, not its runtime backend (libmfx-gen).
# Without the runtime, h264_qsv fails.  The package has moved between edge
# subrepos (testing -> community) before, so list both; --repository scopes
# the edge repos to the second apk call only, so future stable updates
# can't accidentally come from edge.
RUN --mount=type=cache,target=/var/cache/apk apk add \
      ffmpeg=8.1.1-r0 \
      intel-media-driver=26.2.1-r0 \
      libva=2.23.0-r0 \
      libva-utils=2.23.0-r0 \
      mesa-va-gallium=26.1.1-r1 \
      py3-paho-mqtt=1.6.1-r4 \
      py3-yaml=6.0.3-r2 \
      python3=3.14.5-r0 \
      sqlite=3.53.2-r0 \
 && apk add \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing \
      onevpl-intel-gpu=25.4.6-r1

COPY run.sh /run.sh
COPY frigate_compressor /opt/frigate_compressor

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/frigate_compressor

CMD ["/run.sh"]
