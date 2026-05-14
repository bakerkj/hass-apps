ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
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
RUN apk add --no-cache \
      ffmpeg=8.0.1-r1 \
      intel-media-driver=25.4.6-r0 \
      libva=2.22.0-r1 \
      libva-utils=2.22.0-r0 \
      mesa-va-gallium=25.2.7-r1 \
      py3-paho-mqtt=1.6.1-r3 \
      py3-yaml=6.0.3-r0 \
      python3=3.12.13-r0 \
      sqlite=3.51.2-r0 \
 && apk add --no-cache \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing \
      onevpl-intel-gpu=25.4.6-r1

COPY run.sh /run.sh
COPY frigate_compressor /opt/frigate_compressor

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/frigate_compressor

CMD ["/run.sh"]
