# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="FFmpeg Snapshotter" \
    org.opencontainers.image.description="Runs ffmpeg on-demand (per snapshot) to create timestamped JPEG snapshots from RTSP streams and maintains latest.jpg symlinks." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
  coreutils=9.11-r0 \
  ffmpeg=8.1.2-r0 \
  findutils=4.10.0-r1 \
  intel-media-driver=26.2.1-r0 \
  libva=2.23.0-r0 \
  libva-utils=2.23.0-r0 \
  mesa-va-gallium=26.1.6-r0 \
  py3-paho-mqtt=1.6.1-r4 \
  python3=3.14.7-r1

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY ffmpeg_snapshotter /opt/ffmpeg_snapshotter

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/ffmpeg_snapshotter

CMD ["/run.sh"]
