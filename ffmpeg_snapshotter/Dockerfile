ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="FFmpeg Snapshotter" \
    org.opencontainers.image.description="Runs ffmpeg on-demand (per snapshot) to create timestamped JPEG snapshots from RTSP streams and maintains latest.jpg symlinks." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache \
  coreutils=9.8-r1 \
  ffmpeg=8.0.1-r1 \
  findutils=4.10.0-r0 \
  intel-media-driver=25.4.6-r0 \
  libva=2.22.0-r1 \
  libva-utils=2.22.0-r0 \
  mesa-va-gallium=25.2.7-r1 \
  py3-paho-mqtt=1.6.1-r3 \
  python3=3.12.13-r0

COPY run.sh /run.sh
COPY ffmpeg_snapshotter /opt/ffmpeg_snapshotter

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/ffmpeg_snapshotter

CMD ["/run.sh"]
