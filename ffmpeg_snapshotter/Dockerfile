ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3 \
  ffmpeg \
  libva \
  intel-media-driver \
  mesa-va-gallium \
  libva-utils \
  coreutils \
  findutils

COPY run.sh /run.sh
COPY ffmpeg_snapshotter.py /ffmpeg_snapshotter.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
