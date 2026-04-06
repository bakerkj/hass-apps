ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
  python3 \
  ffmpeg \
  libva \
  intel-media-driver \
  mesa-va-gallium \
  libva-utils \
  sqlite

COPY run.sh /run.sh
COPY frigate_compressor.py /frigate_compressor.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
