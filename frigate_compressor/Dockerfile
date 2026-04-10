ARG BUILD_FROM
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

RUN apk add --no-cache \
  python3=3.12.12-r0 \
  ffmpeg=8.0.1-r1 \
  libva=2.22.0-r1 \
  intel-media-driver=25.4.6-r0 \
  mesa-va-gallium=25.2.7-r1 \
  libva-utils=2.22.0-r0 \
  sqlite=3.51.2-r0

COPY run.sh /run.sh
COPY frigate_compressor.py /frigate_compressor.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
