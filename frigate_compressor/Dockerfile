ARG BUILD_FROM
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

# Two apk calls in one RUN: the first uses Alpine 3.23 stable repos; the
# second pulls onevpl-intel-gpu from Alpine edge/testing because Alpine 3.23
# stable only ships the libvpl dispatcher, not its runtime backend
# (libmfx-gen).  Without the runtime, h264_qsv fails.  --repository scopes the alternate
# repo to the second apk call only, so future stable updates can't
# accidentally come from edge.
RUN apk add --no-cache \
      python3=3.12.12-r0 \
      py3-paho-mqtt=1.6.1-r3 \
      ffmpeg=8.0.1-r1 \
      libva=2.22.0-r1 \
      intel-media-driver=25.4.6-r0 \
      mesa-va-gallium=25.2.7-r1 \
      libva-utils=2.22.0-r0 \
      sqlite=3.51.2-r0 \
 && apk add --no-cache \
      --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing \
      onevpl-intel-gpu=25.4.6-r0

COPY run.sh /run.sh
COPY frigate_compressor.py /frigate_compressor.py

RUN chmod +x /run.sh

CMD ["/run.sh"]
