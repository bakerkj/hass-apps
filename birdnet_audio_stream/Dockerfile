ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN set -eux; \
    apk add --no-cache \
        alsa-lib=1.2.14-r2 \
        alsa-utils=1.2.14-r4 \
        alsa-plugins-pulse=1.2.12-r2 \
        ffmpeg=8.0.1-r1 \
        go2rtc=1.9.9-r11

COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
