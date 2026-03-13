ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN set -eux; \
    apk add --no-cache \
        alsa-lib \
        alsa-utils \
        alsa-plugins-pulse \
        ffmpeg \
        go2rtc

COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD [ "/run.sh" ]
