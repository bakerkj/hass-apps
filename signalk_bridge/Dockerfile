# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

# s6 waits S6_KILL_GRACETIME (default 3000ms) tearing down its supervision tree
# after our service has exited. Ours stops in <1s, so shorten it.
ENV S6_KILL_GRACETIME=500

LABEL \
    org.opencontainers.image.title="Signal K to Home Assistant" \
    org.opencontainers.image.description="Publishes Signal K marine data as Home Assistant MQTT Discovery sensors." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

RUN --mount=type=cache,target=/var/cache/apk apk add \
      python3=3.14.5-r0 \
      py3-aiohttp=3.13.5-r0 \
      py3-websockets=16.0-r1 \
      uv=0.11.19-r0

# aiomqtt is not packaged in any Alpine branch, so it comes in via uv.
# aiomqtt 2.x wraps paho-mqtt 2.x -- pin both explicitly for reproducibility
# rather than letting the transitive-dep solver drift on rebuild.
# --break-system-packages overrides Alpine's PEP 668 marker on /usr; this
# container owns its whole Python install, so there is no system package
# manager to fight with.
RUN --mount=type=cache,target=/root/.cache/uv uv pip install \
      --system --no-cache --break-system-packages \
      aiomqtt==2.5.1 \
      paho-mqtt==2.1.0

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
COPY signalk_bridge /opt/signalk_bridge

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/signalk_bridge

CMD ["/run.sh"]
