# syntax=docker/dockerfile:1

# Base is the upstream Signal K server image, not the Home Assistant base -- so
# no bashio and no s6-overlay here; this add-on is a single long-running process,
# and the upstream entrypoint plus a thin wrapper is enough. The version below is
# the single source of truth: the HA builder, the e2e, and CI all build from this
# ARG default (there's no build.yaml), and renovate tracks it here.
ARG BUILD_FROM=signalk/signalk-server:v2.30.0-alpine
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="Signal K Server" \
    org.opencontainers.image.description="Signal K server reading NMEA 2000 from SocketCAN for Home Assistant." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Start as root only so the wrapper can chown /data/signalk (root-owned by the
# Supervisor) to `node`, then drop to it -- the user the upstream image runs the
# server as. Opening the SocketCAN can0 socket needs no privilege. su-exec drops.
# hadolint ignore=DL3002
USER root
RUN apk add --no-cache su-exec

# The upstream alpine image leaves some native modules unbuilt -- notably
# canboatjs's SocketCAN binding (build/Release/canSocket.node), whose compile
# needs linux-headers (which upstream omits), so "Canbus (canboatjs)" can't
# open can0. Rebuild EVERY native module against this image's Node with the
# toolchain present, so no binding is left unbuilt (upstream's now or a future
# version's/plugin's). Then drop the toolchain to keep the image lean. The gate
# fails the build loudly if the critical CAN binding isn't produced OR won't load
# for this image's Node ABI (a bare require() of the compiled .node catches a
# compiled-but-unloadable binding, which a file-exists check would miss).
WORKDIR /home/node/signalk
RUN apk add --no-cache --virtual .canbuild linux-headers python3 make g++ \
    && HOME=/tmp npm rebuild \
    && apk del .canbuild \
    && test -f node_modules/@canboat/canboatjs/build/Release/canSocket.node \
    && node -e "require('/home/node/signalk/node_modules/@canboat/canboatjs/build/Release/canSocket.node')"

COPY LICENSE.md /LICENSE
COPY run.sh /run.sh
RUN chmod +x /run.sh

# Replace the upstream entrypoint so our wrapper can set SIGNALK_NODE_CONFIG_DIR
# to /data before handing off to it.
ENTRYPOINT []
CMD ["/run.sh"]
