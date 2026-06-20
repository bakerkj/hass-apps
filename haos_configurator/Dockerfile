# syntax=docker/dockerfile:1
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

LABEL \
    org.opencontainers.image.title="HAOS Configurator" \
    org.opencontainers.image.description="Manifest-driven installer for persistent HAOS host files (udev rules, init scripts, etc.)." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Pin every apk to a concrete Alpine 3.24 version so renovate's
# alpine_3_23 custom manager (see renovate.json) can bump them.
# nsenter comes from busybox in the HA base image.
RUN --mount=type=cache,target=/var/cache/apk apk add \
      python3=3.14.5-r0 \
      py3-yaml=6.0.3-r2

COPY run.sh /run.sh
COPY haos_configurator /opt/haos_configurator

RUN chmod +x /run.sh \
 && python3 -m compileall -q /opt/haos_configurator

# Override the HA base image's s6-overlay ``/init`` entrypoint.  This add-on
# is a one-shot Python script that does its work and exits — we don't need
# s6's service supervision, and s6-overlay-suexec's "must be pid 1" check
# fires before our code runs under Protection mode (because of how Supervisor
# starts the container in that mode), so the friendly "Protection mode must
# be OFF" message in app.main() never gets reached.  Bypass s6 entirely.
ENTRYPOINT []
CMD ["/run.sh"]
