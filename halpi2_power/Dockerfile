# syntax=docker/dockerfile:1

# Declared before the first FROM so it lands in global scope and is visible to
# the runtime stage's FROM. An ARG declared inside a stage is not.
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24

# ---------------------------------------------------------------------------
# Build stage: cross-compile halpid/halpi for aarch64-musl.
#
# Pinned to $BUILDPLATFORM deliberately. Without it the build stage would run
# under QEMU on an x86 runner, and compiling Rust under emulation takes tens of
# minutes instead of a couple.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM messense/rust-musl-cross:aarch64-musl AS build

# renovate: datasource=github-releases depName=hatlabs/HALPI2-rust-daemon
ARG HALPID_VERSION=5.1.1+1

WORKDIR /src
# Fetched with RUN rather than ADD so the '+' in the upstream tag can be
# percent-encoded from ${HALPID_VERSION}. Hardcoding the encoded tag in an ADD
# would leave renovate bumping the ARG while the build silently kept fetching
# the old release. The lone printf|sed percent-encodes the '+' -- harmless.
# hadolint ignore=DL4006
RUN set -eu; \
    tag_enc="$(printf '%s' "v${HALPID_VERSION}" | sed 's/+/%2B/g')"; \
    echo "Fetching halpid ${tag_enc}"; \
    curl -fsSL "https://github.com/hatlabs/HALPI2-rust-daemon/archive/refs/tags/${tag_enc}.tar.gz" \
      -o /src.tgz; \
    tar -xz --strip-components=1 -f /src.tgz; \
    test -f Cargo.toml

# Patches are applied against a pinned release rather than maintained as a fork.
# If a renovate version bump makes a patch stop applying, this fails the build
# loudly -- which is the intent.
#
# `set -e` is essential: /bin/sh -c has no -e, and a for loop's exit status is
# only its last command's, so without it a failed earlier patch would ship an
# unpatched source and surface only at runtime on the device. `--fuzz=0` makes a
# context-drifted hunk fail hard (default fuzz is 2, which would let a hunk apply
# mis-placed and still exit 0). `--batch` avoids any interactive prompt.
#
# `--forward` is load-bearing, not decoration: if a future upstream bump already
# contains a patch's change, GNU patch's DEFAULT is to silently reverse-apply it
# (prints "Assuming -R", exits 0) -- shipping halpid with the watchdog change
# REMOVED, i.e. Solo-only, discovered only on the boat. `--forward` instead skips
# it and exits 1, so `set -e` aborts the build for review. Verified on patch 2.8.
# Do not add fuzz or --force here.
COPY patches/ /patches/
RUN set -e; for p in /patches/*.patch; do \
      echo "Applying $(basename "$p")"; \
      patch -p1 --fuzz=0 --batch --forward < "$p"; \
    done

RUN cargo build --release --target aarch64-unknown-linux-musl

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM ${BUILD_FROM}

ARG BUILD_VERSION
ENV ADDON_VERSION=${BUILD_VERSION}

# s6 waits S6_KILL_GRACETIME (default 3000ms) tearing down its own supervision
# tree at shutdown, after our services have already exited -- ~3s of dead time.
# Our services stop in <1s, so a short value is safe and cuts most of it.
ENV S6_KILL_GRACETIME=500

LABEL \
    org.opencontainers.image.title="HALPI2 Power" \
    org.opencontainers.image.description="Runs halpid in Co-op mode for graceful shutdown on power outage and publishes power controller telemetry over MQTT." \
    org.opencontainers.image.source="https://github.com/bakerkj/hass-apps" \
    org.opencontainers.image.licenses="MIT"

# Pin every apk to a concrete Alpine 3.24 version so renovate's alpine_3_23
# custom manager (see renovate.json) can bump them.
RUN --mount=type=cache,target=/var/cache/apk apk add \
      python3=3.14.7-r0 \
      py3-paho-mqtt=1.6.1-r4 \
      curl=8.21.0-r0

# halpid resolves the group for its API socket via a hardcoded getgrnam("halpid")
# in main.rs -- it is NOT the configurable `socket-group` (that one only covers
# the LED socket). Upstream's Debian package creates this group in postinst; we
# have to create it ourselves or the daemon exits 1 at startup.
RUN addgroup -S halpid

COPY --from=build /src/target/aarch64-unknown-linux-musl/release/halpid /usr/bin/halpid
COPY --from=build /src/target/aarch64-unknown-linux-musl/release/halpi  /usr/bin/halpi

# halpid is BSD-3-Clause (c) Hat Labs Oy. Binary redistribution requires that
# the copyright notice, conditions and disclaimer accompany the distribution.
COPY --from=build /src/LICENSE /LICENSE.halpid

COPY LICENSE.md /LICENSE
COPY rootfs /
COPY halpi2_power /opt/halpi2_power

RUN python3 -m compileall -q /opt/halpi2_power

# No CMD and no run.sh, unlike the single-process add-ons in this repo: this
# image runs two long-lived services (halpid and the MQTT publisher) as s6-rc.d
# longruns under the base image's s6-overlay (/etc/s6-overlay/s6-rc.d), with an
# init-halpid-conf oneshot rendering the config first. The inherited /init
# entrypoint supervises them. s6 is what delivers SIGTERM to halpid on stop,
# which is how the hardware watchdog gets disarmed cleanly. The run/finish
# scripts ship executable (git mode); COPY preserves it, so no chmod here.
