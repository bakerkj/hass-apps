# Changelog

## [0.8.13](https://github.com/bakerkj/hass-apps/compare/ffmpeg_snapshotter-v0.8.12...ffmpeg_snapshotter-v0.8.13) (2026-06-20)


### Bug Fixes

* **ffmpeg_snapshotter:** use monotonic clock for publish pacing and rate stats ([#157](https://github.com/bakerkj/hass-apps/issues/157)) ([e4f4f17](https://github.com/bakerkj/hass-apps/commit/e4f4f1718abf10fc890bf4547c3f9c9431a00540))


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Documentation

* **ffmpeg_snapshotter:** add config screen labels and descriptions ([#164](https://github.com/bakerkj/hass-apps/issues/164)) ([de47aea](https://github.com/bakerkj/hass-apps/commit/de47aeaeaacdcc95d980a5f5bca16d1cd0ed6d7d))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.8.12](https://github.com/bakerkj/hass-apps/compare/ffmpeg_snapshotter-v0.8.11...ffmpeg_snapshotter-v0.8.12) (2026-05-15)


### Features

* publish prebuilt images to GHCR for remaining addons ([#153](https://github.com/bakerkj/hass-apps/issues/153)) ([3dc7225](https://github.com/bakerkj/hass-apps/commit/3dc72252e951af7690ad423d1e41e5c38b3ff242))


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
