# Changelog

## [0.0.54](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.53...frigate_compressor-v0.0.54) (2026-06-20)


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))
* **mypy:** enable check_untyped_defs + warn_unused_ignores (non-DEP scope) ([#206](https://github.com/bakerkj/hass-apps/issues/206)) ([5d83443](https://github.com/bakerkj/hass-apps/commit/5d83443a7be960bf867b79411cce56f881d4d5be))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.0.53](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.52...frigate_compressor-v0.0.53) (2026-05-19)


### Bug Fixes

* **frigate_compressor:** clamp rate_window and use monotonic clock for rate sensors ([#154](https://github.com/bakerkj/hass-apps/issues/154)) ([3121180](https://github.com/bakerkj/hass-apps/commit/31211809bfb318423394628cfc98599aa7a3e95c))
* **frigate_compressor:** make idx_files_t2_pending_age usable by both eligibility consumers ([#182](https://github.com/bakerkj/hass-apps/issues/182)) ([0f58dd8](https://github.com/bakerkj/hass-apps/commit/0f58dd85f11d3b3c6403961be741a1adc69132c4))


### Documentation

* **frigate_compressor:** add config screen labels and descriptions ([#155](https://github.com/bakerkj/hass-apps/issues/155)) ([c67295d](https://github.com/bakerkj/hass-apps/commit/c67295d57d591afb1e44ce646a9f15cd6579860d))

## [0.0.52](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.51...frigate_compressor-v0.0.52) (2026-05-15)


### Bug Fixes

* **frigate_compressor:** bump config.json to 0.0.51 + correct release-please extra-files paths ([#151](https://github.com/bakerkj/hass-apps/issues/151)) ([0f97be7](https://github.com/bakerkj/hass-apps/commit/0f97be711f8ae8b5a8aa82356cf7cb1cad7c83db))

## [0.0.51](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.50...frigate_compressor-v0.0.51) (2026-05-15)


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))

## [0.0.50](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.49...frigate_compressor-v0.0.50) (2026-05-14)


### Features

* **frigate_compressor:** publish prebuilt image to GHCR via addon-build pipeline ([#145](https://github.com/bakerkj/hass-apps/issues/145)) ([72360fc](https://github.com/bakerkj/hass-apps/commit/72360fc1f6eda979f054782ecbc6fc0d217510fb))


### Miscellaneous Chores

* **deps:** update dependency onevpl-intel-gpu to v25.4.6-r1 ([#143](https://github.com/bakerkj/hass-apps/issues/143)) ([73cbb7c](https://github.com/bakerkj/hass-apps/commit/73cbb7c45813c30023b8d748c134c00c7e0d9b3c))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
