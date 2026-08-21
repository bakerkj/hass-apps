# Changelog

## [0.0.58](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.57...frigate_compressor-v0.0.58) (2026-08-21)


### Miscellaneous Chores

* **deps:** update dependency python3 to v3.14.7-r0 ([#451](https://github.com/bakerkj/hass-apps/issues/451)) ([dccea21](https://github.com/bakerkj/hass-apps/commit/dccea21b978d96bc394dc60e21f3ff6c48116636))
* **deps:** update dependency python3 to v3.14.7-r1 ([#468](https://github.com/bakerkj/hass-apps/issues/468)) ([0eaea96](https://github.com/bakerkj/hass-apps/commit/0eaea9631fbc1facb14fb451e9efc7effdd2ec9f))
* **deps:** update dependency sqlite to v3.53.4-r0 ([#477](https://github.com/bakerkj/hass-apps/issues/477)) ([dbb1806](https://github.com/bakerkj/hass-apps/commit/dbb180679dabca5ee3972884060f8223882f047b))

## [0.0.57](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.56...frigate_compressor-v0.0.57) (2026-08-08)


### Bug Fixes

* never join paho's network thread on any shutdown path ([#444](https://github.com/bakerkj/hass-apps/issues/444)) ([bdc2c02](https://github.com/bakerkj/hass-apps/commit/bdc2c025aa644eb37f79f346130ba0e148546f23))


### Miscellaneous Chores

* **deps:** update dependency mesa-va-gallium to v26.1.6-r0 ([#377](https://github.com/bakerkj/hass-apps/issues/377)) ([bc9ad29](https://github.com/bakerkj/hass-apps/commit/bc9ad293e73a02654200ff5244c232e970117691))


### Tests

* fix aiohttp and sqlite ResourceWarning in test suites ([#348](https://github.com/bakerkj/hass-apps/issues/348)) ([d1f9186](https://github.com/bakerkj/hass-apps/commit/d1f9186a4f8c1393e3ffc57b8910e0774fc3ff53))

## [0.0.56](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.55...frigate_compressor-v0.0.56) (2026-07-24)


### Miscellaneous Chores

* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))

## [0.0.55](https://github.com/bakerkj/hass-apps/compare/frigate_compressor-v0.0.54...frigate_compressor-v0.0.55) (2026-07-17)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update dependency ffmpeg to v8.1.2-r0 ([#281](https://github.com/bakerkj/hass-apps/issues/281)) ([133a954](https://github.com/bakerkj/hass-apps/commit/133a954456aae439165bcbefacfbe2fae43b6978))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))


### Continuous Integration

* **tests:** unify per-addon test jobs under the e2e marker ([#255](https://github.com/bakerkj/hass-apps/issues/255)) ([1cdcd72](https://github.com/bakerkj/hass-apps/commit/1cdcd725b314347777612df3c8ea2473825d5d2c))

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
