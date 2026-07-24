# Changelog

## [0.1.24](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.23...container_info_mqtt-v0.1.24) (2026-07-24)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))

## [0.1.23](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.22...container_info_mqtt-v0.1.23) (2026-06-20)


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update alpine apk packages to v29.5.2-r0 ([#195](https://github.com/bakerkj/hass-apps/issues/195)) ([e685982](https://github.com/bakerkj/hass-apps/commit/e6859825824e3103291ee853b6abc1d6faafdd27))
* **deps:** update dependency docker-cli to v29.5.1-r0 ([#184](https://github.com/bakerkj/hass-apps/issues/184)) ([0e022d1](https://github.com/bakerkj/hass-apps/commit/0e022d1c2d84efff0aef4ee65d5f0aa3cb75ebf2))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.1.22](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.21...container_info_mqtt-v0.1.22) (2026-05-18)


### Bug Fixes

* **container_info_mqtt:** reconcile retained discovery for active containers ([#178](https://github.com/bakerkj/hass-apps/issues/178)) ([aac5aba](https://github.com/bakerkj/hass-apps/commit/aac5aba68853495e9103a2a6428eaea5aec544c9))

## [0.1.21](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.20...container_info_mqtt-v0.1.21) (2026-05-17)


### Miscellaneous Chores

* **deps:** update alpine apk packages ([#176](https://github.com/bakerkj/hass-apps/issues/176)) ([92ab256](https://github.com/bakerkj/hass-apps/commit/92ab2568f56b3367b87da9f2d6ca39659c69737c))

## [0.1.20](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.19...container_info_mqtt-v0.1.20) (2026-05-17)


### Features

* **container_info_mqtt:** report container start time instead of uptime ([#174](https://github.com/bakerkj/hass-apps/issues/174)) ([3bdbcca](https://github.com/bakerkj/hass-apps/commit/3bdbcca35a3801c04677057318c0c6b21d86024a))


### Bug Fixes

* **container_info_mqtt:** coarsen metric rounding to cut recorder churn ([#175](https://github.com/bakerkj/hass-apps/issues/175)) ([d1fbd81](https://github.com/bakerkj/hass-apps/commit/d1fbd8121d42a4c750b02b3f3a80fcb6399c59a9))


### Documentation

* **container_info_mqtt:** add config screen labels and descriptions ([#163](https://github.com/bakerkj/hass-apps/issues/163)) ([1bc9ba4](https://github.com/bakerkj/hass-apps/commit/1bc9ba44c7321c5569f5145fe39ce44f2ea8f82b))

## [0.1.19](https://github.com/bakerkj/hass-apps/compare/container_info_mqtt-v0.1.18...container_info_mqtt-v0.1.19) (2026-05-15)


### Features

* publish prebuilt images to GHCR for remaining addons ([#153](https://github.com/bakerkj/hass-apps/issues/153)) ([3dc7225](https://github.com/bakerkj/hass-apps/commit/3dc72252e951af7690ad423d1e41e5c38b3ff242))


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
