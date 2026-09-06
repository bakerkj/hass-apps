# Changelog

## [0.0.34](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.33...system_resource_tuner-v0.0.34) (2026-09-06)


### Miscellaneous Chores

* **deps:** update dependency python3 to v3.14.7-r0 ([#451](https://github.com/bakerkj/hass-apps/issues/451)) ([dccea21](https://github.com/bakerkj/hass-apps/commit/dccea21b978d96bc394dc60e21f3ff6c48116636))
* **deps:** update dependency python3 to v3.14.7-r1 ([#468](https://github.com/bakerkj/hass-apps/issues/468)) ([0eaea96](https://github.com/bakerkj/hass-apps/commit/0eaea9631fbc1facb14fb451e9efc7effdd2ec9f))

## [0.0.33](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.32...system_resource_tuner-v0.0.33) (2026-07-24)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))

## [0.0.32](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.31...system_resource_tuner-v0.0.32) (2026-06-21)


### Code Refactoring

* **system_resource_tuner:** tighter apply log lines + INFO at startup only ([#268](https://github.com/bakerkj/hass-apps/issues/268)) ([fc9eb50](https://github.com/bakerkj/hass-apps/commit/fc9eb50a8028fb35b8f620e126462e68eb2d3e24))

## [0.0.31](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.30...system_resource_tuner-v0.0.31) (2026-06-21)


### Features

* **system_resource_tuner:** event-driven fast-path + decoupled reconcile ([#267](https://github.com/bakerkj/hass-apps/issues/267)) ([857610a](https://github.com/bakerkj/hass-apps/commit/857610ab6d18d51fb256a208267f3e409a9c53b0))


### Code Refactoring

* **system_resource_tuner:** convert reconcile loop to async + aiodocker ([#263](https://github.com/bakerkj/hass-apps/issues/263)) ([5eb5a35](https://github.com/bakerkj/hass-apps/commit/5eb5a35e66676985144841ded47b0f36b3583763))

## [0.0.30](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.29...system_resource_tuner-v0.0.30) (2026-06-20)


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update alpine apk packages ([#176](https://github.com/bakerkj/hass-apps/issues/176)) ([92ab256](https://github.com/bakerkj/hass-apps/commit/92ab2568f56b3367b87da9f2d6ca39659c69737c))
* **deps:** update alpine apk packages to v29.5.2-r0 ([#195](https://github.com/bakerkj/hass-apps/issues/195)) ([e685982](https://github.com/bakerkj/hass-apps/commit/e6859825824e3103291ee853b6abc1d6faafdd27))
* **deps:** update dependency docker-cli to v29.5.1-r0 ([#184](https://github.com/bakerkj/hass-apps/issues/184)) ([0e022d1](https://github.com/bakerkj/hass-apps/commit/0e022d1c2d84efff0aef4ee65d5f0aa3cb75ebf2))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Documentation

* **system_resource_tuner:** add config screen labels and descriptions ([#166](https://github.com/bakerkj/hass-apps/issues/166)) ([b0325f9](https://github.com/bakerkj/hass-apps/commit/b0325f942ddd624473b5bfed21352ef99041dd8a))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.0.29](https://github.com/bakerkj/hass-apps/compare/system_resource_tuner-v0.0.28...system_resource_tuner-v0.0.29) (2026-05-15)


### Features

* publish prebuilt images to GHCR for remaining addons ([#153](https://github.com/bakerkj/hass-apps/issues/153)) ([3dc7225](https://github.com/bakerkj/hass-apps/commit/3dc72252e951af7690ad423d1e41e5c38b3ff242))


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
