# Changelog

## [0.3.31](https://github.com/bakerkj/hass-apps/compare/intel_gpu_top_mqtt-v0.3.30...intel_gpu_top_mqtt-v0.3.31) (2026-07-24)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))

## [0.3.30](https://github.com/bakerkj/hass-apps/compare/intel_gpu_top_mqtt-v0.3.29...intel_gpu_top_mqtt-v0.3.30) (2026-06-20)


### Bug Fixes

* **intel_gpu_top_mqtt:** use monotonic clock for restart debounce and stall watchdog ([#158](https://github.com/bakerkj/hass-apps/issues/158)) ([b2f7a01](https://github.com/bakerkj/hass-apps/commit/b2f7a0114d4ba49bc8d60f89d9d574857b1728da))


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Documentation

* **intel_gpu_top_mqtt:** add config screen labels and descriptions ([#165](https://github.com/bakerkj/hass-apps/issues/165)) ([0af4797](https://github.com/bakerkj/hass-apps/commit/0af4797ed04ac887cc9da63b8e2b3735b1413af4))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.3.29](https://github.com/bakerkj/hass-apps/compare/intel_gpu_top_mqtt-v0.3.28...intel_gpu_top_mqtt-v0.3.29) (2026-05-15)


### Features

* publish prebuilt images to GHCR for remaining addons ([#153](https://github.com/bakerkj/hass-apps/issues/153)) ([3dc7225](https://github.com/bakerkj/hass-apps/commit/3dc72252e951af7690ad423d1e41e5c38b3ff242))


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
