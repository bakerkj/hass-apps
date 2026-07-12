# Changelog

## [0.0.38](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.37...turbostat_mqtt-v0.0.38) (2026-07-12)


### Features

* **turbostat_mqtt:** publish cpuidle diagnostic counters and consolidate column metadata ([#300](https://github.com/bakerkj/hass-apps/issues/300)) ([7737513](https://github.com/bakerkj/hass-apps/commit/7737513ff72ec0e5761718ed5c5cd19ccd3d7380))

## [0.0.37](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.36...turbostat_mqtt-v0.0.37) (2026-07-12)


### Miscellaneous Chores

* **deps:** update alpine apk packages to v7.1.3-r0 ([#298](https://github.com/bakerkj/hass-apps/issues/298)) ([0613572](https://github.com/bakerkj/hass-apps/commit/0613572a86e3f3350cbba85fddc101bf53858f78))

## [0.0.36](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.35...turbostat_mqtt-v0.0.36) (2026-06-23)


### Bug Fixes

* **turbostat_mqtt:** publish LLC/L2 cache references in M/s instead of 1/s ([#274](https://github.com/bakerkj/hass-apps/issues/274)) ([ec155e3](https://github.com/bakerkj/hass-apps/commit/ec155e3b8c65da0a99f8fd0bebcb0140b0cf3215))

## [0.0.35](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.34...turbostat_mqtt-v0.0.35) (2026-06-23)


### Bug Fixes

* **turbostat_mqtt:** restore LLC/L2 cache columns after upstream rename ([#270](https://github.com/bakerkj/hass-apps/issues/270)) ([b0057d9](https://github.com/bakerkj/hass-apps/commit/b0057d93b6ea464134b19371cfee78e743c5aa20))

## [0.0.34](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.33...turbostat_mqtt-v0.0.34) (2026-06-20)


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## [0.0.33](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.32...turbostat_mqtt-v0.0.33) (2026-05-22)


### Bug Fixes

* **turbostat_mqtt:** stop publishing raw sample and entity attributes by default ([#187](https://github.com/bakerkj/hass-apps/issues/187)) ([8c96cef](https://github.com/bakerkj/hass-apps/commit/8c96cef1a2a4f51a614701fdd37e0c2e0bff4255))
* **turbostat_mqtt:** use monotonic clock for restart debounce and stall watchdogs ([#160](https://github.com/bakerkj/hass-apps/issues/160)) ([1b222b8](https://github.com/bakerkj/hass-apps/commit/1b222b8ff063cd0cdda616a53f6926b6efdcffad))


### Documentation

* **turbostat_mqtt:** add config screen labels and descriptions ([#167](https://github.com/bakerkj/hass-apps/issues/167)) ([e76caae](https://github.com/bakerkj/hass-apps/commit/e76caae1338aa6b2c70bd2e7a66076ff997365a8))

## [0.0.32](https://github.com/bakerkj/hass-apps/compare/turbostat_mqtt-v0.0.31...turbostat_mqtt-v0.0.32) (2026-05-15)


### Features

* publish prebuilt images to GHCR for remaining addons ([#153](https://github.com/bakerkj/hass-apps/issues/153)) ([3dc7225](https://github.com/bakerkj/hass-apps/commit/3dc72252e951af7690ad423d1e41e5c38b3ff242))


### Build System

* use buildkit apk cache mount in addon Dockerfiles ([#149](https://github.com/bakerkj/hass-apps/issues/149)) ([4ffdd60](https://github.com/bakerkj/hass-apps/commit/4ffdd60e064b49de1050eb617604edf04d3cc3f9))


### Continuous Integration

* adopt release-please for monorepo release automation ([#130](https://github.com/bakerkj/hass-apps/issues/130)) ([873c018](https://github.com/bakerkj/hass-apps/commit/873c018662fc5f0b667f11c255ab80c3d1df395a))
