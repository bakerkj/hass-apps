# Changelog

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
