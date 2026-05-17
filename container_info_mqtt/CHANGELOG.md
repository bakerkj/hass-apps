# Changelog

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
