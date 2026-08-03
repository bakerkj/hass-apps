# Changelog

## [0.0.6](https://github.com/bakerkj/hass-apps/compare/signalk_bridge-v0.0.5...signalk_bridge-v0.0.6) (2026-08-03)


### Features

* **signalk_bridge:** adaptive staleness detection (device-gone → unavailable) ([#397](https://github.com/bakerkj/hass-apps/issues/397)) ([d37ddc4](https://github.com/bakerkj/hass-apps/commit/d37ddc4e1e1857173541f9cb2d51fca5a469cdcb))
* **signalk_bridge:** map remaining Victron MPPT solar values ([#399](https://github.com/bakerkj/hass-apps/issues/399)) ([d0365e1](https://github.com/bakerkj/hass-apps/commit/d0365e196a1fabc46705707878b1bcb8bba57d26))
* **signalk_bridge:** surface every value (full audit + publish_unmapped) ([#401](https://github.com/bakerkj/hass-apps/issues/401)) ([68b717f](https://github.com/bakerkj/hass-apps/commit/68b717fa73c939e241c37c074aaff4a14587d5ab))

## [0.0.5](https://github.com/bakerkj/hass-apps/compare/signalk_bridge-v0.0.4...signalk_bridge-v0.0.5) (2026-08-02)


### Features

* **signalk_bridge:** map waypoint/course, autopilot target, water current ([#395](https://github.com/bakerkj/hass-apps/issues/395)) ([bb0c3b1](https://github.com/bakerkj/hass-apps/commit/bb0c3b1f859efdddf5cd84623ad678d6629f6ca1))


### Bug Fixes

* **signalk_bridge:** collapse duplicated instance-vs-label ("Solar Solar" -&gt; "Solar") ([#391](https://github.com/bakerkj/hass-apps/issues/391)) ([d74a15e](https://github.com/bakerkj/hass-apps/commit/d74a15e3aeaf07e28cbf540e442625d6de1fe02b))

## [0.0.4](https://github.com/bakerkj/hass-apps/compare/signalk_bridge-v0.0.3...signalk_bridge-v0.0.4) (2026-07-31)


### Features

* **signalk_bridge:** Victron parity for tanks, battery power, MPPT state ([#382](https://github.com/bakerkj/hass-apps/issues/382)) ([a01a54b](https://github.com/bakerkj/hass-apps/commit/a01a54b1ce0d76e147c25439bc72f4de829e7dad))

## [0.0.3](https://github.com/bakerkj/hass-apps/compare/signalk_bridge-v0.0.2...signalk_bridge-v0.0.3) (2026-07-31)


### Features

* **signalk_bridge:** add suppress_paths + suppress_primary_on_fanout options ([#381](https://github.com/bakerkj/hass-apps/issues/381)) ([470d41f](https://github.com/bakerkj/hass-apps/commit/470d41fed1ef78d95d85606d0507f424df296597))
* **signalk_bridge:** fan out multi-source paths to per-source entities ([#376](https://github.com/bakerkj/hass-apps/issues/376)) ([5f67a92](https://github.com/bakerkj/hass-apps/commit/5f67a920aec34e21d37c4f15714b9604b85edbdd))


### Code Refactoring

* **tests:** let Dockerfile BUILD_FROM default be the single base pin ([#355](https://github.com/bakerkj/hass-apps/issues/355)) ([55c1efd](https://github.com/bakerkj/hass-apps/commit/55c1efd30e1650e08d687d671260b171d359e6b5))

## [0.0.2](https://github.com/bakerkj/hass-apps/compare/signalk_bridge-v0.0.1...signalk_bridge-v0.0.2) (2026-07-26)


### Features

* **signalk_bridge:** add Signal K to Home Assistant MQTT bridge add-on ([6524fe4](https://github.com/bakerkj/hass-apps/commit/6524fe411577eb82c81bf912636b6a930b3eed76))
* **signalk_bridge:** add Signal K to Home Assistant MQTT bridge add-on ([deb8124](https://github.com/bakerkj/hass-apps/commit/deb812474929135a6d78d05275cc9cef6fd6e393))


### Tests

* **signalk_bridge:** run the e2e against the real add-on container ([4b005a8](https://github.com/bakerkj/hass-apps/commit/4b005a8a0bb69aec2a96b01221b6c29f51ac1dae))
* **signalk_bridge:** run the e2e against the real add-on container ([0586bd9](https://github.com/bakerkj/hass-apps/commit/0586bd90535e8903a01af058a6eb34c132d8264f))

## Changelog
