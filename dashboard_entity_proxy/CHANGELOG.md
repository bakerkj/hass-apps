# Changelog

## [0.1.0](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.0.3...dashboard_entity_proxy-v0.1.0) (2026-06-16)


### ⚠ BREAKING CHANGES

* **dashboard_entity_proxy:** anyone who created a customization file at the previous default ``/config/dashboard_entity_proxy.yaml`` needs to move it. From HA's own filesystem view nothing moves — only the path the addon option now defaults to changes — but the option must be updated if it was left at the old default.

### Bug Fixes

* **dashboard_entity_proxy:** migrate map from config to homeassistant_config ([#222](https://github.com/bakerkj/hass-apps/issues/222)) ([03dfbe3](https://github.com/bakerkj/hass-apps/commit/03dfbe3f2cc345e4b3ccbbe6d0a8a3d6cfc03108))

## [0.0.3](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.0.2...dashboard_entity_proxy-v0.0.3) (2026-06-16)


### Features

* **dashboard_entity_proxy:** default customization_file to /config/dashboard_entity_proxy.yaml ([#218](https://github.com/bakerkj/hass-apps/issues/218)) ([606a8a1](https://github.com/bakerkj/hass-apps/commit/606a8a1b669afb1887517c6f43206371f9260b1c))
* **dashboard_entity_proxy:** show reverse-DNS hostname in status UI ([#220](https://github.com/bakerkj/hass-apps/issues/220)) ([4a68a3b](https://github.com/bakerkj/hass-apps/commit/4a68a3b72fef3b6860afba01cb55d94ad57caed8))

## [0.0.2](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.0.1...dashboard_entity_proxy-v0.0.2) (2026-06-14)


### Bug Fixes

* **dashboard_entity_proxy:** raise OUTBOUND_BUFFER + add queue diagnostics ([#213](https://github.com/bakerkj/hass-apps/issues/213)) ([7ebdcbf](https://github.com/bakerkj/hass-apps/commit/7ebdcbf6f9a91785098f31c965a848401f461267))

## 0.0.1 (2026-06-14)


### Features

* **dashboard_entity_proxy:** introduce the addon ([#208](https://github.com/bakerkj/hass-apps/issues/208)) ([58549eb](https://github.com/bakerkj/hass-apps/commit/58549eb57aba18a6e39b1426e5ac43091d58e0bd))
