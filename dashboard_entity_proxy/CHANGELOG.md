# Changelog

## [0.1.2](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.1.1...dashboard_entity_proxy-v0.1.2) (2026-06-16)


### Features

* **dashboard_entity_proxy:** drop dashboard_url_path; client seeds scope ([#229](https://github.com/bakerkj/hass-apps/issues/229)) ([067db33](https://github.com/bakerkj/hass-apps/commit/067db333d1ea3315e78899cd5f83dd3af38e985b))
* **dashboard_entity_proxy:** name dashboard in scope fallback warnings; uncap status sample ([#227](https://github.com/bakerkj/hass-apps/issues/227)) ([32bd553](https://github.com/bakerkj/hass-apps/commit/32bd553081d7ee246a89425c25845303ab53038a))
* **dashboard_entity_proxy:** per-session connect, scope-ready, disconnect logs ([#230](https://github.com/bakerkj/hass-apps/issues/230)) ([a38d399](https://github.com/bakerkj/hass-apps/commit/a38d399fc5bd1e94f32a295f805129dd0a9b5d09))
* **dashboard_entity_proxy:** richer tunnel cards in the status UI ([#232](https://github.com/bakerkj/hass-apps/issues/232)) ([3bc677c](https://github.com/bakerkj/hass-apps/commit/3bc677cab4a16bbf328c8adb9635d5b48e9a29f2))


### Bug Fixes

* **dashboard_entity_proxy:** WS tunnel Origin rewrite for upstream check_origin ([#231](https://github.com/bakerkj/hass-apps/issues/231)) ([bf5cc5e](https://github.com/bakerkj/hass-apps/commit/bf5cc5ef8aeea527a83a968da183af8e6b56fd21))

## [0.1.1](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.1.0...dashboard_entity_proxy-v0.1.1) (2026-06-16)


### Features

* **dashboard_entity_proxy:** gate access logging on log_level=DEBUG ([#226](https://github.com/bakerkj/hass-apps/issues/226)) ([6b7aeeb](https://github.com/bakerkj/hass-apps/commit/6b7aeebc88354c1a1b98bb667a36ceb8827a4837))


### Styles

* **dashboard_entity_proxy:** emphasise hostname over IP in status UI ([#224](https://github.com/bakerkj/hass-apps/issues/224)) ([1cd630c](https://github.com/bakerkj/hass-apps/commit/1cd630c0e1a454939e091cbba4d48862340f992b))

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
