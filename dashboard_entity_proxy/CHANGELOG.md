# Changelog

## [0.1.4](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.1.3...dashboard_entity_proxy-v0.1.4) (2026-07-24)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update alpine apk packages to v1.30.3-r0 ([#272](https://github.com/bakerkj/hass-apps/issues/272)) ([07b769a](https://github.com/bakerkj/hass-apps/commit/07b769a4809c4906c1aad200931f6b68b13340b1))
* **deps:** update alpine apk packages to v1.30.4-r0 ([#326](https://github.com/bakerkj/hass-apps/issues/326)) ([7ef0c36](https://github.com/bakerkj/hass-apps/commit/7ef0c36c864a0176a7f67cc97ee7c5315963c1de))
* **deps:** update alpine apk packages to v1.30.4-r1 ([#334](https://github.com/bakerkj/hass-apps/issues/334)) ([5e499e8](https://github.com/bakerkj/hass-apps/commit/5e499e8e110f77171c0400a46dfab7132fb2093e))
* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))


### Continuous Integration

* **tests:** unify per-addon test jobs under the e2e marker ([#255](https://github.com/bakerkj/hass-apps/issues/255)) ([1cdcd72](https://github.com/bakerkj/hass-apps/commit/1cdcd725b314347777612df3c8ea2473825d5d2c))

## [0.1.3](https://github.com/bakerkj/hass-apps/compare/dashboard_entity_proxy-v0.1.2...dashboard_entity_proxy-v0.1.3) (2026-06-20)


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))


### Tests

* **dep:** replace asyncio.get_event_loop() with get_running_loop() ([#252](https://github.com/bakerkj/hass-apps/issues/252)) ([eb68424](https://github.com/bakerkj/hass-apps/commit/eb68424d6587e6ee1f07f2319ee0eacc93f088db))

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
