# Changelog

## [0.1.0](https://github.com/bakerkj/hass-apps/compare/container_hooks-v0.0.2...container_hooks-v0.1.0) (2026-06-20)


### ⚠ BREAKING CHANGES

* **container_hooks:** ``watch_create_events`` is removed from the addon schema, the Options dataclass, the per-event dispatcher conditional, the Configuration log line, the README configuration table, the translations, and the e2e test write_options callsites. Existing options.json values for this key will be stripped by Supervisor on next save (schema rejects unknown keys) or surface a "ignoring unrecognized top-level key" warning in addon logs.

### Features

* **container_hooks:** always watch docker create events ([#258](https://github.com/bakerkj/hass-apps/issues/258)) ([3dfb11a](https://github.com/bakerkj/hass-apps/commit/3dfb11aa5e4ee026e773f882d019f16931f91d32))
* **container_hooks:** leading+trailing debounce; clarify scope ([#259](https://github.com/bakerkj/hass-apps/issues/259)) ([3669fd6](https://github.com/bakerkj/hass-apps/commit/3669fd610274047e98b24fc89e70ad1c130ba6a5))


### Bug Fixes

* **container_hooks:** derive own container id from /proc/self/mountinfo ([#256](https://github.com/bakerkj/hass-apps/issues/256)) ([88fc21e](https://github.com/bakerkj/hass-apps/commit/88fc21e5df8798cf1c6e32970a549bdbdbfb17c0))

## [0.0.2](https://github.com/bakerkj/hass-apps/compare/container_hooks-v0.0.1...container_hooks-v0.0.2) (2026-06-20)


### Features

* **container_hooks:** new addon for docker container lifecycle hooks ([#233](https://github.com/bakerkj/hass-apps/issues/233)) ([f682646](https://github.com/bakerkj/hass-apps/commit/f682646e9f58a2f752d5de692bf5e9808adb1b5a))


### Miscellaneous Chores

* **3.14:** drop redundant from __future__ import annotations ([#253](https://github.com/bakerkj/hass-apps/issues/253)) ([4eba832](https://github.com/bakerkj/hass-apps/commit/4eba832ce9136f7e1afc3b9ec9ecd975714eb60c))
* **deps:** update home-assistant base images to v3.24 ([#237](https://github.com/bakerkj/hass-apps/issues/237)) ([a9bf453](https://github.com/bakerkj/hass-apps/commit/a9bf4535753e15c7713530b20fbe836725184838))


### Code Refactoring

* per-addon test layout ([#239](https://github.com/bakerkj/hass-apps/issues/239)) ([bc5b93b](https://github.com/bakerkj/hass-apps/commit/bc5b93b60f31901d14f3bd0393289946e1eec1f2))

## Changelog
