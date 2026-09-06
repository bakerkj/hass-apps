# Changelog

## [0.1.3](https://github.com/bakerkj/hass-apps/compare/container_hooks-v0.1.2...container_hooks-v0.1.3) (2026-09-06)


### Miscellaneous Chores

* **deps:** update alpine apk packages ([#497](https://github.com/bakerkj/hass-apps/issues/497)) ([0c1ae23](https://github.com/bakerkj/hass-apps/commit/0c1ae231c08ce372f78aa3e656fd912274a99105))

## [0.1.2](https://github.com/bakerkj/hass-apps/compare/container_hooks-v0.1.1...container_hooks-v0.1.2) (2026-08-16)


### Bug Fixes

* **container_hooks:** log bytes shipped, not tar stream length ([#470](https://github.com/bakerkj/hass-apps/issues/470)) ([e3aa5af](https://github.com/bakerkj/hass-apps/commit/e3aa5af120f718614b705ba610f422f791236ffa))
* **container_hooks:** pin uv and py3-pip apk versions ([#412](https://github.com/bakerkj/hass-apps/issues/412)) ([fbab4a7](https://github.com/bakerkj/hass-apps/commit/fbab4a7f0b769ea6f20ebd4c6591e02d084bdffa))


### Miscellaneous Chores

* **deps:** update dependency python3 to v3.14.7-r0 ([#451](https://github.com/bakerkj/hass-apps/issues/451)) ([dccea21](https://github.com/bakerkj/hass-apps/commit/dccea21b978d96bc394dc60e21f3ff6c48116636))
* **deps:** update dependency python3 to v3.14.7-r1 ([#468](https://github.com/bakerkj/hass-apps/issues/468)) ([0eaea96](https://github.com/bakerkj/hass-apps/commit/0eaea9631fbc1facb14fb451e9efc7effdd2ec9f))


### Documentation

* **container_hooks:** container names are app_*, not addon_* ([#471](https://github.com/bakerkj/hass-apps/issues/471)) ([8c319dd](https://github.com/bakerkj/hass-apps/commit/8c319ddac57aa4bfac87829b22cf45262af65b18))


### Code Refactoring

* **tests:** let Dockerfile BUILD_FROM default be the single base pin ([#355](https://github.com/bakerkj/hass-apps/issues/355)) ([55c1efd](https://github.com/bakerkj/hass-apps/commit/55c1efd30e1650e08d687d671260b171d359e6b5))

## [0.1.1](https://github.com/bakerkj/hass-apps/compare/container_hooks-v0.1.0...container_hooks-v0.1.1) (2026-07-24)


### Miscellaneous Chores

* add per-addon .dockerignore and ship LICENSE in each image ([#302](https://github.com/bakerkj/hass-apps/issues/302)) ([b74f977](https://github.com/bakerkj/hass-apps/commit/b74f977af0ac684e864a88d9fdf64d5f62cef853))
* **deps:** update pre-commit hook astral-sh/ruff-pre-commit to v0.16.0 ([#331](https://github.com/bakerkj/hass-apps/issues/331)) ([6cc91f4](https://github.com/bakerkj/hass-apps/commit/6cc91f47b7c17f2b428b41288c403a9a1a3cdac5))
* harden shellcheck config and fix uncovered findings ([#310](https://github.com/bakerkj/hass-apps/issues/310)) ([868e4f2](https://github.com/bakerkj/hass-apps/commit/868e4f26a6a9bbb1d54fdb67e1d187ccec22bc8f))

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
