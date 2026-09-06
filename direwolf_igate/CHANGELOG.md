# Changelog

## [0.0.7](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.6...direwolf_igate-v0.0.7) (2026-09-06)


### Miscellaneous Chores

* **deps:** update dependency python3 to v3.14.7-r0 ([#451](https://github.com/bakerkj/hass-apps/issues/451)) ([dccea21](https://github.com/bakerkj/hass-apps/commit/dccea21b978d96bc394dc60e21f3ff6c48116636))
* **deps:** update dependency python3 to v3.14.7-r1 ([#468](https://github.com/bakerkj/hass-apps/issues/468)) ([0eaea96](https://github.com/bakerkj/hass-apps/commit/0eaea9631fbc1facb14fb451e9efc7effdd2ec9f))

## [0.0.6](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.5...direwolf_igate-v0.0.6) (2026-08-08)


### Bug Fixes

* **direwolf_igate:** bound the shutdown so a deaf broker cannot cost us SIGKILL ([#436](https://github.com/bakerkj/hass-apps/issues/436)) ([793c5c5](https://github.com/bakerkj/hass-apps/commit/793c5c55ee506993c84e65bc9869788b0c355380))

## [0.0.5](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.4...direwolf_igate-v0.0.5) (2026-08-07)


### Bug Fixes

* **direwolf_igate:** replace the paho threads with an aiomqtt asyncio chassis ([#429](https://github.com/bakerkj/hass-apps/issues/429)) ([d854a55](https://github.com/bakerkj/hass-apps/commit/d854a55d156ce3f03ca3b30d5d1e708976480851))

## [0.0.4](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.3...direwolf_igate-v0.0.4) (2026-08-07)


### Bug Fixes

* **direwolf_igate:** don't join paho on SIGTERM when disconnected ([#407](https://github.com/bakerkj/hass-apps/issues/407)) ([36450b0](https://github.com/bakerkj/hass-apps/commit/36450b04ec18203efc6de6fa699cbc93aae98bfb))
* **direwolf_igate:** don't lose SIGTERM to a blocked read on the main thread ([#417](https://github.com/bakerkj/hass-apps/issues/417)) ([fca88e4](https://github.com/bakerkj/hass-apps/commit/fca88e468fac9cef268a8e90515aaa83969f6083))

## [0.0.3](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.2...direwolf_igate-v0.0.3) (2026-07-31)


### Tests

* **direwolf_igate:** tolerate slow paho teardown in SIGTERM tests ([#389](https://github.com/bakerkj/hass-apps/issues/389)) ([8fedba3](https://github.com/bakerkj/hass-apps/commit/8fedba3e7e0fa04594409e95f687d6841d846fa9))

## [0.0.2](https://github.com/bakerkj/hass-apps/compare/direwolf_igate-v0.0.1...direwolf_igate-v0.0.2) (2026-07-31)


### Features

* **direwolf_igate:** add receive-only APRS IGate add-on ([#368](https://github.com/bakerkj/hass-apps/issues/368)) ([47c13e0](https://github.com/bakerkj/hass-apps/commit/47c13e0b9326741ead177234ac1003eb962963d9))

## Changelog
