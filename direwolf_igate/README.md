# Direwolf APRS IGate (Home Assistant Add-on)

Receive-only APRS IGate built on [Direwolf](https://github.com/wb2osz/direwolf)
with an RTL-SDR dongle as the audio source. Packet audio is demodulated by
`rtl_fm`, decoded by `direwolf`, and forwarded to an APRS-IS Tier 2 server.

## What It Does

- Tunes an RTL-SDR dongle to an APRS frequency (default `144.390M`, US 2 m).
- Pipes raw audio into Direwolf at 24000 Hz (the rtl_fm and direwolf defaults).
- Logs in to an APRS-IS server with `MYCALL` and the matching passcode.
- Emits a position beacon (`PBEACON`) and, optionally, a status beacon
  (`IBEACON`).

## Requirements

- An RTL-SDR USB dongle attached to the host running Home Assistant OS.
- A valid amateur radio callsign and its APRS-IS passcode. The passcode is
  derived from the callsign; `-1` authenticates a read-only feed and cannot gate
  packets, so it will not work here.

## Options

| Option                             | Description                                                         |
| ---------------------------------- | ------------------------------------------------------------------- |
| `mycall`                           | Callsign with SSID (e.g. `N0CALL-10`).                              |
| `iglogin_passcode`                 | APRS-IS passcode for `mycall`.                                      |
| `igserver`                         | APRS-IS Tier 2 server (e.g. `noam.aprs2.net`).                      |
| `frequency`                        | RF frequency for `rtl_fm` (e.g. `144.390M`).                        |
| `rtl_device`                       | RTL-SDR device index or serial number (`0` if you have one dongle). |
| `rtl_gain`                         | RTL-SDR gain in dB. Empty for automatic gain.                       |
| `rtl_ppm`                          | Frequency correction in ppm.                                        |
| `latitude` / `longitude`           | Decimal-degree position used by the beacon.                         |
| `beacon_comment`                   | Comment text appended to the position beacon.                       |
| `beacon_symbol` / `beacon_overlay` | APRS symbol and overlay for the beacon.                             |
| `beacon_delay` / `beacon_every`    | Direwolf beacon scheduling (`mm:ss`).                               |
| `send_status_beacon`               | Whether to emit an `IBEACON` status report.                         |

## Statistics

With `mqtt_enabled`, the add-on publishes its statistics to Home Assistant over
MQTT discovery as a single device. Both a total and a rate are published for the
three packet counters:

| Entity                        | Notes                                                          |
| ----------------------------- | -------------------------------------------------------------- |
| Packets gated to APRS-IS      | Direwolf's own total. It restarts with the add-on.             |
| Packets gated to APRS-IS rate | Packets per minute, from consecutive status beacons.           |
| Packets from APRS-IS          | As above, for packets pulled down from APRS-IS.                |
| Packets from APRS-IS rate     |                                                                |
| RF packets received           | Counted here, from decode lines, in real time.                 |
| RF packets received rate      |                                                                |
| Stations heard (RF)           | Stations over Direwolf's rolling window, not a total.          |
| Stations heard direct         | As above, heard without a digipeater.                          |
| Unique stations seen          | Distinct callsigns since the add-on started.                   |
| Audio level                   | Receiver level from the decode lines. Low means a weak signal. |
| Last packet heard             | Timestamp of the most recent RF decode.                        |
| IGate connected               | The APRS-IS server's answer to the login.                      |

A rate is what most dashboards want: the totals reset whenever the add-on
restarts, whereas a rate says what the gate is doing now. The totals are still
worth keeping, because Home Assistant records them as `total_increasing` and so
handles those resets in its own long-term statistics.

Each rate is averaged over at least ten minutes, and over longer when its source
is slower — the two APRS-IS counters only move when Direwolf emits its status
beacon, so those rates span whatever `beacon_every` is set to. A rate reads
**Unknown** until it has two samples to subtract, which after a restart means
one `beacon_every` for the APRS-IS pair, and it returns to **Unknown** if its
source stops reporting, rather than leaving the last figure standing on a gate
that has stopped working.

## How It Runs

The add-on spawns `rtl_fm` and `direwolf` as siblings connected through a FIFO,
not a shell pipeline. If either process exits, the survivor is terminated and
the container exits with status 1. Enable the add-on's **Watchdog** option if
you want the Supervisor to restart it automatically -- that setting is off by
default, and without it a failure (an unplugged dongle, a USB reset) leaves the
add-on stopped until you start it again. On `SIGTERM` (add-on stop), both
children get `SIGTERM` with a fixed 5 s grace window before `SIGKILL`.

## Troubleshooting

**`No supported devices found`.** `rtl_fm` could not open the dongle. Check that
`rtl_device` matches an attached device — the add-on passes it straight to
`rtl_fm -d`, which accepts either an index or a serial number. A serial is worth
using whenever more than one dongle is attached, since indices can move between
boots.

**Nothing decodes but the log looks healthy.** Confirm the dongle is tuned
correctly before suspecting the software: a wrong `rtl_ppm` on a cheap dongle
will offset you far enough to miss packets entirely. APRS is bursty — several
quiet minutes is normal on a weak antenna.

## Notes

- This is a receive-only IGate. Transmit support is out of scope for now.
- The passcode is logged as `[redacted]` in the rendered `sdr.conf` echo.
- Direwolf logs to stdout, which surfaces as the add-on log.
