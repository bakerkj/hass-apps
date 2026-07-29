# HALPI2 Power

Runs the Hat Labs HALPI2 power controller daemon (`halpid`) inside Home
Assistant OS, and publishes controller telemetry as MQTT Discovery entities.

## Why this exists

The HALPI2 carrier has an RP2040 power controller on I2C bus 1 at address
`0x6d`, backed by supercapacitors. With **no host daemon talking to it**, the
controller runs in **Solo** mode: during a power outage it holds the Compute
Module up on the caps, but it has no way to shut the OS down cleanly, so power
is eventually cut mid-write. On a boat that means filesystem corruption after
every power interruption.

Running `halpid` restores **Co-op** mode: the daemon polls the controller, sees
the input voltage collapse, and shuts the host down gracefully _before_ the
supercapacitors are exhausted.

Stock Home Assistant OS already provides everything else this needs — `i2c-dev`
ships in the image, and `/dev/i2c-1` appears once `dtparam=i2c_arm=on` is set in
`config.txt`.

## Entities

Published under one HA device ("HALPI2 Power Controller"):

| Entity                 | Unit |
| ---------------------- | ---- |
| DC input voltage       | V    |
| Supercapacitor voltage | V    |
| Input current          | A    |
| MCU temperature        | °C   |
| PCB temperature        | °C   |
| Power state            | text |

**Power state is the one to watch.** `OperationalCoOp` means the daemon is
coordinating with the controller and graceful shutdown is active.
`OperationalSolo` means it is not — the add-on logs a warning in that case.

Note the HALPI2's LED bar only turns **green** when a daemon is connected to the
controller. Before installing this add-on a perfectly healthy machine shows
yellow, which is easy to misread as a fault.

## Modes

`mode` selects how the daemon relates to the power controller:

| mode             | Controller state  | Power outage behaviour                    |
| ---------------- | ----------------- | ----------------------------------------- |
| `coop` (default) | `OperationalCoOp` | clean shutdown before the caps run out    |
| `solo`           | `OperationalSolo` | abrupt power cut; telemetry keeps working |

`solo` is useful if you want the sensors without the daemon taking
responsibility for power, or while you are still evaluating the watchdog's
behaviour on your own hardware. It is _not_ the safe default — see below.

## The hardware watchdog — required for Co-op mode

The controller has a watchdog that **hard power-cycles the machine** if it stops
seeing I2C traffic. The firmware feeds it on _any_ I2C operation, so `halpid`'s
regular polling keeps it alive, and `halpid` disarms it on a clean shutdown.

**The watchdog is not optional protection — it is what puts the controller into
Co-op mode.** Verified on hardware:

| `watchdog_timeout_ms` | Controller state  | Graceful power outage shutdown |
| --------------------- | ----------------- | ------------------------------ |
| `10000` (default)     | `OperationalCoOp` | **yes**                        |
| `0`                   | `OperationalSolo` | **no**                         |

So setting it to `0` does not merely forgo crash recovery: it leaves the
controller in Solo, and a power outage ends in the same abrupt power cut you
would get with no add-on installed at all. Telemetry still publishes, but the
reason this add-on exists is gone. The `power_state` entity shows which mode you
are in, and the add-on logs a warning whenever it sees Solo.

Use `mode: solo` rather than setting `watchdog_timeout_ms: 0` directly — the
add-on derives the watchdog from the mode, so the two cannot drift out of sync.
In `coop` a zero timeout is rejected at startup with an explanatory error rather
than silently leaving the controller in Solo.

The trade-off to be aware of: with the watchdog armed, any gap longer than the
timeout with no I2C traffic triggers a hard reset. `halpid` disarms it on
SIGTERM, so a clean add-on stop or update is safe — but a container that is
SIGKILLed will leave it armed and the machine will power-cycle. Time an add-on
restart, an add-on update, and a Home Assistant OS update on your own hardware
before relying on this at sea.

The maximum is 65535 ms (~65 s); the value is a `u16` in the controller
protocol.

## Tuning when it shuts down

Two options decide when the daemon gives up on the input supply:

| Option                       | Meaning                                                              |
| ---------------------------- | -------------------------------------------------------------------- |
| `power_outage_voltage_limit` | volts below which the input is considered lost (default 9.0)         |
| `power_outage_time_limit`    | seconds it must stay below that before shutdown starts (default 5.0) |

Raising `power_outage_time_limit` rides out longer dropouts — engine cranking, a
loose connection, switching between shore and battery — at the cost of spending
supercapacitor charge before the shutdown even begins. Lowering it starts the
shutdown sooner and leaves more reserve to complete it.

**The right value depends on how long your machine actually takes to shut down,
which is a measurement, not a guess.** Run the power outage test, then compare
the timestamp in `/share/halpi2_power/power-outage-events.log` against the final
journal entry of that boot. That difference is the real shutdown duration; the
supercapacitor hold-up time must exceed `power_outage_time_limit` plus that.

## Estimating when it will shut down

Two entities make the countdown visible:

| Entity                   | Meaning                                                                                                              |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| **Power outage elapsed** | seconds the input has been below the threshold (0 when nominal)                                                      |
| **Shutdown in**          | seconds until shutdown starts — the full `power_outage_time_limit` when nominal, counting down during a power outage |

A power outage is recognised from _either_ the controller reporting a
power-outage state or `V_in` falling below the limit, so the countdown appears
immediately rather than waiting for the controller to change state.

> **A note on naming:** halpid internally calls a power outage a _blackout_. You
> will see that term only in its own surfaces — the raw `state` values it emits
> (`BlackoutCoOp` / `BlackoutShutdown`) and the generated `halpid.conf` keys
> (`blackout-*`). This add-on uses _power outage_ everywhere it controls the
> wording.

**Reading and publishing run at different rates, deliberately.** halpid is read
every **0.5 seconds**; telemetry is published every `interval_seconds` (default
10).

That split is not an optimisation, it is a correctness requirement. halpid shuts
down after `power_outage_time_limit` — 5 s by default. Reading only every 10 s
means a power outage can begin _and end_ between two reads, so roughly half of
all events would never be observed at all. Detection must be faster than the
deadline.

One honest limitation: halpid keeps its own power outage timer internally and
does not expose it, so these entities are the add-on's independent measurement
of the same interval. They track closely but are not the daemon's authoritative
countdown — halpid decides, this reports.

## Permissions

- `devices: ["/dev/i2c-1"]` — access to the power controller.
- `hassio_role: manager` — required to call `POST /host/shutdown`. This is the
  narrowest role that permits it (`admin` is **not** required), but it still
  grants broad Supervisor access. If you would rather not grant it, set
  `shutdown_via: mqtt_only` and drive shutdown from a Home Assistant automation
  reacting to the published power state.

## Upstream software

This add-on builds [`halpid`](https://github.com/hatlabs/HALPI2-rust-daemon),
which is **BSD-3-Clause licensed, © Hat Labs Oy**. The upstream licence is
included in the image at `/LICENSE.halpid`.

The build pins a specific upstream release and applies a small patch making the
watchdog timeout configurable (upstream hardcodes it). The patch lives in
`patches/`, and the build fails loudly if a version bump makes it stop applying.

_This add-on is not affiliated with, endorsed by, or supported by Hat Labs Oy._

## Verifying a power outage shutdown after the fact

By the time you look, the machine has power-cycled. Two independent records
survive:

**1. Was the shutdown clean?** The host journal is persistent across boots. Look
at the tail of the boot that was cut:

```
ha host logs --boot -1 | tail -40
```

A clean shutdown ends with an unmistakable sequence:

```
Unmounting HAOS boot partition...
Unmounting Docker persistent data...
EXT4-fs (...): unmounting filesystem ...
Stopping Flush Journal to Persistent Storage...
```

A hard power cut produces none of that — the journal simply stops mid-stream.
That difference is the actual pass/fail of the power outage test.

**2. Why did it shut down, and at what voltage?** Add-on stdout does _not_ reach
the persistent host journal (verified: the previous boot's journal contains
nothing from this add-on), so the shutdown script records its own evidence to
`/share/halpi2_power/power-outage-events.log`, which survives the power cycle:

```
=== 2026-07-22T03:14:07-04:00 power outage shutdown requested ===
  values: {"V_in":8.21,"V_cap":9.14,"state":"BlackoutShutdown",...}
  result: Supervisor accepted /host/shutdown
```

It is written to `/share` rather than the add-on's `/data` deliberately: an
add-on's `/data` volume is private to that add-on, so evidence stored there
cannot be read from an SSH or Samba add-on afterwards -- which is exactly when
you need it. `/share` crosses add-on boundaries. The directory is created at
start-up rather than during the power outage, both to keep the time-critical
path short and so the mapping can be verified without waiting for a real event.

If the journal shows a clean shutdown **and** the event log shows the request,
the whole chain worked. A request logged with no clean shutdown in the journal
means the Supervisor call succeeded but the OS did not finish in time — which
would argue for raising `power_outage_time_limit` so shutdown starts earlier.
