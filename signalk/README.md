# Signal K Server

[Signal K](https://signalk.org/) server for marine data, reading NMEA 2000 from
a SocketCAN interface (`can0`) and making it available to Home Assistant.

## What this provides

An NMEA 2000 CAN adapter (e.g. an MCP2518FD, as found on boards like the HALPI2)
appears as the SocketCAN interface `can0`, carrying raw NMEA 2000 frames.
Nothing in Home Assistant understands those. Signal K decodes them with
[canboatjs](https://github.com/canboat/canboatjs) into a normalised marine data
model — depth, speed, wind, position, engine data, tank levels — and can then
feed Home Assistant over MQTT.

## Requirements

`can0` must exist and be up at 250 kbps (the NMEA 2000 bit rate). How you bring
it up depends on your CAN hardware and OS — on Home Assistant OS the sibling
`haos_configurator` add-on can install the device-tree overlays and a udev rule
that do it (for the onboard controller on boards like the HALPI2, for example).

Check it before debugging Signal K itself:

```
ip link show can0
cat /sys/class/net/can0/statistics/rx_packets
```

If `rx_packets` stays at `0`, no NMEA 2000 traffic is reaching the machine and
no amount of Signal K configuration will help — check the backbone is connected
and powered, and that any 120 Ω termination on your CAN adapter is **off** (an
N2K backbone is already terminated at both ends).

The add-on logs this at startup so it is visible without going looking.

## Setting up the NMEA 2000 connection

The add-on intentionally ships no preconfigured connection: Signal K writes its
own configuration, and letting it do so avoids guessing at a schema.

1. Open the web UI on port 3000 (the default; change it with the add-on's `port`
   option)
2. Create an admin account when prompted
3. **Server → Connections → Add**
4. Data type **NMEA 2000**, source **Canbus (canboatjs)**, CAN interface `can0`

## Networking

Runs with `host_network: true`. This is **required**, not a convenience:
SocketCAN interfaces live in the host's network namespace, and the only way to
reach `can0` from a container namespace would be `vxcan`, which is not built in
the Home Assistant OS kernel. Because of host networking, the configured port
binds directly on the host — make sure it does not collide with anything else.

## Configuration persistence

Signal K keeps everything — connections, plugins, security, vessel identity — in
one directory, which this add-on points at `/data/signalk` so it survives add-on
updates.

## Upstream software and licensing

Built on the official
[`signalk/signalk-server`](https://hub.docker.com/r/signalk/signalk-server)
image (Alpine variant), which bundles
[`@canboat/canboatjs`](https://github.com/canboat/canboatjs). The bundled Signal
K server is **Apache-2.0** (© the SignalK contributors); this add-on's own
wrapper is MIT. This add-on is not affiliated with or endorsed by the Signal K
project.

## Known log noise

On startup you will see:

```
/home/node/signalk/startup.sh: line 54: service: not found
dbus_bus_get_private(): Failed to connect to socket /run/dbus/system_bus_socket
avahi-daemon ... exiting.
```

This is an upstream quirk, not a fault in this add-on: the image's `startup.sh`
uses Debian's `service` command, which does not exist in their own Alpine
variant, so D-Bus and Avahi fail to start. The only consequence is that Signal K
does not advertise itself over mDNS — the server, its API, and NMEA 2000
decoding are unaffected. Reaching it by IP or hostname works normally.

Granting the add-on `dbus: true` would let it use the host's D-Bus instead and
silence this, at the cost of a broader privilege. Not enabled by default.

## Why there is no ingress panel

Home Assistant ingress serves an add-on under a subpath
(`/api/hassio_ingress/<token>/`). Signal K's admin UI cannot be served that way:
its `index.html` mixes relative asset paths with **absolute** ones, and carries
no `<base href>`:

```
src="./assets/index-*.js                          relative  -- fine
src="/@signalk/app-dock/remoteEntry.js            absolute  -- breaks
src="/@signalk/signalk-to-nmea0183/remoteEntry.js absolute  -- breaks
```

Those absolute entries are the webpack Module Federation bundles for plugin
webapps. Under ingress they resolve against the Home Assistant root rather than
the add-on and 404, so the shell loads but plugin panels do not. The server also
issues an absolute redirect from `/` to `/admin/`.

Enabling ingress therefore produces a half-working panel, which is worse than
none, so it is deliberately disabled.

To reach the UI from the Home Assistant sidebar anyway, add an iframe panel in
`configuration.yaml` — Signal K is then served from its own root and everything
works:

```yaml
panel_iframe:
  signalk:
    title: Signal K
    icon: mdi:sail-boat
    url: "http://homeassistant.local:3000" # 3000 is the default; match your `port` option
    require_admin: true
```

Two caveats: Signal K keeps its own login (it runs with `--securityenabled`), so
this is not single sign-on; and if you reach Home Assistant over HTTPS from
outside, a browser will block an `http://` iframe as mixed content. Put Signal K
behind the same TLS terminator in that case.
