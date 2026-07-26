# Dashboard Web Proxy

Reverse-proxies a LAN device's web UI so it can be embedded in a Home Assistant
dashboard (a Webpage/iframe view). Many device web interfaces set
`X-Frame-Options: SAMEORIGIN`, which makes browsers refuse to frame them; this
add-on strips that header, rewrites the session-cookie domain so login persists
through the proxy, and rewrites absolute redirects back to the proxy host.

**One device per port.** Each entry in `sites` listens on its own port and
forwards to one upstream device, so several devices can be proxied behind a
single host (e.g. a Tailscale machine name) without needing per-device DNS
names.

## Options

```yaml
sites:
  - name: mydevice
    upstream: 192.0.2.10
    upstream_port: 80 # optional, default 80
    listen_port: 18800 # must be one of 18800-18819
```

Then embed it — e.g. a Webpage dashboard pointing at
`http://<this-host>:18800/`.

## Notes

- Works only for web apps that use **relative** URLs (as most device SPAs do);
  apps with hardcoded absolute links would need HTML rewriting, which this does
  not do.
- The proxy performs no authentication of its own -- anyone who can reach the
  listen port (including over Tailscale) reaches the device's own login page,
  which is the only gate. The device's `upstream` is admin-set, so treat the
  listen ports as trusted.
- **Stripping `X-Frame-Options`/CSP removes the device's clickjacking
  protection** -- that is exactly what makes the UI embeddable, but it also lets
  any page frame the proxied UI. Keep the listen ports on a trusted network
  (e.g. Tailscale) and don't expose them to the open internet.
- For a device whose login uses a `SameSite` session cookie, embedding only
  works when Home Assistant and this proxy are reached via the **same
  registrable domain** (e.g. the same Tailscale host name, different port =
  still same-site). If you open HA from a different origin (a LAN IP, `*.local`,
  or the Nabu Casa cloud URL) than the proxied port, the browser may drop the
  cookie and login won't stick; use the same host name for both, or put the
  proxy behind TLS.
- Ports are fixed at 18800-18819 (Home Assistant add-on ports are static) -- a
  high, uncommon range chosen to avoid colliding with other add-ons/services. If
  a port is ever taken, change its **host** port in the add-on's Network
  settings (the `listen_port` stays the same); add more to `config.json` for
  extra sites.
