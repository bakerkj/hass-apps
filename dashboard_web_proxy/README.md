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
    upstream_scheme: http # optional, http|https, default http
    upstream_ssl_verify: false # optional, only meaningful with https, default false
    head_prepend: "" # optional; HTML spliced before </head> (see below)
    listen_port: 18800 # must be one of 18800-18819
```

Then embed it — e.g. a Webpage dashboard pointing at
`http://<this-host>:18800/`.

For an HTTPS-only device UI, set `upstream_scheme: https` (and typically leave
`upstream_ssl_verify` at its default `false`, since LAN devices normally present
self-signed certs). The proxy sends SNI so vhost-based upstreams work.

`head_prepend` is an arbitrary HTML fragment that the proxy splices in just
before the upstream response's `</head>`. Any per-device workaround — a `<meta>`
tweak, a polyfill, a `<script>` that stubs an object the SPA expects — lives
here, in operator configuration, so the add-on itself doesn't grow a new option
every time a device firmware changes shape.

Contract:

- Anchored on `</head>`; the response must contain that close tag verbatim.
- The upstream `Content-Encoding` is cleared for sites with a non-empty
  `head_prepend` so nginx `sub_filter` sees plain HTML.
- The string is inserted verbatim into an nginx `sub_filter` replacement, so it
  must not contain a literal `'` (would close the enclosing string), `</head>`
  (would confuse the anchor), or `$` (nginx variable-interpolates the
  replacement regardless of quoting — a template-literal `${…}` in JS would fail
  `nginx -t` and take the container down, and a name that collides with a real
  nginx variable would silently substitute request state into the served HTML).
  All three are rejected at addon start with a clear error.
- The proxy performs no authentication, so treat `head_prepend` as running with
  the device UI's own trust — anything you inject executes same-origin with it.

Example — a device SPA that runs cross-origin-nested throws `SecurityError` on
`window.parent`/`window.top` reads and any click-handler that touches them dies.
This snippet swallows those reads (returning a chainable no-op), and routes a
named global (`layer`, from layui's popup lib loaded locally) through so the
SPA's `parent.layer.open(...)` calls still work:

```yaml
head_prepend: >-
  <script>(function(){try{void window.parent.location.href;return}catch(_){}var
  s=new Proxy(function(){},{get:function(_,k){return
  k==="layer"?window.layer:s},apply:function(){return s},set:function(){return
  true}});Object.defineProperty(window,"parent",{configurable:true,get:function(){return
  s}});Object.defineProperty(window,"top",{configurable:true,get:function(){return
  s}});})();</script>
```

The same-origin check at the top keeps the shim from breaking legitimate
sub-frames that need to reach their real parent. Leave `head_prepend` empty for
a plain pass-through — that's the default and preferred where a device doesn't
need help.

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
