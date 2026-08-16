# Container Hooks

This add-on runs per-container hook scripts whenever a Docker container on the
host starts. It subscribes to the Docker events stream so hooks fire on every
container start (boot, manual restart, add-on update), not just at boot.

It runs in parallel to the community
[Run On Startup.d](https://community.home-assistant.io/t/run-on-startup-d/271008)
add-on. The two use distinct script directories so both can be installed at the
same time.

## Why

You want to change another add-on's behavior — patch a bug, neutralise an
unwanted feature, slot a configuration file into place — without forking and
rebuilding its image. Once the upstream image is fixed (or your fork lands
upstream) you delete the hook and you're done; nothing to maintain.

There are four intervention points, ordered earliest-to-latest in the
container's lifecycle. The first three all fire on the docker `create` event
(pre-start); the fourth fires on `start` (post-start):

1. **Stage a file tree into the target's writable layer** via `pre-start-files/`
   — declarative, fastest (~10-30 ms), no shell.
2. **Apply unified-diff patches** against files already inside the target via
   `pre-start-patches/*.patch` — for tweaking a few lines of an existing file
   without shipping a whole replacement.
3. **Run a script in the add-on container** (with docker CLI access) via
   `pre-start/*.sh` — for branching or scripted logic the declarative paths
   can't express.
4. **Run a script inside the target container** via `scripts/*.sh` — on the
   `start` event, after the target's entrypoint is up.

### Timing reality

Pre-start interventions are racing against the docker daemon's `create → start`
transition (typically 50-200 ms on a Home Assistant box) and against whatever
the target does very early in its boot (s6-overlay's `cont-init.d` scan, the
entrypoint reading its config, the language runtime importing modules). The fast
paths usually win comfortably — `put_archive` lands in ~10 ms median, the patch
path in ~15-150 ms — but **none of this is guaranteed**. A loaded box, a slow
docker daemon, an aggressive entrypoint, or a target that reads its config the
instant it starts can all close the window.

If you need a deterministic intervention point, prefer:

- **Files that the target's own init mechanism explicitly waits for** — e.g.
  dropping a script into `/etc/cont-init.d/` on s6-overlay images, which the
  supervision tree blocks on before launching any service.
- **Language-level monkey-patches** that ride the target's own initialisation —
  e.g. a Python `sitecustomize.py` shipped via `put_archive`, which the
  interpreter loads at startup before any user code. This dodges the race
  entirely because you're not modifying the target's behavior on disk; you're
  inserting code that the target will unconditionally execute as part of its
  normal startup.

The post-start path (`scripts/`) doesn't race against anything time-critical
inside the target, so it has no timing constraints worth noting beyond "the
target has to be running first."

## What It Does

- Subscribes to container lifecycle events via the docker socket using
  `aiodocker`. Auto-reconnects on docker daemon hiccups.
- Optional initial sweep over already-running containers when the add-on itself
  starts, so hooks aren't skipped for containers that came up before the add-on
  did.
- For each event, looks under `<base_dir>/<container>/` for hooks (post-start
  scripts, pre-start files, pre-start patches, pre-start scripts). Output is
  captured to per-container log files under `<base_dir>/<container>/logs/`.
- Pre-start (`create` event) hooks land files inside the target's writable layer
  before its entrypoint runs — see "Pre-Start Hooks" below.
- Per-container debounce window suppresses rapid re-fires from Supervisor
  watchdog flaps.
- Architecture is `asyncio`-based: each event dispatch is a concurrent task, so
  a slow hook on one container doesn't block hooks for another.

## Layout

Everything for a single container lives under one directory:

```
<base_dir>/<container_name>/
├── scripts/             # post-start (*.sh, lex-sorted, run inside the target)
├── pre-start/           # pre-start scripts (*.sh, lex-sorted, run in add-on)
├── pre-start-files/     # pre-start file tree (put_archive into target root)
├── pre-start-patches/   # pre-start unified diffs (*.patch, lex-sorted)
└── logs/
    ├── post-start.log   # scripts/ output
    └── pre-start.log    # pre-start*/ output
```

> **Host-side path:** drop files at
> **`/config/container_hooks/<container_name>/`** on the host. The add-on
> container sees the same tree at
> `/homeassistant/container_hooks/<container_name>/` because Supervisor maps
> `homeassistant_config:rw` for us.

`base_dir` is the add-on's view of that tree (default
`/homeassistant/container_hooks/`); change it only if you also remap the
underlying mount.

The directory name must equal the docker container name **exactly** — it is
matched literally, and a directory that matches nothing is silently inert: no
hook runs, and nothing is logged, because logs are written per matched
container. Read the name off the host rather than assuming a pattern:

```sh
docker ps --format '{{.Names}}'
```

Supervisor names add-on containers `app_<slug>_<name>` (e.g.
`app_xxxxxxxx_esphome`). Older Supervisor releases used an `addon_` prefix, so a
tree set up under the old scheme stops firing after an upgrade — with no error,
since an unmatched directory is indistinguishable from one that was never meant
to match.

### Ordering

Files inside `scripts/`, `pre-start/`, and `pre-start-patches/` run in
**lexicographic** order of their filenames. The convention is to prefix with
`00-`, `10-`, `20-`, …:

```
app_xxxxxxxx_esphome/scripts/
├── 00-first.sh
├── 10-second.sh
└── 20-third.sh
```

All files in a directory run sequentially on a single dispatch, sharing the same
env-var context. Container-level debounce still applies once per dispatch, so a
Supervisor watchdog flap won't run the whole set twice.

## Hook Script Environment

Each hook receives these environment variables. Post-start hooks pass them
through the aiodocker `container.exec(..., environment=[...])` API (not the
`docker exec -e` CLI, which the add-on doesn't shell out to); pre-start hooks
inherit them as process env from the addon-side
`asyncio.create_subprocess_exec`.

The `ROCS_` prefix is a vestige of this add-on's previous name
(`run_on_container_start`). It is kept for backwards compatibility with hooks
written against the old name.

| Variable            | When set                            | Value                                               |
| ------------------- | ----------------------------------- | --------------------------------------------------- |
| `ROCS_REASON`       | always                              | `initial_sweep`, `event_start`, `container_created` |
| `ROCS_CONTAINER`    | always                              | container name                                      |
| `ROCS_CONTAINER_ID` | `event_start` / `container_created` | docker container ID from the event                  |
| `ROCS_IMAGE`        | `event_start` / `container_created` | image name from the event                           |
| `ROCS_TIMESTAMP`    | `event_start` / `container_created` | docker event time (unix seconds)                    |

Example hook script that branches on context:

```bash
#!/bin/bash
case "$ROCS_REASON" in
  initial_sweep)
    # Add-on just started; container was already running.
    ;;
  event_start)
    # Container just (re)started; hook runs early in its lifetime.
    ;;
  container_created)
    # Pre-start window; target is created but not yet started.
    ;;
esac
```

## Configuration

| Option                | Default                          | Description                                                                                                                                                                                                                    |
| --------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `log_level`           | `INFO`                           | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`).                                                                                                                                                                       |
| `base_dir`            | `/homeassistant/container_hooks` | Root of the per-container hook tree. See "Layout" above.                                                                                                                                                                       |
| `initial_sweep`       | `true`                           | Process currently-running containers when the add-on starts.                                                                                                                                                                   |
| `debounce_seconds`    | `2`                              | Per-container debounce window for the post-start `scripts/` path only, in seconds, 0-60 (`0` disables). Pre-start hooks (`pre-start-files/`, `pre-start-patches/`, `pre-start/`) bypass debounce — see "Debounce scope" below. |
| `skip_containers`     | `[]`                             | Full docker container names to ignore (e.g. `app_xxxxxxxx_esphome`). The add-on always skips its own container by resolved full name in addition to anything listed here.                                                      |
| `container_overrides` | `[]`                             | Per-container overrides. See "Per-Container Overrides" below.                                                                                                                                                                  |

### Per-Container Overrides

```yaml
debounce_seconds: 2 # global default
container_overrides:
  - container: app_xxxxxxxx_esphome
    debounce_seconds: 0 # never debounce this one
  - container: app_flapping_thing
    debounce_seconds: 10 # longer window for a noisy watchdog
```

Only set the fields you want to override; everything else falls through to the
global defaults. Today only `debounce_seconds` is overridable; this shape leaves
room for more per-container knobs without breaking existing config.

### Debounce scope

`debounce_seconds` applies **only to the post-start `scripts/` path** (the
`start`-event dispatch). It does not gate any pre-start hook:

| Hook                        | Event    | Debounced? |
| --------------------------- | -------- | ---------- |
| `pre-start-files/`          | `create` | No         |
| `pre-start-patches/`        | `create` | No         |
| `pre-start/*.sh`            | `create` | No         |
| `scripts/*.sh` (post-start) | `start`  | Yes        |

Pre-start hooks fire once per container lifecycle (each `create` is unique to a
new container), so a debounce window would never apply to them anyway. Patches
and staged files persist for the container's lifetime in the writable layer; a
stop/start of the same container doesn't re-fire pre-start hooks because it
doesn't re-fire `create`.

Debounce is **leading + trailing**: the first `start` event in a window fires
immediately, and if any further `start` events arrive while the window is still
open, one **trailing** fire runs at the end of the window with the most recent
event. So a watchdog flap that lands two restarts close together always gets at
least one post-start run for the latest container state — the second restart
isn't silently dropped if no third event arrives.

## Pre-Start Hooks (create events)

The add-on subscribes to docker container `create` events. The daemon emits
`create` between the container's writable filesystem layer existing and its
entrypoint running — for any container on the host (Supervisor add-ons,
`docker compose`, plain `docker run`), not just Supervisor-managed ones. This is
the only window in which you can stage files into the target's writable layer
before its entrypoint reads them.

Three pre-start mechanisms are available; all fire on the same event.

### Fast path: `pre-start-files/`

Drop a tree at `<base_dir>/<container>/pre-start-files/` and the add-on tars it
up and `put_archive`'s it into the target container's root in a single HTTP PUT
against the docker socket. Path within the tree maps 1:1 to the target — e.g.
`pre-start-files/etc/cont-init.d/00-rocs-probe` lands as
`<container>:/etc/cont-init.d/00-rocs-probe`.

```
<base_dir>/app_xxxxxxxx_esphome/pre-start-files/
└── etc/
    └── cont-init.d/
        └── 00-my-init       # s6 runs this before any service starts
```

No bash, no docker CLI — typically ~10-30 ms total. Use this for tight race
budgets or when you just want declarative file staging.

> **Mode preservation:** the file mode is tarred verbatim from the source. If
> the target init system expects the file to be executable (e.g. s6's
> `/etc/cont-init.d/*` scripts), `chmod +x` your source file on the host —
> otherwise the file lands with whatever your umask produced (usually 644) and
> s6 silently skips it.
>
> **Symlinks are preserved as symlinks** — `pre-start-files/foo` pointing at
> `bar` ships to the target as a symlink, not as `bar`'s contents. Use this when
> you want a stage-link farm; if you want the contents, drop the file itself in
> the tree.

### Patch path: `pre-start-patches/*.patch`

Drop unified-diff `*.patch` files at
`<base_dir>/<container>/pre-start-patches/`. For each patch, the add-on:

1. parses the affected paths out of the diff,
2. `get_archive`s the current copy of each file from the to-be-started target,
3. applies the patch in memory (pure Python via `patch-ng` — no `patch`
   subprocess),
4. `put_archive`s the result back into the target.

```
<base_dir>/app_xxxxxxxx_esphome/pre-start-patches/
├── 00-disable-foo.patch
└── 10-rename-bar.patch
```

Patches sort lexicographically — prefix with `00-`, `10-`, `20-` to control
order. The file mode of the original (e.g. executable bit on a script) is
preserved through the round-trip.

Use this when you only want to tweak a few lines of an existing file inside the
target rather than ship a whole replacement. Median ~15 ms but watch the docker
socket contention note in [Timing reality](#timing-reality) above — observed
tail can land at ~150 ms.

### Script path: `pre-start/*.sh`

When you need branching or scripted logic:

```bash
#!/bin/bash
# <base_dir>/app_xxxxxxxx_esphome/pre-start/00-stage-patch.sh

docker cp \
  "/homeassistant/container_hooks/$ROCS_CONTAINER/pre-start-files/usr/local/lib/python3.13/site-packages/foo/bar.py" \
  "$ROCS_CONTAINER:/usr/local/lib/python3.13/site-packages/foo/bar.py"
```

Pre-start hooks run **in the add-on container**, not inside the target — the
target isn't running yet. The script has docker CLI access and is expected to
use it (typically `docker cp`) to stage files into `$ROCS_CONTAINER`. Source
files must live somewhere the add-on can read; `/homeassistant/...` (which the
add-on maps via `homeassistant_config:rw`) is always available. Slower (~70-150
ms) than the fast path because of bash + docker CLI startup.

### Ordering across the three pre-start paths

If multiple pre-start paths are configured for the same container, they fire in
this order on each `create` event:

1. `pre-start-files/` (put_archive, fastest)
2. `pre-start-patches/*.patch` lex-sorted (get_archive + patch + put_archive)
3. `pre-start/*.sh` lex-sorted (bash, slowest)

Time-critical staging runs first; scripted logic runs last. All three share the
same race against Supervisor's subsequent `docker start` and the target's early
init — see [Timing reality](#timing-reality).

A failing stage logs a warning to `pre-start.log` and the add-on log, then the
remaining stages **still run**. The add-on does not abort the dispatch on the
first failure — if the `put_archive` step can't reach the target, the patch and
script stages still try. This is deliberate: pre-start hooks are best-effort,
and partial application is usually preferable to all-or-nothing.

Pre-start output is captured to `<base_dir>/<container>/logs/pre-start.log` so
it doesn't mix with the post-start hook's log.

## Requirements

- `docker_api: true` — needed for the events stream + `put_archive` +
  `container.exec` API calls.
- `hassio_role: manager` — needed to operate against other add-on containers.
- `map: homeassistant_config:rw` — mounts the user's config dir at
  `/homeassistant` inside the add-on so the per-container tree is visible.
- Protection mode must be **off** for the docker socket to be mounted and for
  `container.exec` to reach into target containers.

## Lifecycle and Limits

- Post-start hooks (`event_start` / `initial_sweep`) are shipped into the target
  via `put_archive` and run with aiodocker's `container.exec` API. No `docker`
  CLI is invoked.
- Pre-start hooks (`container_created`) run in the add-on container and have a
  brief window before the target's entrypoint starts. See "Pre-Start Hooks
  (create events)" above.
- Hook output is captured to per-container log files but is not buffered to the
  add-on's own log; check `<base_dir>/<container>/logs/post-start.log`
  (post-start) or `<base_dir>/<container>/logs/pre-start.log` (pre-start) for
  the script's stdout/stderr.
- An exit code other than zero is logged but does not cause the add-on to retry.
- **Concurrent dispatches** are capped at **10**. A burst (e.g.
  `docker compose up` of 50 containers) queues cleanly behind the cap rather
  than overloading the docker socket.
- **Graceful shutdown** is signal-escalated. The first `SIGTERM`/`SIGINT` stops
  accepting new events and drains in-flight dispatches; a second signal cancels
  in-flight tasks immediately (which can leave a target with a partially-staged
  `put_archive` tree); a third hard-exits. So if you re-send `TERM` while the
  add-on shows `draining N in-flight tasks`, you're cancelling
  mid-`put_archive`/`exec`.
