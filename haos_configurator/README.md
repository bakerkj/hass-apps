# HAOS Configurator

> **_USE AT YOUR OWN RISK!!_**
>
> This add-on writes arbitrary files to arbitrary host paths and runs arbitrary
> commands on the host (whatever the manifest declares). Please review the
> source and fully understand what it does before you install it anywhere — the
> burden is on you to evaluate this add-on.

A one-shot Home Assistant add-on that installs persistent host files into HAOS
according to a user-supplied manifest. You drop your files and a `manifest.yaml`
into the add-on's config directory, start the add-on, and the listed files are
written to the host with per-file on-change actions (reload udev, run an init
script, etc.).

The add-on itself is generic — it has no opinion about _which_ files belong on
the host. The [`examples/`](examples/) directory in this repo contains a sample
media-mount udev rule that you can copy and adapt; the add-on will not install
anything until you put a manifest in place yourself.

## The add-on's config directory

Supervisor mounts the add-on's config directory at `/config` inside the
container. On the host, that directory appears as

```
/addon_configs/<repo_id>_haos_configurator/
```

For this add-on installed from `github.com/bakerkj/hass-apps`, Supervisor
assigns the repo prefix `0f7b38ce`, so the full path is

```
/addon_configs/0f7b38ce_haos_configurator/
```

(`<repo_id>` is an 8-character prefix Supervisor derives from the add-on
repository URL; if you fork this repo and install from a different URL, the
prefix changes. Either way the in-container path is `/config`, so the add-on
works regardless.)

Drop `manifest.yaml` and any source files it references into
`/addon_configs/0f7b38ce_haos_configurator/` (the same path File editor and
Studio Code Server display). It survives add-on rebuilds and HA upgrades.

## Manifest

`manifest.yaml` (in the add-on's config directory):

```yaml
files:
  - src: 80-haos-media.rules # relative to the add-on config dir
    dst: /etc/udev/rules.d/80-haos-media.rules # absolute host path
    on_change: [reload_udev, trigger_block]

actions:
  reload_udev:
    run: [udevadm, control, --reload-rules]
  trigger_block:
    run: [udevadm, trigger, --subsystem-match=block, --action=add]
```

- **`files[]`** — what to install. `src` is relative to the add-on's config
  directory; `dst` is absolute on the host. `mode` is optional; default is
  `0755` for `*.sh` and `0644` otherwise. **Always quote `mode`** so YAML
  doesn't reinterpret it as a decimal integer.
- **`on_change`** — a list of named actions to fire if (and only if) this file
  was actually written on this run.
- **`actions`** — a map of `name → {run: …}`. Commands run in the host's
  mount/uts/ipc namespaces via `nsenter -t 1`. Two forms of `run` are accepted:
  - **Array form** (recommended): `run: [argv0, argv1, …]`. Each element is one
    argv slot. No shell, so paths and arguments cannot shell-inject. No pipes,
    redirects, or env-var expansion.
  - **String form**: `run: "shell command"`. Exec'd via `sh -c`. Use this if
    your action genuinely needs shell features (pipes, redirects, env vars).

### How `on_change` is resolved

For each file the add-on hashes the source (locally, with Python's `hashlib`)
and the host's current destination (via `nsenter sha256sum`). If they differ,
the file is written and that file's `on_change` actions are queued. After all
files are processed, queued actions fire **once each**, in the order they appear
under `actions:`. If `reload_udev` is referenced by three different files, it
still fires only once.

Files whose content already matches the host are silently skipped — so
restarting the add-on on a quiet system does no work.

## Persistence model

The add-on follows the
[HAOSConfigurator/HassOsEnableSSH](https://github.com/adamoutler/HAOSConfigurator/tree/main/HassOsEnableSSH)
pattern: with `host_pid: true`, `full_access: true`, and `SYS_ADMIN`, it uses
`nsenter -t 1 -m` to enter the host's mount namespace and writes directly into
HAOS-persistent paths.

- `/etc/udev/rules.d/` is writable through the `hassos-overlay` overlay, so
  files written there persist across reboots.
- `/mnt/data/` is the persistent `hassos-data` partition.

The manifest and source files are read inside the container as plain Python file
IO from `/config`, which Supervisor bind-mounts from
`/addon_configs/0f7b38ce_haos_configurator/` on the host. Only the host-side
operations — hashing existing destinations, streaming source bytes into
HAOS-persistent paths, and running `on_change` actions — go through `nsenter`.
Every internal `nsenter` call passes argv as a list, so paths in the manifest
can contain spaces, quotes, or other shell metacharacters without injecting. The
add-on uses `sh -c` only when an `actions[].run` is given as a string;
array-form actions are exec'd directly without a shell.

## First run

The add-on does **not** auto-populate the config directory. You must supply your
own `manifest.yaml`.

If no manifest is present the add-on logs an error and exits. To get started,
copy something from the [`examples/`](examples/) directory in this repo into the
config directory and edit it.

## Options

| Option               | Default | Description                                                                                                |
| -------------------- | ------- | ---------------------------------------------------------------------------------------------------------- |
| `dry_run`            | `false` | Log what would be installed and which actions would fire, without writing or running anything.             |
| `apply_post_actions` | `true`  | Run queued `on_change` actions after a file is changed. Set `false` to install files without side effects. |
| `log_level`          | `DEBUG` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`.                                                                |

## Usage

1. Install the add-on. With `boot: manual` and `startup: once`, it only runs
   when you start it.
2. Copy your `manifest.yaml` and source files into
   `/addon_configs/0f7b38ce_haos_configurator/` (see
   ["The add-on's config directory"](#the-add-ons-config-directory) above).
3. (Recommended) Set `dry_run: true` and start the add-on once. The log will
   show what _would_ be installed and which actions _would_ fire, without
   touching the host. Confirm it matches what you expected.
4. Set `dry_run: false` and start the add-on. Confirm in the log that files were
   written and the expected actions ran.
5. Stop the add-on (it has already done its work). Reboot to confirm the on-host
   configuration survives a clean boot.

## Requirements & security

| Privilege           | Why                                                                                                           |
| ------------------- | ------------------------------------------------------------------------------------------------------------- |
| `full_access: true` | Required to enter host namespaces and write to host paths such as `/etc/udev/rules.d/` and `/mnt/data/sbin/`. |
| `host_pid: true`    | Needed so `nsenter -t 1` can target the host's PID-1 (init) and join its mount/uts/ipc namespaces.            |
| `SYS_ADMIN`         | Required to call `nsenter` and to re-trigger udev (`udevadm control` / `udevadm trigger`).                    |
| `apparmor: false`   | The default Home Assistant AppArmor profile blocks `nsenter` and writing to host paths.                       |

Protection mode must be **off** for this add-on (Home Assistant blocks
`nsenter`-style host access otherwise).

Because this add-on can write arbitrary files to arbitrary host paths and run
arbitrary commands on the host (whatever the manifest declares), audit its
source and verify what it does before installing. The burden is on you! **_USE
AT YOUR OWN RISK!!_**

## Notes

- The add-on does its work and then exits. Leaving it installed-but-stopped is
  fine. It will not re-run automatically at boot.
- To detect drift: a hand-edit on the host that diverges from the manifest's
  source will be detected by the sha256 compare and re-overwritten on the next
  start (with the source winning).
- Mode-only changes are not detected (only content). To re-set a host file's
  mode, also tweak its content — e.g., add a trailing newline.
