# Persistent CPU microVMs

`beam vm` creates a named resource with a durable writable root and stable
desktop, terminal, and application URLs. Every launch uses `use_vm`; GPU options
are rejected. The guest runs systemd as PID 1. Stop releases compute; start is a
cold boot of the same filesystem. Enabled user services start again normally.
Pause releases compute while preserving memory and running processes; start
resumes that checkpoint unless `--cold` explicitly discards it.

```sh
beam --context local vm new dev --desktop --docker-enabled --cpu 2 --memory 2048
beam --context local vm desktop dev --url
beam --context local vm ssh dev
beam --context local vm exec dev -- systemctl status
beam --context local vm stop dev
beam --context local vm start dev
```

The SDK exports `VM` from both `beta9` and `beam`:

```python
from beam import VM

vm = VM("dev", desktop=True, context="local").create()
print(vm.desktop_url)
vm.fs.upload_file("./app.service", "/etc/systemd/system/app.service")
vm.process.exec("systemctl", "daemon-reload").wait()
vm.process.exec("systemctl", "enable", "--now", "app.service").wait()
vm.stop()
vm.start()  # same root, machine ID, SSH host keys, and URLs
```

## Options and commands

New VMs accept `--image`, `--image-id`, `--dockerfile`, `--build-context`,
`--build-secret`, `--template`, `--desktop`, `--docker-enabled`, `--cpu`,
`--memory`, `--disk-size`, repeated `--env KEY=VALUE`, `--secret`, and `--port`,
plus `--ttl`, `--pool`, `--no-ssh`, `--sync`, and `--json`. `--context` retains
Beam's meaning: a saved connection profile. Docker build context therefore uses
`--build-context`. GPU and GPU capacity flags are deliberately absent.

Defaults are 1 CPU, 1024 MiB, and a 50 GiB root. Desktop defaults are 2 CPUs and
2048 MiB. SDK/CLI creation enables SSH by default and uploads only a local public
key. Unspecified resource options inherit from a template. The default base is
Ubuntu 22.04; custom bases must use Debian/Ubuntu with apt. Desktop image builds
currently require Ubuntu 22.04. Images include OpenSSH, rsync, ttyd, Docker,
Compose, Codex, and Claude Code. The Docker daemon starts only when requested.

Use `beam vm image build . -f Dockerfile --desktop` to build a reusable image
with VM services, then pass its result to `vm new --image-id`. Existing image IDs
must already contain these services. Registry images and Dockerfiles passed
directly to `vm new` receive the service layer automatically.

Lifecycle commands are `new`, `list`, `get`, `start`/`resume`, `stop`, `rm`,
`pause`, `fork`, and `update`. Access commands are `exec`, `ssh`, `scp`, `sync --watch`, `desktop`, and
`terminal`. `ports`, `expose`, and `unexpose` manage published ports.
`port-forward dev 15432:5432` binds localhost through an authenticated tunnel and
does not publish a URL. `prompt` supports `--agent claude|codex`, `--cwd`, and
`--detach`/`--background`. `logs --unit` reads the journal; `logs --session` reads
durable prompt logs. Exec, prompt, and logs work without SSH.

Creation also accepts `--auto-resume`, `--idle-action stop|pause`, repeated
`--metadata KEY=VALUE`, `--request-id UUID`, `--block-network` or repeated
`--allow-network`, repeated `--protected-port`, `--disk NAME:/MOUNT:SIZE`, and
`--volume NAME:/MOUNT`. `list --metadata KEY=VALUE --status running` filters
resources. `update` changes metadata and idle policy; `network` changes the
outbound firewall live and on subsequent launches. UUID creation requests can
be safely retried with the same body; changing that body returns a conflict.

`exec --detach --json` returns a process ID and its runtime identity. `ps`,
`logs --pid PID`, and `kill PID --container-id ID` reuse the sandbox process
manager. Process handles belong to a runtime; reacquire them after a launch.
Foreground exec maintains an idle activity lease and cancels the child at its
deadline. Detached work should use `--ttl 0`, or a client-owned `vm.keep_alive()`
context. `metrics` samples guest CPU, memory and root filesystem usage.

SDK storage uses the existing `DurableDisk` and `Volume` objects. Extra disks
must use qcow/ext4 and unique absolute mount paths. Shared volumes are external
mutable storage, so they are not included transactionally in memory snapshots.
VM removal owns only the VM's root, not attached disks or volumes.

```python
from beam import DurableDisk, VM, Volume

vm = VM("dev", desktop=True, auto_resume=True, idle_action="pause", ttl=300,
        metadata={"project": "editor"}, protected_ports=[8080, 7681],
        disks=[DurableDisk("dev-data", "10GiB", "/data")],
        volumes=[Volume("shared", "/shared")], context="staging").create()
with vm.keep_alive():
    vm.process.exec("python3", "train.py").wait()
vm.pause()
vm.start()  # live guest processes continue
```

`snapshot create`, `snapshot list`, `template create/list/show/rm`, and `fork`
reuse immutable durable-disk snapshots. Fork accepts a VM or an explicit VM
snapshot ID, with an optional new name. Templates remain available after deleting
their source VM. Forks and template launches get independent root disks, machine
IDs, SSH host keys, and URL handles. Templates are workspace-private;
`template create --public` is reserved and rejected, as in Tama.

## Persistence and systemd

The VM resource owns the disk `vm-<resource UUID>`. Runtime container IDs change
on every launch. The existing qcow driver restores the newest committed
generation of that disk; a fork's source snapshot is only its initial seed.
`/run` and `/tmp` are transient. System-managed machine identity, guest hostname,
SSH authorization, and boot environment are refreshed before user units start.
Other root files, installed packages, enabled units, and `/workspace` persist.

Standard systemd service, timer, socket, and target units are supported. The VM
environment is supplied through a private transient systemd manager configuration,
including workspace secrets. Services can override it using normal unit options.
Management responses omit environment values; forks and private templates retain
the stored launch environment.
The scheduler configures the guest network; networkd, NetworkManager, and resolved
are masked so they cannot replace that configuration. Systemd's guest agent is
ordered before sysinit at startup and after ordinary services during shutdown.

Stop commits the root before requesting poweroff, waits for worker finalization,
and then adopts the final committed generation, including shutdown-time writes.
`--no-snapshot` omits the visible snapshot artifact and still commits the root.
Snapshot or finalization errors are surfaced and retain the runtime identity for
recovery. Interrupted stops retain their intent and artifact identity in Postgres.
Lifecycle advisory locks prevent concurrent operations across gateway replicas.

Running VMs commit a recovery root approximately once a minute. Worker failure
may lose writes after the latest completed commit; this is not synchronous remote
storage. Snapshots are filesystem snapshots, not application transactions. This
version supports both cold stop/start and RAM pause/resume. VM service shutdown gets at least
120 seconds; raise the worker termination grace period for units that need longer.

Memory pause seals disk generations while the hypervisor is suspended, then
terminates compute without systemd shutdown. Warm resume requires the original
active workspace credential and unchanged paired disk generations. An incomplete
pause, unavailable checkpoint, or changed disk produces an error; `start --cold`
explicitly boots the latest durable storage. Referenced checkpoints are retained
by checkpoint garbage collection. RAM checkpoints resume the same VM; forks and
templates remain filesystem snapshots.

VMs have their own `vm` stub kind and `vm-` runtime prefix, allowing their
usage and pricing to be distinguished from sandboxes. New VM IDs are 16 hex
characters; generated names look like `calm-otter-a3b19f`. Explicit names and
existing VM identities continue to work.

`--ttl` is optional and measures idle time from SDK operations and active
HTTP/WebSocket/tunnel connections. Unset or `0` keeps the VM running. Expiry
snapshots the durable root and shuts down the microVM, retaining its name,
disk and URLs. `start` cold boots that root and enabled systemd services.
`--idle-action pause` also preserves RAM. Restarting explicitly resets the idle
clock. Guest background work alone does not count as external activity.

`--auto-resume` allows authenticated SDK operations and published URL traffic to
start a stopped or paused VM. HTTP requests wait for the application's TCP port
before forwarding once, preserving POST bodies and WebSocket upgrades. Resume
may take seconds; application-level readiness remains the application's job.

## Persistent URLs

Configure a wildcard domain pointing to the gateway, with wildcard TLS:

```yaml
abstractions:
  vm:
    defaultPool: vms
    domain: vm.example.com
    baseURL: https://gateway.example.com
```

`defaultPool` selects the CPU microVM pool for `vm new` without `--pool`.
An explicit `--pool` overrides it. Leave it unset to use the scheduler's
eligible microVM pools. The selected pool must use `containerRuntime: microvm`.
Creation checks the VM API before building an image and reports the selected
HTTP gateway when it is unavailable.

URLs use `<name>-<random handle>-<port>.vm.example.com`, with a separate random
128-bit handle independent of the resource UUID. They remain fixed across cold
boots and gateway restarts. Unprotected published URLs allow anyone holding the
URL to reach that guest service. Workspace tokens never appear in URLs.
SSH stays behind workspace authentication. Removing a VM or unpublishing a port
revokes routing immediately; forks never share a handle.
Revoking the owning token also denies new URL access and stops the VM during
reconciliation. Another active workspace token can explicitly start it again.

Protected ports require `X-Beam-VM-Token`, available through
`vm.traffic_access_token` or `access-token`. `access-token --rotate` revokes old
tokens and browser sessions. `vm.access_url(port, ttl=600)` exchanges a temporary,
port-scoped link for a Secure/HttpOnly browser cookie and redirects to the stable
URL. `desktop --url` and `terminal --url` do this automatically for protected
ports. Access credentials are stripped before application forwarding, and denied
requests never wake a stopped VM. Sessions grant traffic access only, with a
maximum lifetime of one hour. Guest applications still control their own users.

Local configuration uses `vm.localhost:1994` and plain HTTP. The generic path
fallback is `/vm/<handle>/<port>/`; applications using absolute browser paths
should use the wildcard domain. Desktop and terminal use their own origin.

Desktop uses foreground KasmVNC under systemd, Openbox, a transient display
directory, and boot-scoped X locks. It allows resizing and targets 60 FPS. Video
uses JPEG to limit CPU spent on full-frame WebP encoding. The gateway streams
WebSocket messages with reusable 64 KiB buffers instead of allocating whole
frames. Frame rate still depends on the workload, network, and assigned CPU.

`vm.desktop` adds screenshots, screen size/resize, mouse/click/drag/scroll,
keyboard shortcuts, Unicode clipboard paste, application launch and CPU MP4
recording over the existing authenticated sandbox transport. Finish recordings
with `vm.desktop.stop_recording(handle)`, then download with `vm.fs`. `screenshot`
saves a PNG through the CLI. Screenshots and recordings do not require a public
desktop URL; `vm.aio` exposes the shared async process, filesystem and Docker APIs.

## Development verification

The development gateway hot-reloads in Okteto. Staging uses release images with
`GATEWAY_TAG=<custom CI tag> make start-stage` and a separate Okteto state folder.
Run `make worker` with a kubeconfig explicitly
pointing at the development cluster: its cleanup step deletes local worker jobs.
Migration 056 adds only VM identities and artifact metadata. There is no new
scheduler or disk storage backend.

The guest kernel currently builds for amd64 and requires KVM. The ARM k3d cluster
on this workstation has no `/dev/kvm`. Backend, SDK, CLI, database, guest config,
systemd container, and browser desktop checks can run locally; the full VM smoke
test needs an amd64 KVM development worker connected to this gateway.

Run `python hack/vm-smoke.py --context local --pool <development-vm-pool>` on that
setup. It checks root persistence, arbitrary enabled units, shutdown writes,
stable URLs and machine identity, independent forks, and templates after source
removal. It creates and removes only its own resources.

`e2e/vm_tests/audit.py --profile staging --image-id <prepared-desktop-image>
--name audit-<run> --report <path>` checks memory pause, protected traffic,
automatic wake, extra disks/volumes, network policy, async files, process control,
metrics and desktop APIs. Run the same arguments with `--cleanup` after browser
validation to remove that run's VM and unregister its disk/volume.
See [the competitor audit](vm-competitor-audit.md) for scope and remaining platform
distinctions.
