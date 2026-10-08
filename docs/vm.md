# Persistent CPU microVMs

`beam vm` creates a named resource with a durable writable root and stable
desktop, terminal, and application URLs. Every launch uses `use_vm`; GPU options
are rejected. The guest runs systemd as PID 1. Stop releases compute; start is a
cold boot of the same filesystem. Enabled user services start again normally.

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

Lifecycle commands are `new`, `list`, `get`, `start`/`resume`, `stop`, `rm`, and
`fork`. Access commands are `exec`, `ssh`, `scp`, `sync --watch`, `desktop`, and
`terminal`. `ports`, `expose`, and `unexpose` manage published ports.
`port-forward dev 15432:5432` binds localhost through an authenticated tunnel and
does not publish a URL. `prompt` supports `--agent claude|codex`, `--cwd`, and
`--detach`/`--background`. `logs --unit` reads the journal; `logs --session` reads
durable prompt logs. Exec, prompt, and logs work without SSH.

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
version resumes cold and does not save RAM. VM service shutdown gets at least
120 seconds; raise the worker termination grace period for units that need longer.

`--ttl` measures idle time from SDK operations and active HTTP/WebSocket/tunnel
connections. `0` disables idle stopping. Restarting explicitly resets the idle
clock. Guest background work alone does not count as external activity.

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
boots and gateway restarts. Treat published URLs as access credentials: anyone
with the URL can reach that guest service. Workspace tokens never appear in URLs.
SSH stays behind workspace authentication. Removing a VM or unpublishing a port
revokes routing immediately; forks never share a handle.
Revoking the owning token also denies new URL access and stops the VM during
reconciliation. Another active workspace token can explicitly start it again.

Local configuration uses `vm.localhost:1994` and plain HTTP. The generic path
fallback is `/vm/<handle>/<port>/`; applications using absolute browser paths
should use the wildcard domain. Desktop and terminal use their own origin.

Desktop uses foreground KasmVNC under systemd, Openbox, a transient display
directory, and boot-scoped X locks. It allows resizing and targets 60 FPS. Video
uses JPEG to limit CPU spent on full-frame WebP encoding. The gateway streams
WebSocket messages with reusable 64 KiB buffers instead of allocating whole
frames. Frame rate still depends on the workload, network, and assigned CPU.

## Development verification

The gateway hot-reloads in Okteto. Run `make worker` with a kubeconfig explicitly
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
