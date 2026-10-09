# Persistent VM competitor audit

Reviewed 2026-10-09 against the official Boat, Blaxel and E2B documentation. This comparison concerns CPU development machines and agent/computer-use environments. Vendor latency and FPS claims are not measurements of Beam. Private-preview features are identified below.

## Sources

- Boat: [CLI](https://docs.boat.dev/cli-reference), [HTTP API](https://docs.boat.dev/api/v1), [hosting](https://docs.boat.dev/hosting), [long tasks](https://docs.boat.dev/long-running-tasks), [snapshots](https://docs.boat.dev/snapshots), [desktop streaming](https://docs.boat.dev/desktop-streaming), [agents](https://docs.boat.dev/integrated-agents), [webhooks](https://docs.boat.dev/webhooks), [environments](https://docs.boat.dev/environments).
- Blaxel: [processes](https://docs.blaxel.ai/Sandboxes/Processes), [filesystem](https://docs.blaxel.ai/Sandboxes/Filesystem), [preview URLs](https://docs.blaxel.ai/Sandboxes/Preview-url), [sessions](https://docs.blaxel.ai/Sandboxes/Sessions), [fork](https://docs.blaxel.ai/Sandboxes/Fork), [expiration](https://docs.blaxel.ai/Sandboxes/Expiration), [egress proxy](https://docs.blaxel.ai/Sandboxes/Proxy-domains), [non-root execution](https://docs.blaxel.ai/Sandboxes/Non-root-user), [schedules](https://docs.blaxel.ai/Sandboxes/Schedules).
- E2B: [persistence](https://docs.e2b.dev/sandbox/persistence), [automatic resume](https://docs.e2b.dev/sandbox/auto-resume), [filesystem-only snapshots](https://docs.e2b.dev/sandbox/filesystem-only-snapshots), [metadata](https://docs.e2b.dev/sandbox/metadata), [metrics](https://docs.e2b.dev/sandbox/metrics), [background commands](https://docs.e2b.dev/commands/background), [PTY](https://docs.e2b.dev/pty), [file watching](https://docs.e2b.dev/filesystem/watch), [network access](https://docs.e2b.dev/network/internet-access), [public access](https://docs.e2b.dev/network/restrict-public-access), [desktop SDK](https://docs.e2b.dev/sdk-reference/desktop-python-sdk/v2.4.2/desktop).

## Feature comparison

| Capability | Boat | Blaxel | E2B | Beta9 VM |
| --- | --- | --- | --- | --- |
| Named persistent machines, full writable OS, SSH, systemd and Docker | Full-machine workflow | Process-oriented sandbox | Sandbox / templates | Existing CPU microVM resource, owned durable root, arbitrary systemd units, SSH/SCP/rsync, nested Docker |
| Release compute without deleting files | Disk-only stop / hydrate | Standby retains state; archive keeps files | Memory pause or filesystem-only pause | Explicit disk-only `stop` and RAM-preserving `pause`; resource retained until removal |
| Preserve active processes through suspension | Stop does not preserve manually launched processes | Standby preserves memory | Pause preserves memory and processes | Added terminal memory checkpoint, paired disk generations and explicit warm resume; no silent cold fallback |
| Resume on access | Hydrate workflow | Automatic standby resume | SDK and HTTP automatic resume | Added opt-in SDK / stable HTTP and WebSocket URL wake, with application socket readiness before forwarding |
| Filesystem fork / reusable templates | Snapshots and environments | Fork private preview | Snapshots and templates | Existing independent root forks, named snapshots and private templates; added snapshot removal |
| RAM-preserving clone / rollback | Disk snapshot workflow | Checkpoint/fork private preview | Full-memory snapshots | Warm resume of the same VM added. Independent RAM clone and rollback remain a separate runtime feature |
| Retry-safe creation | Idempotency key | Create-if-absent helpers | Connect / create helpers | Added workspace-scoped request UUID, identical replay, conflict detection, SDK body retention after lost responses |
| Metadata, discovery and mutable idle policy | Mutable TTL / names / subdomains | Labels / expiration | Metadata filters / timeout updates | Added metadata, exact-match filters, status filters, TTL / idle-action / auto-resume updates |
| Detached commands, status, kill and logs | Async exec / tasks | Named process API, restart, streaming | Background commands, reattach, streaming | Existing shared sandbox process transport; added VM CLI detach / JSON exec / ps / kill / PID logs |
| Interactive stdin and PTY | SSH / terminal | Live stdin | Live stdin / PTY | Existing SSH and terminal support; managed-process stdin remains finite input followed by EOF |
| File read/write/upload/download/stat/search/edit | File API / CLI | File API | File API | Existing `vm.fs` and its async facade; shares sandbox implementation |
| Filesystem event subscriptions | API / tooling dependent | File watch | File watch | File waiting and rsync watch exist; managed inotify event subscriptions remain open |
| Durable extra disks / shared files | Persistent disk / environments | Volumes, Agent Drive private preview | Volumes / snapshots | Added first-class existing durable disks and workspace volumes; VM removal owns only its root |
| Stable app URLs and private tunnels | Hosting / forwarding | Preview URLs | Per-port domains / proxy | Existing pinned VM handle, published ports and authenticated private TCP tunnels |
| Protected traffic and browser sharing | Scoped keys / expiring stream links | Preview auth and scoped sessions | Traffic token | Added per-port protection, rotatable VM traffic token and expiring port-bound browser sessions. Sessions grant published app traffic only, never management access |
| Outbound network restriction | Network controls | Domain/method/path proxy | Internet-off, CIDR/domain rules, proxy | Added existing host firewall policies at create/update and every launch; CIDR allow list or full blocking |
| Desktop computer use | Moonlight / noVNC; tooling | No equivalent documented desktop suite | Screenshot, mouse, keyboard, resize, apps, recording | Existing CPU KasmVNC streaming; added PNG screenshots, mouse/keyboard/drag/scroll, resize, launch and CPU recording |
| Resource usage | Usage dashboards | Platform metrics | CPU/memory/disk metrics | Added guest CPU/memory/root disk sample; fleet/billing metrics remain platform concerns |
| Agent harness | Integrated conversations, steering, events | Agent platform | Code interpreter / agent integrations | Existing Codex/Claude prompt sessions and durable logs; conversational orchestration is not replicated inside VM lifecycle |
| Scheduling / lifecycle integrations | Signed lifecycle webhooks | Scheduled commands | Lifecycle webhooks | Guest systemd timers and Beam schedules available; durable signed VM lifecycle webhooks remain open |
| SDK / build ecosystem | Python and TypeScript / environment builds | Python and TypeScript | Python and TypeScript / versioned builds | Python `VM` and Beam CLI, image IDs / Dockerfiles / private templates; dedicated TypeScript VM SDK and template versions remain open |

## Persistence contract

`stop` waits for systemd shutdown and final disk commit. `pause` instead seals durable disks while the hypervisor is suspended, captures memory, and terminates compute without a systemd shutdown that would invalidate the saved memory image. Enabled units restart after a cold boot; live processes continue after warm resume.

A warm resume requires the original active workspace credential, an available compatible microVM checkpoint and unchanged paired durable-disk generations. Changed storage or an incomplete pause produces a recoverable error. `start --cold` explicitly discards memory and boots the latest durable disks. Referenced VM checkpoints are excluded from checkpoint garbage collection. Shared volumes are external mutable storage, not transactionally included in a RAM snapshot. Runtime PID/process handles must be reacquired after container identity changes.

Automatic resume is opt-in and may take seconds; no sub-25-ms promise is made. HTTP requests are forwarded once only after readiness, so a cold start does not duplicate a POST. An open public HTTP/WebSocket or private tunnel counts as activity. Detached guest work must use a zero idle timeout or an explicit activity lease when idle stopping is enabled.

## Remaining distinctions requiring separate infrastructure

These are real differences, not claimed parity: memory-preserving clones/rollback, managed live-stdin/PTY and filesystem event transports, signed durable lifecycle webhooks, frontend management sessions, domain/method/path-aware egress proxy with secret injection, custom domain certificates and dedicated IPs, versioned/public template distribution, and a TypeScript SDK. They require ownership/recovery or transport changes beyond wrapping the existing VM/sandbox abstractions. VM pause/restore, firewalls and browser traffic sessions do not substitute for those capabilities.

GPU support is deliberately excluded. Account/billing/organization management, model gateways, and vendor-hosted conversational agents belong to their respective platform products and are outside the CPU VM contract.

## Validation

Focused unit tests cover retry replay/conflicts, metadata filtering/replacement, live network updates, protected traffic, access-session scope/expiry/rotation, request-preserving automatic wake, terminal pause, paired disk rejection and explicit cold recovery. SDK tests cover selected-profile builds, retry request retention, CLI registration and shared transport behavior.

Staging validation and the exact tested image tags are recorded after deployment in the PR. Documentation availability does not establish measured vendor performance or Beam end-to-end correctness.
