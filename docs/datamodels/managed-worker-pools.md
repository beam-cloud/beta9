# Managed worker pools

Managed worker pools are the control-plane-owned inventory that serves public
serverless workloads. They are distinct from tenant-owned private and
marketplace pools even though all three can use the same agent protocol.

## Controller selection

| Worker pool configuration | Controller | API behavior |
| --- | --- | --- |
| `mode: local` | local Kubernetes controller | visible, config-managed |
| `mode: external` with `provider` | legacy remote-k3s controller | visible, config-managed |
| `mode: external` without `provider` | agent controller | visible, config-managed |
| API-created pool | agent controller | editable |

The legacy implementation lives in
`pkg/scheduler/pool_external_legacy.go`. The agent implementation now owns
`pkg/scheduler/pool_external.go`, which leaves a direct removal path once no
provider-backed external pools remain.

## API and authorization

The management contract is exposed at `/api/v1/pools`:

- `GET /api/v1/pools`
- `POST /api/v1/pools`
- `PUT /api/v1/pools/:name`
- `DELETE /api/v1/pools/:name`

Every route requires an authenticated cluster-admin token. The compute service
performs the same check again so a caller cannot bypass authorization by
invoking service methods without the HTTP middleware. beta9 does not model a
separate operator role or return client-specific authorization hints.

Config-defined pools remain read-only in this API so `config.yaml` stays their
single source of truth. API-created pools are persisted in compute state,
restored by the compute service after a gateway restart, and rejected if their
name collides with a config-defined pool. The Compute page is one client of
this API; no client identity or UI provenance is stored in the backend.

## Worker configuration

API requests use the existing `WorkerPoolConfig` JSON schema rather than
a second pool model. The complete configuration is persisted, returned to the
caller, and used to rebuild the agent controller. This retains settings such
as GPU type, runtime, container concurrency, network slots, selector behavior,
priority, preemption, cache configuration, paths, pool sizing, job spec, and
runtime configuration. Fields that only apply to the local Kubernetes or
legacy provider controller remain available to config-defined pools and are not
silently rewritten.

The common management controls include `default_machine_cost` (the existing
dollars-per-machine-second rate used for usage accounting). The advanced JSON
editor always round-trips the complete `WorkerPoolConfig`, including nested and
future fields, instead of projecting it into a lossy client-specific schema.

API-created pools are always `mode: external` with no `provider`. Local
and provider-backed external pools continue to be fully supported through
`config.yaml`; they are shown in the Compute page without pretending their
infrastructure-specific settings can be changed safely at runtime.

## Adding machines

For an agent-backed managed pool, `beta9 machine create --pool <name>` returns
the systemd installer command. Its join token:

- contains 256 bits of random entropy and is stored only as a hash;
- expires after 30 minutes;
- is scoped to the admin workspace and selected pool;
- is bound to the generated machine ID and the first joining fingerprint; and
- cannot join a private, marketplace, or unmanaged pool.

Each pool also has a random instance identity. Deleting and recreating the same
pool name changes that identity, so installers minted for the deleted pool stay
invalid even if their 30-minute TTL has not elapsed.

Pool updates and deletion require all machines and workers to be drained first.
Machine deletion also refuses to proceed while that machine has active
containers.

## Pool telemetry

The Compute page's Metrics tab consumes the heartbeat already emitted by every
agent-managed machine. The existing scheduler health monitor emits the same
compact pool snapshot every ten seconds for local Kubernetes and legacy
provider pools, so migration status does not determine observability. No
Grafana or Prometheus proxy sits in the request path.

Metric heartbeats remain on the authenticated workspace event stream for the
live view and are also written to the dedicated S2 stream
`events/workspaces/<workspace>/compute/metrics` for historical aggregation.
Keeping them out of the compute lifecycle stream preserves fast machine and
worker history reads.

`GET /api/v1/metrics/:workspaceId/pool-timeseries` returns per-pool capacity,
utilization, and cost buckets. Ordinary users are always scoped to the
workspace from their authenticated token, even if they alter the route. A
cluster admin additionally receives the admin workspace resolved by the
server. The response lists those server-authorized workspace IDs so the client
can open the matching live streams; the existing workspace middleware validates
each stream independently.

During rollout, historical reads fill only the prefix that predates the new
metrics stream from the legacy workspace stream. That fallback naturally stops
once the selected range is fully covered by the dedicated stream.

## Migration

To migrate a provider-backed external pool to the agent controller:

1. Drain and remove its legacy machines.
2. Remove the `provider` field while keeping `mode: external` and the useful
   worker-pool settings.
3. Restart the control plane so the config-defined agent pool is materialized.
4. Run `beta9 machine create --pool <name>` on each replacement node and use
   the emitted systemd installer.

Do not run both controller types under the same pool name. Config names are
authoritative and collisions with persisted API-managed state fail closed.
