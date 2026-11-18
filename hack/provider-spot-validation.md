# Provider reservation validation — 2026-11-18

These representative checks exercised the real staging compute-offerings and
private-pool reservation APIs under Okteto. Each reservation used a new pool
pinned to one provider, offer, region and purchase mode. The production gateway
was not deployed or restarted.

| Provider | Purchase mode | Hardware and region | Result |
| --- | --- | --- | --- |
| Hetzner | On-demand | cpx31 CPU, ash | Passed |
| Verda | On-demand | CPU.4V.16G, FIN-01 | Passed |
| Verda | Spot | 1V100.4V, FIN-01 | Passed |
| Shadeform | On-demand | A6000_plus, desmoines-usa-1 | Passed |
| Shadeform | Spot | H200_sxm5, helsinki-finland-3 | Passed |
| Nebius | Spot | RTX Pro 6000, eu-south1 | Passed |

Every pass required:

1. Selecting a real offering and creating a reservation through the staging API;
   native VM creation, agent registration and a ready worker in the requested pool.
2. A finite Beam workload reaching terminal exit code 0. CPU runs verified
   deterministic arithmetic. GPU runs launched an actual CUDA kernel over 1,024
   elements and verified its exact output SHA-256; GPU enumeration alone did not
   count.
3. Two fresh Beam containers reading a shared 128 MiB volume fixture, a small
   synthetic model fixture and image files, with exact checksums and outputs.
4. A fingerprint-verified managed SSH session running a cache-only probe in the
   owned worker. Two complete 128 MiB reads from its local raw cache service
   matched the fixture SHA-256. The probe had no origin/fill path, and both probe
   files were removed.
5. Separate reads of existing container lifecycle events showing successful
   mounted-image cache hits for both fresh containers.
6. Deleting the test volume and pool, followed by independent volume inventory,
   pool lookup and native provider reads proving the VM and owned managed disks
   were absent.

The GPU kernel result SHA-256 was
`5fa9487595a23e13bb88e1848bea5d81589d9e3c379f0a0e432e481499f144ab`
on every GPU run. The final Nebius run used gateway source `0448d6d58`, recognized
the RTX PRO 6000 Blackwell Server Edition with driver 580.173.02, and completed all
workload/cache checks before its native VM and disk were deleted.

## Evidence and retry history

Sanitized source ledgers, independent lifecycle-event evidence, volume inventory
and the combined validation matrix are retained locally in
`/private/tmp/beta9-stage-provider-smoke`. Mounted-image evidence is separate from
the original harness result; the original ledgers were not rewritten to pretend
the harness collected those events. The workload-bridge cache flag was an
unprobed placeholder; the fingerprint-verified host probe supplied the direct
cache-service evidence.

- Verda CPU reservations originally selected a CUDA image whose initialization
  failed before agent registration. Plain Ubuntu fixed CPU boot; CPU on-demand
  and V100 Spot then passed the complete checks.
- One Verda Spot attempt completed CUDA but timed out uploading the cache
  fixture. An independent upload/read-back check diagnosed the fixture path, and
  a new reservation subsequently passed every check. The incomplete attempt is
  not counted as a pass.
- Earlier Nebius attempts exposed a price cap below a changed native quote,
  provider preemption during startup, and pricing-policy activation beyond the
  former wait budget. A subsequent H100 attempt failed with native
  `NotEnoughResources` despite positive capacity advice. These attempts were
  cleaned up and are not counted as passes. The successful RTX Pro 6000 test
  changed both platform and region and verified the native quote was covered by
  the actual active pricing policy before compute startup.
- The staging build now produces matching agent artifacts for both CPU
  architectures before building the gateway, matching the production Dockerfile
  workflow.

## Code and rollout checks

Provider and provisioner race suites, focused compute-reservation race tests and
old-code-fails regressions cover the provider contracts, policy readiness,
disabled-provider lifecycle reconciliation, pending-boot timeout and Verda
accepted-response ambiguity. The latter uses the real adapter against local
HTTP: an accepted purchase with no response ID and delayed list visibility must
retain its tagged cleanup record and later delete the instance, with exactly one
purchase POST.

AWS, GCP and Vast are disabled for new discovery/purchases in the saved staging
and production configuration. Credentials and existing-instance lifecycle
operations remain available. Production configuration also contains the approved
existing Verda account SSH key ID. These narrowly guarded configuration updates
and scoped AWS Terraform changes are tracked in
[infra PR #234](https://github.com/beam-cloud/infra/pull/234). Production remains
on gateway image `0.1.860`; the new controls activate with the normal new gateway
image rollout. No compatibility shim or same-image restart is used.

## Limits and deferred cache discussion

This validates the listed hardware and purchase modes, rather than every SKU,
region, workload, interruption/recovery path or billing-renewal scenario. Verda
may retain zero-cost startup-script records after VM deletion; the approved
account SSH key remains intentionally configured.

Cache locality, cache transport, worker listeners and protobuf APIs have no net
changes in this PR. Each test used one machine in one provider-specific pool.
Local cache reads and mounted-image reuse are proven; cross-node and
cross-provider cache reachability remain unvalidated. The relay is preserved
separately on `codex/deferred-cache-relay` for the deferred design discussion.

Static provisioned pools select a single provider. Private reservation pools
instead accept a provider allowlist, and adding capacity can accumulate provider
and region filters. Cache locality does not itself establish network
reachability: existing peer discovery dials advertised addresses directly.
Provider selection, locality isolation and peer reachability therefore need to
be considered together before changing cross-provider cache behavior.
