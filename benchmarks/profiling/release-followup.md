# CI release and duplication cleanup

Production PR: https://github.com/beam-cloud/beta9/pull/2107

Validated production commit `d429f4ceec91583d39a8001718f44d9e6e5890e6` in
Okteto and on the staging `microvm-cpu` node. This follows the earlier
[release consolidation](consolidation.md); its results remain historical.

## What changed

- Published [goproc v0.1.18](https://github.com/beam-cloud/goproc/releases/tag/v0.1.18)
  from merged main `a70f7739dd771769faa7af3df8c9579f5aec429b` through its existing
  release workflow. [CI run 41861454053](https://github.com/beam-cloud/goproc/actions/runs/41861454053)
  built and uploaded Linux amd64 and arm64 binaries successfully. Both downloaded
  assets match GitHub's SHA256 digests. The worker downloads this release instead
  of compiling goproc from source; go.mod also pins v0.1.18. CLIP remains on merged
  master `f6838982cfe2`.
- Shared bounded batch collection between VM mutation, stub, disk and firewall
  queues. Shared stub match/create/touch/activity SQL between single and batch
  calls, preserving forced creation and commit fences; normalized config key
  order without rounding large integers. Shared rollback-only batch retry policy.
- Shared guest bind/read-only mount setup, SDK readiness decisions and cache
  watermark normalization; collapsed identical guest mount fallbacks. Reused
  test setup for mounts, quota/weight, capture slots, detached markers, sparse
  roundtrips, database pools and IPv6 failures. Removed leftover trust-hook timing.
- Fixed cleanup cancellation to join firewall deletion before deciding whether
  to release/quarantine the address. Added the missing aggregate failure count
  for guest-control readiness errors. Both have regression coverage.

The follow-up removes 37 production lines overall (193 additions / 230 deletions),
with expanded regression coverage. Profiling stays off the production branch.
The measured single-thread native zstd choice and other retained mechanisms are
unchanged; see the consolidation table for their individual evidence.

## Staging results

Normal Okteto Air supervision, no startup instrumentation. Ten bursts of 10 and
two bursts of 50 give 100 samples at each size. Fixture: 0.1 CPU, 512MiB RAM,
fresh 1GiB root, prepared image and client connections outside the clock, unique
SMT isolation for each VM. The per-node settings remain those documented in
[the consolidation](consolidation.md#reproducing-the-configured-test-node).

| Burst | Startup p50 / p95 / p99 | Worst burst start | Worst durable pause | Worst restore |
| --- | --- | --- | --- | --- |
| 10 | 433 / 504 / 528ms | 0.533s | 7.276s | 0.792s |
| 50 | 1,129 / 1,236 / 1,249ms | 1.255s | 4.950s | 5.294s |

All 200 initial and restored RAM/disk/boot proofs matched, all fresh boot IDs
were distinct, and all owned VMs were removed. Every startup and pause met the
1.5s / 8s budgets. The second 50-VM restore exceeded the earlier 5s budget by
294ms; its report records a failed timing budget and remains in the quantiles.
There were no RPC, guest proof or cleanup failures. A 10-VM pause had a 7.276s
outlier; the pooled pause p95/p99 were 2.929s / 4.229s.

Startup and pause met the requested stopping thresholds. Restore tail latency
at 50 remains unresolved. This study does not establish the original subsecond
100-VM target, performance with larger guests or cold image downloads, or an
isolated performance benefit from refactoring. The previous source-built goproc
and released binary use different Go versions (1.25.6 / release CI 1.22.10), and
host/cache conditions vary; the two studies are not a controlled ablation.

## Reliability and handoff

An owned paused probe was aged eight days in stub recency and filesystem
metadata. A restart of the same image reloaded that aged index. Normal maintenance
removed the native checkpoint, archive, CAS and qcow copies; the verification
poll observed all absent after 12.438s. This is time from the post-restart poll,
not the full time since ageing. Origin restore preserved exact RAM, disk and
boot identity, then the probe was removed. All three pre-existing VMs retained
boot identities across the restart. Real-FUSE tests separately cover image
expiry and busy readers; qcow tests retain unpublished zero writes.

Affected Linux race suites, focused batch/watermark regressions, isolated real
Postgres temporary-table tests, seven SDK readiness cases and privileged
filesystem/firewall tests passed. All 1,077 checked source files in Okteto
matched the committed source. The deployed overlay contains those worker/init
builds and downloads/verifies the actual goproc CI binary. A complete worker
base-image rebuild was not repeated.

Final handoff: normal Air supervisor running, health HTTP 200, worker uncordoned,
253 prepared network namespaces, only the three pre-existing VMs running.
NVMe 51.9% used, 1.54TiB available. The existing TLS gateway remains independent
of the removed profiling tunnel.

Image digest: `sha256:f17418b50b10748b0beb0affbedabae765dfcc0f23168a56ee2ff28acd0b5de2`.

[Summary](../results/release-dry-followup-20261104/summary.json),
[raw cohorts](../results/release-dry-followup-20261104/runs/),
[expiry proof](../results/release-dry-followup-20261104/retention.json),
[CI and test evidence](../results/release-dry-followup-20261104/validation.json),
[final handoff](../results/release-dry-followup-20261104/handoff.json).

## Subsequent file consolidation

Production commit `e7e474f0` groups VM persistence, artifacts and advisory locking
in `pkg/repository/vm.go`; mutation/stub/disk batching and shared stub upserts in
`vm_batch.go`. VM tests are in the two corresponding test files. Checkpoint
capture IO and disk-publication helpers and tests move into the existing
`pkg/worker/criu.go` / `criu_test.go`. Net reduction: 12 files and 122 lines.

Tokenized non-import declarations match before/after across both packages:
1,385 repository and 3,203 worker declarations, including SQL and tests.
The affected Linux race suites and five real Postgres integration cases pass,
including 100 independent VM locks acquired in 32.151ms. Okteto source hashes
match for all 131 synchronized files; all 13 superseded files are absent. The
normal gateway reports HTTP 200. The lifecycle timings above remain measurements
of `d429f4ce`; file moves do not establish a new performance result.

[Consolidation verification](../results/release-dry-followup-20261104/consolidation.json)
and [test output](../results/release-dry-followup-20261104/consolidation-validation.log).
