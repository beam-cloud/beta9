# CI release and duplication cleanup

Production PR: https://github.com/beam-cloud/beta9/pull/2107

Latest review fixes and staging validation: [review-fixes.md](review-fixes.md).

Latest cleanup and staging smoke: [guest diagnostics cleanup](#guest-diagnostics-cleanup).
The earlier 200-VM study below remains associated with its original commit.

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

## Guest diagnostics cleanup

Production commit `5da46649` removes the browser-name special case and all
script-content matching from CA hook dispatch. Every valid executable hook runs
through the ordinary path; its own script controls early-init behavior. The
special-case-only test is removed; the existing trust installation, rotation,
generic hook and PATH tests pass.

Removed boot-phase logs, elapsed-boot prefixes, process-start/sync timing output,
the logging-only TCP readiness poller and cache working-set timing diagnostics.
Removed expected-busy image-unmount debug output. Actual qcow cleanup failures
remain visible as warnings. No new Debug/Trace calls remain in the production
diff. Net follow-up change: three added lines, 113 removed.

Linux guest-init and worker race suites passed. The matching staging overlay
passed privileged checkpoint/cgroup, real-FUSE, firewall and overlay checks.
All 1,065 checked source files matched the commit. The CI-released goproc binary
remains unchanged and verified.

One burst of 10 and two bursts of 50 exercised 110 fresh launches and durable
pause/restores using the same fixture/settings. This is a smoke test and repeat,
not a replacement for the earlier 100-sample-per-size study or an isolated
performance ablation.

| Cohort | Whole-burst start | Durable pause | Restore |
| --- | --- | --- | --- |
| 10 | 0.539s | 2.623s | 0.587s |
| First 50 | 1.250s | **8.279s** | 4.142s |
| Repeat 50 | 1.163s | 6.572s | 4.440s |

Every initial/restored RAM, disk and boot proof matched; all 110 boot identities
were distinct, and all owned VMs were removed. The first 50-VM pause missed 8s
by 279ms. Its slow checkpoint spent 676ms archiving 39,168,172 bytes and 6,297ms
inside `UploadWithReader`, measured after upload-slot admission. This identifies
the slow call but does not distinguish file IO, client/retries, network and origin
service time. The failed timing sample remains in the evidence. The repeat met
all budgets; these observations do not establish an 8s pause-tail guarantee.

The three existing VMs retained boot identities across the rollout. Final
handoff: only those three running, worker uncordoned, normal Air supervision,
HTTP health 200 and 253 prepared network namespaces.

Image: `sha256:4ae97d50455e29e9bb465cd3467aab44b3d402dfbebc864d7a8e534107f07c82`.

[Summary and raw cohorts](../results/release-cleanup-20261104/summary.json),
[Linux tests](../results/release-cleanup-20261104/linux-validation.log),
[privileged checks](../results/release-cleanup-20261104/privileged-validation.log),
[slow-upload receipt](../results/release-cleanup-20261104/pause-upload-outlier.log),
[handoff](../results/release-cleanup-20261104/handoff.json).
