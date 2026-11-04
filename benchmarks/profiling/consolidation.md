# Release consolidation

Production PR: https://github.com/beam-cloud/beta9/pull/2107

Current review fixes and staging validation: [review-fixes.md](review-fixes.md).

Latest validation after the goproc CI release and duplication cleanup:
[release-followup.md](release-followup.md). The earlier release measurements
below remain historical; the follow-up records a 5.294s restore tail miss at 50.
Its latest cleanup section also records an 8.279s pause outlier and a 6.572s
repeat after removing hook-name special casing and boot/debug instrumentation.

This branch preserves the complete research snapshot. The release branch drops
instrumentation and non-improving experiments. Measurements below support
individual mechanisms; multi-change burst runs do not isolate each change's
contribution to end-to-end latency.

| Retained change | Evidence / reason |
| --- | --- |
| Detached qcow TTL and pressure eviction | 3,650 recoverable copies reclaimed, 398,360,776,704 allocated bytes. Real allocation-map tests retain unpublished data and zero writes. Origin restore after expiry retained RAM, disk and boot identity. |
| Mutable-volume requirement coalescing; retired VM stub filtering | Obsolete hashes no longer remain protected indefinitely. Retired-stub filtering reduced the observed set from 2,810 to 39 in about 101ms. Fail-open guards retain ambiguous/shared references. |
| Idle image mount expiry | Active VM references, recent use, concurrent image loading and real open FUSE readers fence unmount. Removing the empty mountpoint lets archive eviction proceed. |
| VM, stub and disk transaction batching; bounded connection prewarm | Aurora logged-table probe: 100 individual commits 4,292/638/163ms; batches of 16 56/55/16ms; one batch 25/19/11ms. Baseline showed IO:XactSync waits, not exhausted CPU credits. Configurable; tested staging pool 32, batch cap 128. |
| QSD write-back negotiation | Avoids repeated boot-time ext4 write-through stalls while preserving flush semantics. |
| Sparse qcow chunk coalescing | Nine objects reduced to one; exact file hash and qemu-img compare matched. Stored size rose 1.73MB to 2.09MB, trading sparse gaps for fewer origin operations. |
| Snapshot CPU allowance with serialized resource updates | Same-image 0.1-vCPU capture 638ms vs 79ms at 1 CPU, with quota throttling observed. Exact quota/weight restored before any possible resume, including errors. |
| Pause before bounded capture IO admission | 100-VM v42 pause 7,855ms with 32 IO slots and 100 unarchived captures; all state proofs passed. Capture admission does not permit premature durable acknowledgement. |
| One native zstd thread with frame checksum | 32 concurrent encoders about 0.36s vs 0.80s with two threads, near-identical size and matching decoded hashes. Native encoder CPU also below portable encoder in same-input probe. Portable fallback retained. |
| Terminal single-disk publication overlap | Disk pivot fences RAM capture; origin publication still fences availability; cancellation joins the publisher. v30 proved state integrity but did not independently meet the pause target. |
| Detached attachment hint | Exact persisted payload hash, synchronous fallback on any payload change. 100 fast detaches, max 8.8ms vs prior roughly 2s median sync-heavy teardown. Authoritative synced manifest remains intact. |
| Read-only shared guest init; disjoint guest mounts capped at four | Avoids duplicate executable-page faults and serial independent mounts. Captured init and process manager remain pinned across worker upgrades. Ancestor, duplicate and symlink-alias mount targets stay ordered. |
| MicroVM boot CPU weight and authenticated-readiness handback | Modest v39 gain with shares 400 / weight 16. Tenant limits applied once authenticated readiness is known; per-VM SMT isolation retained. |
| Guest-control readiness gate; SDK readiness reuse | Avoids authenticated RPC probes before a control connection exists and a redundant SDK wait after server-confirmed readiness. Probe reduction alone is not claimed as an isolated end-to-end win. |
| Firewall cleanup batching | Coalesces concurrent rule walks/deletes under the existing firewall lock; keeps IP/slot quarantine through completion. Correctness tests cover batching and failures; independent whole-burst benefit is unquantified. |
| CLIP stateless immutable mounts | Upstream PR58 merged to master f6838982cfe2. Real-FUSE OPEN 324→3, OPENDIR 10→1, exact reads/mmap/cookies and busy-reader fences passed. Full burst gains are confounded by other changes. |
| goproc bounded output drain and wait race fix | Upstream PR5 merged to main a70f7739dd77. Preserves delayed output under CPU contention, reports drain failure with nonzero exit, and removes a ProcessState read/write race. Full race suites passed repeatedly. |

Removed from production: lifecycle/RPC/SQL/HTTP/guest tracing, profile sampling,
benchmark harness, buffered JSON logger, boot rendezvous and admission gates,
virtiofsd thread tuning, kernel initcall debugging and systemd status experiment,
TCP socket preflight, cache quiet periods, upload pacing/multipart/streaming and
hedging, long-window compression, and unused tracing-only context wrappers.
The unpublished core-scheduling experiment was never deployed or committed.

The earlier pooled 10/50 study is in ../results/burst-scaling.json. The final
trimmed release validation is recorded separately after its staging run, with
normal gateway supervision and merged upstream pins.

## Release-validation discovery: deferred network sweep

The first trimmed build launched 10 VMs in 593ms, but four subsequent execs lost
connectivity. The worker then logged removal of 481 stale network slots. Initial
cleanup had deferred behind the previous process's pool lock while refill still
created new slots under the same worker ID. The retry classified those new slots
as previous-process leftovers and deleted networks already assigned to the burst.

The release fix gates pool refill until the initial stale sweep finishes. Ordinary
on-demand container networking remains available while that sweep is deferred.
A race regression exercises concurrent refill attempts before/after the fence;
the closed-pool regression explicitly starts past that fence. This is a confirmed
correctness fix, not an inferred dependency or guest-boot performance issue. The
failed cohort is retained in the validation evidence and excluded from successful
cohort latency statistics.


## Final staging validation

Commit `c877158ccb0a32d301cf561bb59d0d4864f149b7`, normal Okteto gateway,
merged CLIP/goproc heads, per-VM SMT isolation retained. Fixture: 0.1 CPU,
512MiB RAM, fresh 1GiB root, prepared image and HTTP connections outside timing.
Ten bursts of 10 and two bursts of 50 provide 100 samples at each size.

| Burst | Startup p50 / p95 / p99 | Worst burst start | Worst durable pause | Worst restore |
| --- | --- | --- | --- | --- |
| 10 | 435 / 504 / 521ms | 523ms | 5.449s | 0.775s |
| 50 | 1,233 / 1,334 / 1,354ms | 1.372s | 7.549s | 4.844s |

All 200 initial and restored RAM/disk/boot proofs matched, fresh boot IDs were
unique across the study, all owned VMs were removed, and every timing budget
passed. The failed pre-fix v49 cohort remains separate from these quantiles.

An owned paused probe was aged eight days in stub recency and filesystem
metadata. Restarting the same release image reloaded that aged cache index;
normal maintenance removed its native checkpoint, archive/CAS and qcow copies
in 58.916s. Origin-backed restore preserved its RAM, disk and boot identity.
The probe was removed; all three pre-existing VMs also retained boot identities
across the restart. Busy image readers and unpublished qcow zero writes remained
protected in real filesystem tests.

Affected Linux race suites passed; the full worker race suite was rerun after
the network fix, with ten focused race repetitions. Privileged qcow/FUSE/init
checks and seven SDK readiness cases passed. 1,073 source files in Okteto matched
the committed source hashes. Binary build information confirms the merged
CLIP/goproc versions without local replacements.

The prior profiling-only study reported startup p95 458/1,116ms at 10/50; the
release study reports 504/1,334ms. Gateway supervision, logging and lifecycle
workloads differ, so this is not a controlled ablation and cleanup itself is not
claimed to improve startup. The release preserves the requested timing budgets
and fixes the reproduced network failure.

Remaining: guest-boot scaling at larger bursts, tight pause/restore tail margins
at 50, and the original 100-VM subsecond objective (not demonstrated here).
These staging observations do not establish production tail-latency guarantees
for larger memory sizes or cold image downloads.

[Raw cohorts, failed cohort and expiry evidence](../results/release-validation-20261104/summary.json).


### Reproducing the configured test node

The burst results use these settings on the 32-logical-CPU `microvm-cpu` node;
they are per-node tuning, not universal production defaults. Deploy the release
image with the documented settings when reproducing these measurements.

```text
WORKER_CONTAINER_START_CONCURRENCY=128
CONTAINER_NETWORK_SLOT_POOL_SIZE=256
BETA9_MICROVM_CHECKPOINT_CONCURRENCY=100
BETA9_MICROVM_SNAPSHOT_IO_CONCURRENCY=32
BETA9_CHECKPOINT_ARCHIVE_CONCURRENCY=32
BETA9_CHECKPOINT_UPLOAD_CONCURRENCY=32
BETA9_CHECKPOINT_DISK_PIPELINE=1
BETA9_NETWORK_CLEANUP_BATCH=1
```

Gateway Postgres: maxOpenConns=32, warmConnections=32, vmBurstBatchSize=128.
Cache reconciliation: filterRetiredVMStubs=true, recentStubTTLSeconds=604800.
GOMAXPROCS and optional spare-refill quiet period use their defaults. Shared
read-only init, four independent guest mounts, native single-thread zstd when
installed, and authenticated-readiness CPU handback are in the release code.

Final handoff: worker uncordoned, 253 prepared slots plus three pre-existing
VMs; normal Okteto Air supervision active; each pre-existing boot identity
preserved; the worker uses gateway.stage.beam.cloud:443, independent of the
profiling reverse tunnel. NVMe remained about 55% used with 1.6TiB available.
