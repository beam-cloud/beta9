# Release consolidation

Production PR: https://github.com/beam-cloud/beta9/pull/2107

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
