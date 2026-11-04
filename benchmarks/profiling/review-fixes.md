# VM lifecycle review fixes

PR: https://github.com/beam-cloud/beta9/pull/2107

Production commit `09f1f6e306f6cf8a99e1ed5ae107b023db0920ad` addresses the
remaining active review findings. All 69 Cubic threads are resolved; 53 were
open at the start of this pass. Removed profiling/experiment threads have
specific explanations; accepted fixes were checked against current code.

## Changes

- Mutation, stub and disk batches return their database outcome after enqueue;
  cancellation before dispatch still rejects the request. Ambiguous completion
  and unrecognized class-40 errors cannot replay a batch. Rollback retry codes
  are explicit; `40003` is completion-unknown, as listed in
  [PostgreSQL's error codes](https://www.postgresql.org/docs/16/errcodes-appendix.html).
- A failed lock query quarantines its shared session. Existing owners retain
  the connection until they release; only then is it discarded with any unknown
  locks. New acquisitions wait for that session to drain. This avoids actively
  dropping a healthy connection's other advisory locks after a query error.
  It does not make PostgreSQL locks survive a lost network/database session.
- CPU preparation failure before native snapshot resumes the original guest;
  snapshot-started failures and resource-restoration failures remain terminal.
  Disk publication is canceled and joined before filesystem fallback, including
  cancellation while waiting for the seal fence.
- Firewall batch failures are isolated with idempotent per-slot retries, so
  successful slots can release their addresses. Releasing a long-running
  container refreshes its image mount's idle grace. Spare-size persistence
  failure retains the in-memory replenishment policy. Routine prune deadlines
  no longer generate warnings or debug output.
- Both Dockerfiles use CI-released goproc v0.1.18 and one shared amd64/arm64
  checksum manifest. zstd is pinned to tested Ubuntu version
  `1.4.8+dfsg-3build1`. Stub JSON argument assertions and shared batch-failure /
  integration fixtures address the remaining test review comments.

## Validation

Affected Linux race suites passed: repository, runtime, worker, disk, VM service
and both gateway service packages. Six real Postgres integration cases passed,
using isolated temporary tables or random advisory-lock keys. A server-rejected
lock query left the original owner fenced against a second gateway; 100
independent locks acquired in 41.295ms while leaving query capacity available.

All five GitHub checks passed for the production-fix revision. Follow-up
`715cca5b` changes only the cancellation test's query gate; 20 race repetitions
passed with the database outcome held explicitly instead of relying on a delay.
The staging image's production code is unchanged by that test-only follow-up.

Both architecture-specific goproc download/checksum build paths passed with the
actual Docker ignore rules. A deliberately corrupted digest failed the build.
The pinned zstd package downloaded successfully from the Ubuntu repository.
The staging overlay uses the exact committed worker/runtime code and checksum
install fragment; a complete CUDA worker base-image build was not repeated.

Privileged shared-init/cgroup, checkpoint/cancellation, real FUSE, firewall and
overlay checks passed. All 1,065 checked source files in Okteto matched the
commit. The runtime fixes and new tests passed in the final staging image.

One fresh cohort at each size used the same configured `microvm-cpu` fixture:
0.1 CPU, 512MiB, fresh 1GiB root, prepared image/connections, per-VM SMT isolation.

| Burst | Whole-burst start | Durable pause | Restore |
| --- | --- | --- | --- |
| 10 | 0.530s | 3.879s | 0.580s |
| 50 | 1.316s | **8.813s** | 4.460s |

All 60 RAM/disk/boot proofs matched before and after restore; all fresh boot
identities were distinct, and all owned VMs were removed. The 50-VM pause missed
8s by 813ms. Its slow checkpoint archived in 371ms and spent 5,557ms inside
`UploadWithReader`, measured after upload admission. These operational durations
do not isolate file IO, client/retries, network and origin-service contributions.
The failed timing budget is retained; the pause-tail target is not established.

Final handoff: all three original VM boot identities preserved, only those VMs
running, worker uncordoned, 253 prepared namespaces and normal Air supervision
with HTTP health 200. Image digest:
`sha256:2e11341869bba27fcd171622beb94751e1f1e59af4e81a3b3f4814921228af26`.

[Review audit](../results/cubic-review-20261104/review-threads.json),
[smoke summary and raw cohorts](../results/cubic-review-20261104/summary.json),
[Linux race results](../results/cubic-review-20261104/linux-race.log),
[cancellation repetitions](../results/cubic-review-20261104/cancellation-repeat.log),
[Postgres results](../results/cubic-review-20261104/postgres.log),
[packaging checks](../results/cubic-review-20261104/packaging.json),
[privileged checks](../results/cubic-review-20261104/privileged.log),
[handoff](../results/cubic-review-20261104/handoff.json).

Earlier mechanism evidence and the full 100-sample-per-size study remain in
[release-followup.md](release-followup.md) and [consolidation.md](consolidation.md).
