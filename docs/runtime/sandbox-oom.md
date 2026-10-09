# Sandbox memory failures

The customer runs **gVisor**. The VMA fix addresses their reproduced Rust build
crash. **Process-level OOM enforcement remains unimplemented for gVisor**;
the separate VM implementation does not satisfy that requirement.

## Diagnosis

The prod2 repro exhausted the host's 65,530 VMA limit. A systrap subprocess
had 65,364 mappings at the last sample; `munmap` failed with `ENOMEM` and
panicked gVisor. Host memory peaked at 10.30 GiB against a 22 GiB limit, with
zero OOM kills. Worker startup raises `vm.max_map_count` to 4,194,304 and
preserves higher values. The customer's earlier SIGSEGV is not independently
attributed to this cause.

The memory-hog case is separate. gVisor's advertised 16 GiB limit is virtual:
its [memory controller](https://github.com/beam-cloud/gvisor/blob/f5056750a788c74f5aabcb328dd4973a56b56f42/pkg/sentry/fsimpl/cgroup2fs/memory.go#L165)
stores limits without enforcing them. Host Linux sees sentry/stub/gofer
processes under a buffered 22 GiB cap. Host group killing and the worker's
gVisor OOM stop path terminate the sandbox.

## Required gVisor follow-up

1. Implement atomic guest memory charging and reclaim in the sentry, enforcing
   the exact requested budget across descendants. Audit anonymous memory,
   fork/COW, shared memory, tmpfs, file mappings and GPU host-memory allocations.
   Account shared pages once; periodic RSS polling is not an exact hard limit.
2. Keep PID 1 and goproc outside the workload budget with reserved headroom.
   Place execs and nested Docker workloads before they run. After reclaim fails,
   select and SIGKILL an eligible guest process, preserving the sentry, manager
   and other healthy processes. Changing host `memory.oom.group` is insufficient.
3. Publish real guest OOM counters/events with victim PID, usage and limit.
   Application OOM events must bypass sandbox stop/delete. goproc v0.1.16
   supplies exit 137 and completed Wait/Status/stream results with retained logs.
4. Preserve charging and events across checkpoint/restore, then publish and
   pin a runsc release before enabling the worker integration.

## Separate VM implementation

Native VM sandboxes enforce the requested budget in `/beam-workload`, with
individual-process OOM killing and no swap. PID 1 and goproc use `/beam-control`.
Guest RAM includes 512 MiB control headroom plus the existing 256 MiB host VMM
reserve. goproc v0.1.16 atomically places exec children in the workload cgroup.
Application OOM events retain logs and leave the sandbox running. Existing
memory checkpoints carry old init/manager state and must be recreated.

## Checks before rollout — deferred

On **gVisor**, rerun the Rust repro on 4 vCPU / 16 GiB RTX 5090 and RTX 4090
sandboxes. For the OOM follow-up, require victim exit 137, readable logs/events,
a surviving CI parent and heartbeat, unchanged sandbox identity, and a working
subsequent exec. Cover concurrent allocators, shared memory, nested Docker,
rapid allocation and checkpoint/restore.

For VM-only rollout, run `TestMicroVMResourcesAndOOM` and the same survival
checks, verify capacity reserves, restores and GPU support. VM results cannot
stand in for gVisor acceptance. No customer migration or runtime default change
is included. Validation and Okteto use remain deferred; only the separately
authorized goproc release binaries were built.
