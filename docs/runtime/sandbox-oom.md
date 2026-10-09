# Sandbox memory failures

The customer uses **gVisor**. This fixes the reproduced VMA crash;
**gVisor process-level OOM enforcement remains a follow-up**. The separate
VM implementation does not satisfy that requirement.

## Diagnosis

The prod2 repro exhausted the host's 65,530 VMA limit: systrap had 65,364
mappings at the last sample, then `munmap` returned `ENOMEM` and panicked
gVisor. Host memory peaked at 10.30 GiB against 22 GiB, with zero OOM kills.
Worker startup raises `vm.max_map_count` to 4,194,304, preserving higher values.
The earlier customer SIGSEGV has not been independently attributed.

The memory-hog case is separate: gVisor's [memory controller](https://github.com/beam-cloud/gvisor/blob/f5056750a788c74f5aabcb328dd4973a56b56f42/pkg/sentry/fsimpl/cgroup2fs/memory.go#L165)
stores the advertised 16 GiB limit without enforcing it. Host Linux accounts
sentry/stub/gofer under a buffered 22 GiB cap; host group killing and the
worker's gVisor OOM stop path terminate the sandbox.

## Required gVisor follow-up

1. Enforce the exact workload budget through atomic sentry memory charging and
   reclaim. Cover anonymous memory, fork/COW, shared memory, tmpfs, file mappings
   and GPU host allocations; charge shared pages once. RSS polling cannot enforce it.
2. Reserve headroom for PID 1 and goproc outside that budget. Place execs and
   nested Docker workloads before execution. After reclaim fails, SIGKILL an
   eligible guest process while preserving the sentry, manager and healthy
   processes. Changing host `memory.oom.group` alone cannot provide this.
3. Report guest OOM counters/events with victim PID, usage and limit, bypassing
   sandbox stop/delete. goproc v0.1.16 supplies exit 137 and completed
   Wait/Status/streams with retained logs.
4. Preserve charging/events across checkpoint/restore; release and pin runsc
   before enabling worker integration.

## Separate VM implementation

Native VMs use `/beam-workload` for the exact budget, individual-process OOM
killing and no swap; PID 1 and goproc use `/beam-control`. Guest RAM adds 512 MiB
control headroom, with the existing 256 MiB host VMM reserve. goproc v0.1.16
atomically places exec children; application OOM events leave the sandbox up.
Recreate memory checkpoints carrying old init/manager state.

## Checks before rollout — deferred

- **gVisor:** rerun Rust on 4 vCPU / 16 GiB RTX 5090 and RTX 4090. OOM acceptance
  requires exit 137, readable logs/events, surviving CI parent and heartbeat,
  unchanged sandbox identity and a subsequent exec. Cover concurrent/rapid
  allocators, shared memory, nested Docker and checkpoint/restore.
- **VM:** run `TestMicroVMResourcesAndOOM` and the same survival checks; verify
  capacity reserves, restores and GPU support. VM results do not establish
  gVisor acceptance. No customer migration or runtime default change is included.

Validation and Okteto use remain deferred. Only the separately authorized
goproc release binaries were built.
