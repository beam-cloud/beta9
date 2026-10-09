# Sandbox memory failures and rollout

The affected customer runs **gVisor**. The VMA change addresses their reproduced
under-limit runtime crash. The VM-only OOM implementation below does **not** fix
their process-level OOM requirement. That requirement remains unimplemented for
gVisor; customer acceptance must run on gVisor without changing runtimes.

The prod2 Rust repro failed because the host's `vm.max_map_count` was 65,530.
A gVisor systrap subprocess reached 65,364 VMAs at the last sample; splitting
a VMA during `munmap` then failed with `ENOMEM` and panicked the runtime.
The traced run used at most 10.30 GiB of host cgroup memory against a 22 GiB
limit, with zero OOM kills. This is distinct from exceeding application memory.
Worker startup raises the map limit to gVisor's recommended 4,194,304, preserving
higher existing values. The older customer SIGSEGV has not been independently
attributed to this cause.

The existing gVisor memory test has a different cause: its displayed 16 GiB
cgroup limit is virtual, while the host enforces a buffered 22 GiB budget on
sentry/stub/gofer processes. Host group OOM killing and the worker's explicit
gVisor OOM stop path terminate the sandbox. Changing only the group flag cannot
give host Linux independently accounted guest application processes.

## Required gVisor OOM change — not implemented

The pinned runsc source confirms the enforcement gap: the v1 memory limit is a
[stub controller file](https://github.com/beam-cloud/gvisor/blob/f5056750a788c74f5aabcb328dd4973a56b56f42/pkg/sentry/fsimpl/cgroupfs/memory.go#L87).
The v2 [memory controller](https://github.com/beam-cloud/gvisor/blob/f5056750a788c74f5aabcb328dd4973a56b56f42/pkg/sentry/fsimpl/cgroup2fs/memory.go#L165)
explicitly retains `memory.max` without enforcing it, and its `memory.events`
reports fixed zero counters. The sentry has per-cgroup memory accounting and
guest process identities; enforcement and OOM policy must use those identities.

Implement this as a separate change in the gVisor fork, then wire its guest OOM
events into the worker:

1. Enforce the exact requested application budget inside the sentry. Charge
   committed pages atomically against a workload cgroup and its ancestors before
   allowing allocations/faults to proceed; release charges on reclaim/free.
   Cover anonymous memory, fork/COW, shared memory, tmpfs, file mappings and
   GPU-related host-memory allocations without double-counting shared pages.
   Existing reporting counters need auditing; periodic RSS sums cannot provide
   an exact hard limit, and returning ENOMEM alone does not meet the kill policy.
2. Keep guest PID 1 and goproc in a separate control budget with runtime
   headroom. Place every exec into the guest workload before it runs, including
   descendants and nested Docker containers. The pinned kernel supports
   `CLONE_INTO_CGROUP`, but placement alone does not enforce memory.
3. Reclaim within the workload; if allocation still cannot fit, select and
   SIGKILL one eligible guest process/thread group, then wait for released
   charges before retrying. Exclude the process manager and control processes.
   Serialize concurrent OOM decisions and keep the sentry/gofer alive. Do not
   use host `memory.oom.group=0` as a substitute for guest victim selection.
4. Expose real guest OOM counters and an event containing victim PID, usage,
   limit and reason. Forward it as an application event without calling
   `handleOOMKill` or canceling/deleting the sandbox. Keep genuine host runtime
   OOMs distinct. The goproc signal/status change in this branch can supply exit
   137 and retain output once a guest victim is killed.
5. Preserve controller state, charges and event delivery across checkpoints
   and restores. Publish a pinned runsc release and checksum before enabling
   the worker integration; do not enable a policy with only reporting support.

Acceptance before a gVisor rollout: use a 4 vCPU / 16 GiB gVisor sandbox with a
memory hog, small CI parent and independent heartbeat. Require an individual
victim exit 137, retained stdout/stderr and OOM event, continued heartbeat,
unchanged sandbox identity and successful subsequent exec. Repeat with several
allocators, fork/shared-memory workloads, nested Docker, rapid allocations and
checkpoint/restore. Then rerun the exact Rust script on RTX 5090 and RTX 4090
gVisor sandboxes, checking both guest and host events and VMA counts. All checks
remain deferred at the user's request.

## Separate VM-only implementation

This implementation applies to **native Linux MicroVM sandboxes**. It is an
additional runtime change, not customer remediation or a migration decision:

- `/beam-workload` enforces the exact requested memory in bytes (16 GiB means
  17,179,869,184 bytes), with `memory.oom.group=0` and no swap allowance.
  All exec descendants and nested Docker containers share this aggregate budget.
- PID 1 and goproc stay in `/beam-control`, outside that budget. Guest RAM adds
  512 MiB for control/kernel overhead; the host cgroup also adds the existing
  256 MiB VMM headroom. Capacity planning must include both reserves.
- goproc places execs into the workload before they run using
  `CLONE_INTO_CGROUP`. SIGKILL completes with exit 137 through Wait, Status and
  streamed exec, retaining stdout/stderr. Other signals use 128 + signal.
- Guest `memory.events` produces `runtime.application_oom_killed` events with
  kill count, limit, observed usage and peak. These events do not mark the
  runtime OOM-killed or stop/delete the sandbox. A genuine host runtime OOM
  remains a separate terminal failure.

Linux chooses an individual victim; a CI parent can observe its child's exit
137 and report the build failure while the sandbox and unrelated processes
continue. Manual SIGKILL also returns 137, but does not create an OOM event.
The pinned goproc v0.1.15 is patched during both worker image builds, keeping
the existing client protocol.

## Required checks before a VM rollout

No fix build, tests, deployment or Okteto validation were run for this change.
The added integration assertions are acceptance checks to run later.

1. Build both worker image paths and run `TestMicroVMResourcesAndOOM` in the
   isolated MicroVM harness. Confirm exact limits, control/workload membership,
   exit 137, retained logs, OOM events, a surviving CI parent and heartbeat,
   unchanged sandbox PID and successful subsequent exec.
2. Repeat with concurrent allocators and nested Docker containers: aggregate
   memory must be capped, individual victims killed, and other processes survive.
   Check both buffered and streamed exec completion, without lost final output.
3. Confirm capacity reservation for control/VMM overhead, CPU quota, memory
   checkpoints and restores, and continued event delivery after restores.
   Existing memory checkpoints carry old init/manager state: recreate those
   sandboxes before relying on the new semantics.
4. Rerun the exact Rust script on 4 vCPU / 16 GiB RTX 5090 and RTX 4090 VMs;
   compare guest/host memory events and host VMA counts. Exercise a 16 GiB memory
   hog and verify a process kill, exit 137 and retained CI logs without losing
   communication.

Deploy the VMA fix independently. Optional VM-only canaries can use `use_vm`
after these checks and GPU support pass. Those canaries cannot satisfy the
customer's gVisor acceptance criteria. Runtime defaults and production settings
are unchanged here; no customer migration is included.
