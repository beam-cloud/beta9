# Sandbox memory failures and rollout

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

The separate OOM change applies to **native Linux MicroVM sandboxes**:

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

## Required checks before rollout

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
4. Rerun the exact Rust script on 4 vCPU / 16 GiB RTX 5090 and RTX 4090 sandboxes;
   compare guest/host memory events and host VMA counts. Exercise a 16 GiB memory
   hog and verify a process kill, exit 137 and retained CI logs without losing
   communication.

Deploy the VMA fix independently. Route OOM acceptance canaries through
`use_vm` only after these checks and GPU support pass; gVisor does not gain
native process-level OOM semantics from this change. Expand VM routing after
canaries pass. Runtime defaults and production settings are unchanged here.
