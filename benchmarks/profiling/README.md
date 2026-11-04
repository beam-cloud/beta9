# VM burst profiling

Research snapshot for the VM burst investigation. Production consolidation is on
`ll/fix-race-contention`. This branch retains instrumentation and experiments so
that their evidence can be inspected without adding them to the production PR.

## API burst harness

Run from the repository root with its SDK dependencies installed:

```sh
PYTHONPATH=sdk/src python benchmarks/vm_burst.py \
  --context staging-admin --pool microvm-cpu \
  --worker-id WORKER_ID --image-id PREPARED_IMAGE_ID \
  --count 10 --cycles 1 --trace-startup --retain-mismatches \
  --output /tmp/burst-10.json
```

`--okteto` uses the existing local forwarding ports 11993/11994. The harness
requires the staging gateway profile, verifies worker/pool placement, records
resource ownership before launch, and removes only its uniquely named VMs.
`--cycles 0` measures startup and initial proofs without pause/restore.

Image preparation and HTTP client connections are outside the launch clock.
Every launch creates a fresh VM and root disk. Each phase reports whole-burst
wall time and per-VM nearest-rank p50/p95/p99/max. Proofs compare RAM, disk and
boot identity; mismatches are retained when requested. Fresh boot IDs must be
unique. A successful diagnostic retry does not replace an original failed proof.

For low counts, p95 and p99 may both be the maximum. The scaling study pooled
100 launches at each size: ten bursts of 10 and two bursts of 50. Their observed
per-VM p95/p99 were 458/465ms and 1116/1123ms. All 200 proofs passed and all
owned VMs were removed. These are observations from one staging testbed, not
an independently sampled production tail-latency guarantee.

## Timing boundaries

`--launch-gate FILE` holds the burst after setup. `--post-startup-gate FILE`
holds proof seeding immediately after measured startup. `--post-launch-gate`
holds pause after initial proofs. `--post-pause-gate FILE` holds restore after
pause. Files are created by the external controller to release each boundary.

Use these boundaries to keep proof commands outside startup profiles and restore
outside pause profiles. Early fixed-duration captures mixed those workloads;
their aggregate CPU/FUSE counts must not be called startup-only measurements.

## Worker and guest traces

`BETA9_TRACE_VM_STARTUP=1` enables bounded worker lifecycle traces and guest boot
boundaries. API requests use `X-Beam-Startup-Trace: 1`. Traces include API/stub/SQL,
claim RPC, network, disk, cgroup, filesystem daemon, hypervisor, control connection,
process-manager listener/readiness and checkpoint phases. Spans overlap and may
be nested aliases; do not sum their percentiles.

Guest CPU counters use Linux USER_HZ=100. Steal is scheduling delay, not CPU work.
Counters include kernel boot; process counters reset across the agent exec.
Guest, worker, gateway and client clocks are separate. Do not align their Unix
epochs without calibration. Use component-local offsets and duration comparisons.
Restore does not yet have complete attribution: observed gaps inside the worker
trace were about 3.95s median / 6.05s p95 in the fully traced 100-VM baseline.

`BETA9_PROFILE_STARTUP=1` enables loopback pprof and block/mutex sampling.
`BETA9_MICROVM_KERNEL_INIT_TRACE=1` records initcall timings in the guest kernel
ring buffer; loglevel=4 suppresses their serial output. Both were disabled for
the 10/50 scaling study. The temporary diagnostic gateway still had sampling
active, identically for both loads.

## Native host capture

Copy this directory to the dedicated Linux test node. The helper requires perf,
bpftrace, tcpdump, curl, iproute2 and permission to inspect the worker:

```sh
python3 native_vm_profile.py --port PPROF_PORT --phase startup --seconds 4 \
  --worker-container WORKER_CONTAINER --network-interface NIC \
  --output /tmp/burst-native-startup
```

The helper acknowledges perf FIFO control before reporting ready, uses
`--clockid mono`, and records the clock in `clock.json`. Host perf samples do not
resolve guest kernel instructions on this testbed: `svm_vcpu_run` is not evidence
of a specific guest kernel bottleneck. BPF records hypervisor lifecycle and FUSE
operation counts. TCP capture contains SYN/RST headers only. Node-wide data must
be filtered to the harness interval and owned VM IDs before attribution.

## Evidence

`../results/burst-scaling.json` contains pooled and per-cohort results, stage
quantiles and guest CPU/steal observations. `mechanism-evidence.json` retains
correctness and performance experiments, including rejected approaches and
measurement-scope corrections. An experiment that passed correctness did not
necessarily improve whole-burst latency.

Release decisions and their supporting measurements: [consolidation.md](consolidation.md).
The reproduced deferred network-sweep fix is carried here as well.

Latest release validation after the goproc CI release and duplication cleanup:
[release-followup.md](release-followup.md).
