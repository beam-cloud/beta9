# GPU microVMs for application kiosks

Design proposal, 2026-10-26. This document describes future work; GPU microVMs
are not an existing VM capability. It makes no assertion about GPU capacity,
provider availability, or price. No infrastructure changes or provisioning are
part of this proposal. CPU-only kiosk prototyping and measurement continue.

## Decision and success criteria

Implement a separate, explicitly selected Cloud Hypervisor pool that assigns
one qualified physical PCI GPU exclusively to each VM through VFIO. Qualify a
GPU, host firmware/kernel, guest kernel/driver, display server, and encoder as
one versioned profile. Start with Linux amd64, one GPU per VM, and cold boot
from durable disks. Preserve the existing CPU-only pool and defaults.

The target is an ordinary Linux application's entire graphical session:
GTK/Qt windows, Electron, dialogs, drawing canvases, 3D views, and media. An
application must not integrate a streaming SDK to run. Application MCP servers
retain the existing guest-process and protected-port contract.

A successful GPU profile must demonstrate both accelerated application
rendering and hardware video encoding through the actual protected browser
URL. A working CUDA program or `nvidia-smi` alone does not meet this condition.
GPU assignment must fail closed when the required graphics or encoder path is
unavailable; a software fallback is a reported diagnostic mode, not a passing
GPU session. Functional compatibility and measured browser latency determine
release readiness, rather than the presence of a device or a configured FPS.

## Current implementation and measured baseline

The repository deliberately rejects GPU requests for VMs:

| Location | Current behavior and implication |
| --- | --- |
| `pkg/types/vm.go`, `VMSpec` | Explicitly CPU-only, without GPU fields. |
| `pkg/abstractions/vm/api.go`, `decode` | Rejects unknown fields, including GPU options. Image lookup also rejects GPU image builds. |
| `pkg/gateway/services/stub.go`, `validateUseVM` | Rejects GPU type/count together with `use_vm`. |
| `pkg/abstractions/pod/vm.go`, `RunVM` | Requires a persistent sandbox VM and rejects `RequiresGPU()`. |
| `pkg/scheduler/scheduler.go`, `runtimeAcceptsRequest` | Only CPU sandbox/VM requests enter a `microvm` worker; VM requests cannot fall back to a container runtime. |
| `pkg/runtime/microvm_linux.go`, `Capabilities`/`Prepare` | Advertises RAM checkpointing but no GPU or CDI support. Clears OCI Linux device entries; passing host `/dev/nvidia*` through the container spec would not attach a guest PCI device. |
| `pkg/runtime/microvm.go`, `microVMHypervisorArgs` | Attaches virtio-fs, block disks, net, vsock, RNG and balloon; emits no `--device`. |
| `docker/Dockerfile.microvm` | Pins Cloud Hypervisor v53.0 and guest Linux 6.12.111. Boots the custom kernel directly, with built-in drivers, no initramfs or module bundle. The supported build/runtime path is currently amd64. |
| `docker/microvm/guest-kernel.config` / `.fragment` | `CONFIG_MODULES` and `CONFIG_DRM` are disabled; the fragment disables NUMA. Existing VFIO symbols in this *guest* config do not supply a graphics driver or configure the *host* IOMMU. |
| `sdk/src/beta9/abstractions/vm/install` and `kiosk` | Installs Selkies 2.1.0 and distro Xvfb, starts display `:1`, Openbox and `h264enc`. Selecting that codec does not prove a hardware encoder was selected. |

The current staging guest was measured with `glxinfo -B`: `Accelerated: no`,
renderer `llvmpipe (LLVM 20.1.2)`, and no `/dev/dri`. It is a CPU reference,
not a failed GPU deployment. The ongoing CPU effort includes additional CPU
capacity and encoder-quality experiments; this proposal does not change it.

The observed path is browser → raw TCP NLB port 443 → authenticated gateway
ICE routing → private guest port 8443. On the measured Mac Retina viewport,
about 1728 × 789 CSS pixels at DPR 2 become 3456 × 1578 captured pixels.
Small-sample optical input response with the older 4-CPU/8-GiB H.264 CBR setup
was approximately 180–225 ms; browser telemetry showed roughly 53 ms media
RTT, under 1 ms mean receive jitter-buffer delay, and 2.6 ms decode per frame.
These are exploratory observations, not release-quality percentiles or a
measurement isolating server encoding. A GPU cannot remove physical RTT,
application event-loop delay, TCP retransmission stalls, or client presentation
delay.

## Rendering, capture and encoding are separate requirements

| Stage | Requirement | Evidence to collect |
| --- | --- | --- |
| Application rendering | Correct GLX/EGL/Vulkan driver and toolkit/browser configuration; no forced software renderer. Some 2D application work remains on the CPU. | GL/EGL renderer, Vulkan device, application GPU diagnostics, representative visual tests. |
| Display and composition | A headless display whose application framebuffer is actually GPU-backed; correct resize, DPI, windows and cursor behavior. | Display-server log, screen/device association, frame pacing, dialog and resize tests. |
| Capture and conversion | Capture the complete session with bounded buffering; measure CPU readback, RGB-to-YUV conversion and GPU upload where present. | Selected capture backend, buffer/copy path, capture/convert time and queue depth. |
| Encoding | A qualified hardware H.264 encoder at the required physical resolution, FPS and chroma format. | Encoder capability probe, actual backend/session log, encode timing, load and failure status. |
| Delivery | Negotiated browser codec, recovery after congestion, and bounded input/media queues. | Optical input/frame counters, WebRTC statistics and browser-visible images. |

NVIDIA documents NVENC as a dedicated engine separate from graphics/CUDA
cores. Consequently, compute capability, graphics capability and encoder
capacity must be inventoried separately; verify the exact GPU SKU and runtime
encoder capabilities rather than inferring them from a family name.
[NVIDIA NVENC Application Note](https://docs.nvidia.com/video-technologies/video-codec-sdk/13.0/nvenc-application-note/index.html)

Selkies 2.1.0 supports hardware NVENC/VA-API paths and software fallback. Its
container images also contain display integration that this repository's
distro-Xvfb installation does not automatically acquire. Keep `h264enc` for
initial browser compatibility, but record the selected backend explicitly.
This is Selkies/pixelflux integration work, not a GStreamer pipeline change;
the pinned release moved capture and encoding away from FFmpeg.
[Selkies 2.1.0 release](https://github.com/selkies-project/selkies/releases/tag/2.1.0)

For perspective, 3456 × 1578 RGBA at 60 FPS moves approximately 1.31 GB/s
before compression for each full-frame copy. That is a local framebuffer
bandwidth calculation, not the compressed network bitrate. First bring-up may
use a measured readback/upload path, but it must not be called zero-copy.
Optimize away copies only after the complete path is correct.

## Supported device path and excluded shortcuts

Cloud Hypervisor v53.0 documents PCI VFIO assignment using a sysfs device path
and requires related functions, such as a GPU's audio function, to be handled
together. The initial worker generates arguments like the following from a
trusted allocation, never from user-supplied paths:

```text
--platform vfio_p2p_dma=off
--device path=/sys/bus/pci/devices/0000:65:00.0/,id=gpu0
         path=/sys/bus/pci/devices/0000:65:00.1/,id=gpu0-audio
```

The addresses are illustrative. Only attach functions present in the qualified
inventory; the complete isolation/reset domain is reserved to the same VM.
The v53 VFIO documentation also describes GPU-specific BAR/P2P adjustments;
do not enable them generically. [Cloud Hypervisor v53.0 VFIO guide](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v53.0/docs/vfio.md)

| Option | Disposition |
| --- | --- |
| Whole physical PCI GPU, `vfio-pci` | Initial implementation. Host dedicates the complete qualified device/domain; guest owns the vendor driver. |
| SR-IOV VF | A PCI VF can use PCI VFIO assignment, but that does not establish graphics, display heads, encoder access, reset isolation, licensing or vendor support for a particular GPU VF. Separate profile qualification and PF/VF management are required before exposure. |
| Mediated device / NVIDIA vGPU | Linux provides an mdev framework, but that alone does not establish Cloud Hypervisor/vendor integration. No mdev profile is proposed for v1; require a documented CH device path and a tested, supported host-manager/guest-driver combination first. |
| MIG, CUDA time slicing, Thunder | Do not treat compute partitioning or the existing virtualized GPU manager as a graphical PCI device. Do not advertise graphical or encoder capability based on these labels. |
| virtio-gpu / virgl / rutabaga / generic vhost-user GPU | Not a usable v53 alternative. Its generic frontend accepts a GPU device type, but upstream explicitly says virtio-GPU is known not to work because required protocol messages are unsupported. Do not infer support from flag parsing. |
| Host render-node/CDI bind mounts | Useful for shared-kernel containers; not equivalent to a GPU PCI function inside this VM. Do not share host driver libraries or device nodes as the microVM implementation. |

The mdev and vGPU statements distinguish available frameworks from a qualified
product: [Linux mediated-device API](https://www.kernel.org/doc/html/latest/driver-api/vfio-mediated-device.html),
[NVIDIA vGPU support matrix](https://docs.nvidia.com/vgpu/latest/product-support-matrix/).
The vhost-user limitation is explicit in
[Cloud Hypervisor v53.0 generic vhost-user documentation](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v53.0/docs/generic-vhost-user.md).

## Host qualification and physical topology

Use a dedicated Linux host/pool for first bring-up. A GPU instance type or
existing serverless GPU pool is not evidence that the machine can host KVM
and reassign its GPU to a nested guest. Verify accessible `/dev/kvm`, device
ownership, IOMMU support and provider permission for this exact deployment.
Prefer direct control of the PCI topology for the first profile; no existing
provider SKU is declared suitable by this spec.

The host admission probe must establish:

1. Hardware virtualization and enabled DMA/interrupt remapping in firmware and
   host Linux; real IOMMU groups; usable VFIO APIs. Never use no-IOMMU mode,
   ACS-override group splitting, or unsafe interrupts to make a host pass.
2. Stable GPU identity, PCI BDFs, all companion functions, upstream bridge,
   IOMMU group and reset domain. Reject a domain containing host management,
   storage, network or another tenant's assigned device. VFIO groups are the
   isolation ownership unit; topology can make that unit larger than a card.
   [Linux VFIO isolation model](https://www.kernel.org/doc/html/latest/driver-api/vfio.html)
3. Qualified reset and error recovery after normal shutdown, guest crash,
   driver hang, VMM SIGKILL and worker restart. Discover supported reset methods
   and test them; a sysfs `reset` file or advertised FLR is not sufficient proof.
   A secondary-bus reset must never affect another live VM. Quarantine failures
   until an operator-approved recovery or host power cycle passes admission.
4. Sufficient PCI MMIO/BAR address space, MSI/MSI-X support and verified guest
   initialization without option-ROM/firmware assumptions. This runtime direct
   boots a kernel; do not silently add QEMU/OVMF behavior or undocumented GPU
   masking workarounds. Add a different boot profile only if evidence requires it.
5. NUMA node and PCI locality for CPU cores, RAM and GPU, plus PCIe link width
   and speed. Initially place the VM within one host NUMA node near the GPU and
   bind VMM/vCPU/guest RAM allocation there. Reserve enough local memory and
   account for DMA-pinned memory, VFIO mappings/memlock limits and VMM overhead.
   Budget the full guest RAM; do not count the current balloon/free-page-reporting
   savings until reclaim has been qualified with the GPU's DMA mappings.
   Current guest single-node NUMA can remain if the VM fits; larger/multi-node
   profiles need explicit topology and a different guest kernel configuration.
6. Exact host kernel, VFIO implementation, CH binary/checksum and firmware
   versions. Start with the documented group/sysfs VFIO path. v53 also parses
   `iommufd`/FD-based device access, but use it only as a separately tested host
   backend; it does not create a migration capability. [CH v53.0 configuration source](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v53.0/vmm/src/config.rs)

Initially leave peer-to-peer GPU DMA and multi-GPU cliques disabled by policy.
The single-GPU kiosk does not require them, and cross-device paths complicate
isolation and reset domains. Reserve the GPU for the complete VM lifetime;
do not let host display/CUDA services or serverless workers open it concurrently.
Prefer boot-time per-device binding on dedicated hosts rather than repeatedly
unbinding an actively used host graphics driver.

Host IOMMU isolation is mandatory even when no guest `virtio-iommu` is exposed.
Adding a virtual IOMMU inside the guest does not replace host DMA protection;
keep the initial guest device topology minimal and qualify changes separately.

Keep host locality and user-facing network locality as separate admission
facts. A physically local CPU/GPU pair on a distant host can still produce an
unacceptable interactive session.

## Guest kernel, image and graphical session

Create a versioned graphics kernel/profile alongside the current CPU kernel.
Do not replace the kernel for every microVM. `docker/Dockerfile.microvm` must
build/export that kernel, exact matching module tree and required firmware;
the worker selects their trusted paths from the GPU profile. Enable DRM/KMS,
DMA-BUF and vendor dependencies appropriate to that profile, and loadable
modules when the vendor driver requires them. Pin kernel release/config,
module ABI, vendor driver/userspace libraries, Mesa/ICDs and firmware as an
immutable image compatibility manifest. Guest updates must not silently
substitute a distro kernel for the kernel the VMM actually boots.

For NVIDIA, install the complete guest graphics/encoding components, not just
a compute-only userspace package. NVIDIA describes separate core, modeset,
UVM and DRM kernel modules; compile/sign/package the selected compatible
modules for the actual custom guest kernel. An apt/DKMS build against the
image builder's running host kernel is not valid. Build on CPU infrastructure;
hardware is needed for qualification, not to turn the VM image build into a
GPU-allocated application. [NVIDIA kernel-module guide](https://docs.nvidia.com/datacenter/tesla/driver-installation-guide/kernel-modules.html)

Extend `cmd/vminit/main.go` and `systemd.go` only where necessary to expose
guest-created device nodes and run a GPU readiness unit before
`beam-desktop.service`. The guest already mounts devtmpfs; actual PCI probing
and driver initialization must produce the guest nodes. Apply device/group
permissions to the guest session user and to explicitly requested Docker
workloads inside the guest. Guest Docker GPU integration is a separate feature
from assigning the GPU to the VM; do not promise it by enabling Docker alone.

Use a GPU-backed Xorg display as the initial qualified session while keeping
`:1`, Xauthority, Openbox, application argv/cwd/env, clipboard, input and window
management compatible. A headless NVIDIA X server can start without a monitor
with its documented empty-initial-configuration behavior, but virtual screen
sizes and dynamic resizing still need qualification. Do not assume a dummy
Xorg driver accelerates the screen. [NVIDIA X configuration options](https://download.nvidia.com/XFree86/Linux-x86_64/580.95.05/README/xconfigoptions.html)

Upstream Selkies has patched Xvfb and native headless Wayland paths; these are
possible later display profiles, not reasons to assume the current distro
Xvfb already uses the GPU. A patched Xvfb profile needs pinned build/source and
DRI3/driver verification. A Wayland profile needs end-to-end input, screenshot,
resize, clipboard, Xwayland and application compatibility work: existing
`xdotool`, `scrot` and `x11grab` helpers cannot be reused unchanged for native
Wayland windows. [Selkies 2.1.0 graphics setup](https://github.com/selkies-project/selkies/blob/2.1.0/docs/start.md)

The encoder runs inside the guest against the guest driver. NVIDIA's encode
library is supplied by that driver; probe codec, dimensions and formats at
runtime. Begin with H.264 8-bit and a browser-negotiated chroma format; verify
text fidelity instead of equating a requested full-color flag with delivered
4:4:4. Use a low-latency preset without frame reordering/lookahead where the
chosen backend supports that configuration, and bound capture/encode queues.
Record actual settings. Hardware encoding can still consume substantial CPU
for capture/conversion or copy frames through system RAM.
[NVIDIA NVENC programming guide](https://docs.nvidia.com/video-technologies/video-codec-sdk/13.0/nvenc-video-encoder-api-prog-guide/index.html)

Extend kiosk status with actual renderer, display/capture backend, encoder,
codec/chroma, physical capture size, driver/profile version and a concise
failure reason. Probe before application readiness and again after a driver
or encoder restart. The current SDK/MCP recording helpers intentionally use
FFmpeg `libx264`; leave that behavior documented or introduce a separate
explicit hardware recording option. Browser streaming acceleration does not
automatically accelerate recordings or PNG screenshots.

## API, SDK, CLI and MCP contract

Proposed additive `VMSpec` field, not implemented:

```json
{
  "gpu": {"profile": "kiosk-pci-v1", "count": 1},
  "kiosk": {"command": ["/usr/local/bin/application", "/workspace/project"]},
  "idle_action": "stop"
}
```

`kiosk-pci-v1` is an illustrative future profile ID, not an available SKU.
A profile resolves to an operator-qualified hardware class, guest compatibility
manifest, exclusive PCI allocation mode, graphics API capabilities, H.264
encoder limits and lifecycle restrictions. Publish a capability catalog only
for profiles actually admitted in that installation, distinguishing configured
support from presently free capacity. Do not accept arbitrary PCI paths, device
IDs, ROMs, reset commands, host driver names or hypervisor arguments in the API.

Omitting `gpu` retains the current CPU-only contract. Initially require
`count == 1`; reject unsupported fields/counts/profiles. Explicit GPU requests
must not fall back to CPU or to a shared-kernel runtime. Validate guest image
manifest compatibility before scheduling. Preserve request-ID idempotency,
creation digests, template inheritance, redaction and workspace authorization.
The GPU request is immutable after VM creation in v1.

Carry the profile through `Service.prepare` → immutable stub →
`ContainerRequest`; retain ordinary type/count fields for accounting, but add
an explicit PCI-assignment/profile discriminator so app GPU requests cannot be
mistaken for VM assignment. Relax CPU-only guards only for this validated path
and qualified microVM workers. Do not simply flip the runtime's global `GPU`
boolean: the existing worker uses it to trigger NVIDIA/CDI container injection.
Add a distinct PCI-assignment capability and per-request checkpoint eligibility.

Expose a read-only resolved runtime view containing profile, model, driver and
render/encode health, plus `memory_pause: false` and `live_migration: false`.
Keep host BDFs, group IDs, lease tokens and internal file paths out of public
responses. Distinguish unsupported configuration, incompatible image,
unavailable capacity, assignment failure and unhealthy-device errors. A queued
start must remain honest about pending GPU capacity and normal scheduling
timeouts; `auto_resume` requests a cold start and does not reserve an idle GPU.

Add the same typed field to Python `VM`, CLI `vm new --gpu-profile ...`
with count fixed to one, server MCP `vmSpecSchema`, and local image-build
options for choosing the compatible graphics image/profile. Update generated
protobuf/SDK surfaces where the internal request or agent protocol changes.
MCP descriptions and SDK docstrings must qualify the existing universal
pause/resume wording. Keep the existing confirmation behavior for disruptive
VM actions; adding a GPU does not need a second approval model.

## Inventory, scheduling and assignment

Current agent capacity comes from NVIDIA driver queries, GPU names/IDs/counts
and container-oriented preflight. A device bound to `vfio-pci` can disappear
from `nvidia-smi`; it must not disappear from PCI inventory or become eligible
for double allocation. Current `FreeGpuCount` alone cannot represent reset
domains, compatible graphics/encoder profiles or pending cleanup.

Extend agent registration/heartbeat and machine/worker inventory with:

| Internal field group | Required content |
| --- | --- |
| Stable identity | Host-scoped device ID, vendor/device/subsystem IDs, serial/UUID where available, BDFs and companion functions. |
| Isolation/topology | IOMMU group and reset-domain membership, NUMA node, PCIe link/topology, allowed reset method. |
| Qualified capability | Profile IDs, host/kernel/VFIO/firmware versions, guest compatibility manifest IDs, tested graphics/codec limits. |
| Ownership and health | Binding driver, admission generation, state, lease owner and fencing generation, last successful reset/sanitization/health check. |

Use explicit states `available → reserved → assigned → cleaning → available`,
with `quarantined` reachable on any uncertain transition. Aggregate free counts
are derived from healthy, unleased domains; container GPU capacity and VFIO
capacity are disjoint even when they refer to the same physical hardware.

The scheduler must require `runtime=microvm`, PCI capability, exact profile
compatibility, free CPU/RAM/locality and an atomic device-domain lease. The
existing Redis worker capacity transaction in `pkg/repository/worker_redis.go`
must not report capacity available until the device manager has made it safe.
Use one host-level owner across agent/worker slots, with durable allocation
journaling and fencing by machine/worker/VM generation. A lease timeout or lost
heartbeat fences admission; it does not prove the old VMM released the GPU.

Launch ordering:

1. Atomically reserve resources and the entire domain, then persist its lease.
2. Host controller revalidates identity, group/reset topology, driver binding
   and generation; confirms no competing users; performs the qualified reset.
3. Supply the worker only the approved allocation. Start CH with the generated
   device arguments, matching kernel profile and bounded VFIO permissions.
4. Wait for guest PCI/driver health, then display, real renderer, encoder,
   application window and optional MCP handshake. Keep exec readiness distinct
   from graphical service readiness, as the current SDK already does.
5. On any failure, reap CH and its helpers before detaching/cleaning. Mark the
   allocation available only after reset, sanitization and health checks pass.

`pkg/agent/worker_container.go` currently launches privileged host-network
workers and uses Docker `--gpus` for GPU slots. A VFIO slot must bypass the
NVIDIA container-runtime setup and make only its approved VFIO control/group
resources and required sysfs view available to the VMM. Put privileged binding
and reset operations in a narrow host controller, not in guest-originated RPCs.
Existing privilege does not eliminate the need to check the allocation.

Worker restart currently kills leftover VMMs in `sweepLeftoverVMs`; extend this
reconciliation to recover the lease journal, reap confirmed owners, reset and
quarantine as necessary. Cover partial startup before `microVMInstance` exists,
`Delete`/teardown, OOM, disk-finalization failure, forced stop, worker rollout
and host reboot. Disk cleanup completion and GPU cleanup completion are both
required barriers before capacity returns.

Provisioning must require these host capabilities in addition to GPU labels.
Extend `pkg/agent/preflight.go`, agent protocol, `pool_agent.go` and compute
offer/launch filtering; leave a provider unsupported when it cannot establish
KVM/IOMMU/device reassignment. Initial alpha uses a dedicated private pool
restricted to one workspace. Multi-workspace reuse requires evidence of
vendor-defined device/VRAM sanitization and isolation after adversarial guest
shutdown; a successful reset is not by itself a proof of erased tenant data.

## Stop, pause, snapshots, fork and migration

Saved application files and a live GPU execution context are different state.
GPU memory, command queues, fences, driver state and encoder sessions are not
reconstructed merely by saving guest RAM or a qcow disk.

| Operation | Initial GPU VM contract |
| --- | --- |
| Stop/start | Keep existing disk-durability barrier; terminate guest/VMM, clean and release GPU. Start leases a compatible GPU and cold-boots services/applications. Unsaved application state is lost. `--no-snapshot` still does not waive final filesystem durability. |
| Idle stop / auto-resume | Supported as stop/cold-start, subject to available compatible capacity. Reject `idle_action=pause` for the GPU profile. |
| Disk snapshot/template | Supported saved-disk artifact with launch spec/profile requirements. Existing root snapshot path freezes/flushes filesystems; this is not an application save command or GPU/RAM capture. The public VM artifact references the root; do not promise independently mounted disks/volumes are cloned. |
| Fork | Cold boot a new identity and independent writable root from the snapshot. Acquire a separate GPU lease; source can keep running only if another qualified GPU is free. No cloned VRAM, open windows, undo stacks or encoder connections. Additional disks retain their existing explicit sharing/snapshot semantics. |
| Pause/resume with preserved RAM | Reject before lifecycle mutation, guest freeze or `vm.pause`. Return an actionable unsupported-operation error; do not silently downgrade pause to stop. The current terminal checkpoint releases compute and cannot safely stand in for a GPU sleep. |
| RAM snapshot/restore | Reject the worker/runtime memory-snapshot path for this profile, including automatic startup checkpoints. Reject importing RAM artifacts containing unknown GPU devices. `start(cold=true)` remains the explicit recovery behavior. |
| Live migration / memory fork | Unsupported for v1. Cold relocation through committed disks is possible only to a compatible host/profile, with a new lease and application restart. |
| Guest reboot | Must be separately qualified through driver/device reset; if unsupported, surface that restriction and require stop/start. It must not release the lease during an in-place reboot. |

The current pause implementation in `pkg/abstractions/vm/pause.go` pairs a
terminal memory checkpoint with durable disk generations. At runtime,
`Checkpoint` freezes disks, `snapshotMicroVM` pauses CH, snapshots RAM/device
state and eventually ends or restores the VMM. Add the GPU rejection before
any of those steps. Existing `rewriteSnapshotConfig` only rewrites network,
block, filesystem and vsock resources; it is not a GPU relocation mechanism.

Upstream v53.0 does support VFIO snapshot/restore for devices exposing the
kernel migration-v2 protocol through a supporting variant driver; generic
`vfio-pci` is explicitly non-migratable. This qualification does not establish
support for the proposed physical GPU. The v53 snapshot documentation uses a
Mellanox NIC example, not a generic graphics guarantee.
[CH v53.0 snapshot requirements](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v53.0/docs/snapshot_restore.md#vfio-devices)

Newer v54.0 documents live migration for migratable VFIO devices with DMA dirty
tracking, destination device/iommufd FDs and additional restrictions. Upgrading
CH alone would not make an ordinary passthrough GPU migratable, nor add that
flow to this repository. Future GPU RAM restore/migration needs an explicitly
supported vendor migration driver, measured device-state capture/restore,
equivalent destination hardware, compatible drivers/firmware and CH versions,
disk/RAM consistency, network identity/session handling and failure recovery.
Treat it as a separate project and profile capability.
[CH v54.0 VFIO live-migration requirements](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v54.0/docs/vfio.md#live-migration-of-vfio-devices)

## Network and latency gate

Keep protected signaling, generation/token fencing and private media routing
in `pkg/abstractions/vm/webrtc.go` and `kiosk-webrtc.js`. No public GPU/media
device port is needed. The current WebRTC path uses ICE-TCP on NLB 443 rather
than an assumed direct UDP path. TCP loss can stall ordered media delivery;
more encoder throughput cannot repair that behavior. A future UDP/TURN path
would require explicit networking, authentication and abuse/capacity design.

Measure input arrival → application response → capture → conversion → encode
→ gateway transport → browser decode → presentation. Use both headed-browser
pixel counters and high-speed-camera input-to-photon measurement. Record
actual client ↔ gateway and gateway ↔ worker location/RTT, CPU/GPU NUMA
placement, cross-zone/region routing, packet loss, retransmissions, bandwidth,
queue depth and media RTT. Do not subtract round-trip estimates from optical
results to manufacture a server-only improvement.

Reuse `benchmarks/kiosk/README.md` and its public-path qualification gate:
at least 55 observed FPS, p95 frame interval ≤25 ms, at least 30 input-response
samples, p95 input response ≤80 ms and no unanswered keys on a path with
≤20 ms RTT. Test the real Retina physical capture size as well as 1080p and
the documented DPR-2 comparison. A local tunnel run is diagnostic only.
The observed approximately 53 ms media-RTT staging path is not evidence that
the low-RTT gate is met. Qualify an appropriate physical placement or revise
the declared target before claiming an interactive latency pass.

Repeat at 60 ms RTT, constrained bandwidth and packet loss; require recovery
to current frames without replaying an expanding backlog. Compare CPU render
plus CPU encode, GPU render plus CPU encode, CPU render plus GPU encode, and
GPU render plus GPU encode sequentially at matched CPU/RAM, viewport, image,
client, application and network conditions. This separates the value of each
stage. Match visual quality/bitrate constraints and preserve CPU benchmark
results; GPU acceleration must not be credited for unrelated placement or
configuration changes.

## Implementation milestones and validation

| Milestone | Deliverable and completion evidence |
| --- | --- |
| 0. Evidence/profile selection | Read-only host inventory and vendor support review; choose one exact graphics-and-encode GPU/host/guest tuple. Record reset domain, licensing/session limits, firmware, driver and network location. No public profile until it passes admission. |
| 1. Isolated device bring-up | Dedicated test host; minimal CH v53 VM with one VFIO GPU and matching graphics kernel. Prove PCI enumeration, real GL/EGL rendering, hardware H.264 encoding, guest reboot behavior and repeated reset after clean/forced termination. No production scheduler exposure. |
| 2. Host ownership/runtime | Atomic domain leases, trusted CH arguments, rollback/reconciliation/quarantine, disjoint app/VM GPU accounting and per-instance checkpoint rejection. Fault-inject each startup/cleanup boundary and concurrent allocation race. |
| 3. Contract and image | Add typed profile request to API/SDK/CLI/MCP; preserve unknown-field rejection, request idempotency, image compatibility and CPU behavior. Validate incompatible templates, unsupported counts, missing capacity, idle pause and direct memory-checkpoint requests. |
| 4. Kiosk qualification | GPU-backed X11, selected capture/encoder telemetry, application/MCP readiness, resize/HiDPI/clipboard/dialog/reconnect/revocation tests. Run GTK, Qt, Electron, drawing, 3D and media family matrix through protected public URLs. Prove saved-project snapshot/fork independence and cold-start semantics. |
| 5. Limited release | One-workspace private pool, bounded concurrency, operational reset/quarantine alerts and capacity visibility. Pass complete optical/visual/network gate with stored versions/results, then evaluate cross-workspace sanitization and wider host supply separately. |

Meaningful tests belong at the existing guard/ownership boundaries:
`configuration_test.go`/`service_test.go`/`pause_test.go`, stub validation tests,
scheduler runtime/capacity tests, agent preflight/slot tests, worker lifecycle
tests and runtime hypervisor-argument/cleanup tests. Add real Linux/VFIO
integration coverage in the style of `microvm_integration_test.go`; mocks do
not establish IOMMU isolation, GPU reset, vendor driver initialization or
hardware encoding. Verify repeated failed boots and worker replacement never
return an unsafe device to free capacity. Keep CPU-only lifecycle and kiosk
tests as regression coverage.

Minimum bring-up capacity is one qualified Linux KVM host, one exclusively
assignable GPU/domain, suitable guest CPU/RAM/local memory and persistent disk
space. One device suffices for sequential rendering/encoding comparisons. A
concurrent source-and-fork test requires two qualified free GPU allocations;
host-loss/cold-relocation testing requires two qualified hosts and destination
headroom. For N active v1 sessions, budget N exclusive GPUs/domains, plus any
explicitly planned spare capacity, CPU/RAM and encoder-session headroom.
Stopped VMs consume durable storage and release GPUs after cleanup. Queue or
reject beyond qualified capacity; do not infer capacity from provider catalogs
or promise overcommit. Capacity acquisition and spending need a separate
operational decision; this document provisions nothing.

## Code touchpoints

These are implementation targets, not changes made by this document:

| Area | Exact repository touchpoints |
| --- | --- |
| Public VM state/validation | `pkg/types/vm.go` (`VMSpec`, `VMArtifact`); `pkg/abstractions/vm/configuration.go` (`validate`, `validateKiosk`); `api.go` (`decode`, create/inheritance/action handlers); `pkg/repository/vm.go` (JSON persistence). Keep allocation leases separate from durable VM/artifact specs. |
| Stub and request contract | `pkg/abstractions/vm/lifecycle.go` (`prepare`, `start`); `pkg/gateway/services/stub.go` (`validateUseVM`); `pkg/abstractions/pod/vm.go` (`RunVM`); `pkg/types/scheduler.go` (`ContainerRequest`, `Worker`); `pkg/gateway/gateway.proto` and generated clients. |
| Placement/accounting | `pkg/scheduler/scheduler.go` (`runtimeAcceptsRequest`, worker filtering/scoring); `pool_agent.go`, `pool_local.go`; `pkg/repository/worker_redis.go`, `worker_redis_evict.go`; `pkg/types/config.go` (`WorkerPoolConfig`). GPU VM leases must not become evictable app capacity. |
| Host agent/provisioning | `pkg/agent/preflight.go`, `capacity.go`, `protocol.go`, `worker_container.go`, `worker_config.go`, `state.go`; `pkg/gateway/services/compute/launch.go`, `byoc_aws.go`, `pkg/compute`. Extend registration/slot protobufs and inventory persistence as needed; device names/counts alone are inadequate. |
| Worker allocation/cleanup | `pkg/worker/lifecycle.go` (`spawn`, existing GPU assignment/CDI and final cleanup); `worker.go` (runtime/manager initialization); `nvidia.go`, `thunder.go` (preserve container paths); new host PCI assignment manager. |
| VMM planning/execution | `pkg/runtime/runtime.go` (`Capabilities`, per-request lifecycle eligibility); `microvm.go` (`microVMHypervisorArgs`, `microVMKernelCmdline`, snapshot config rewriting); `microvm_linux.go` (`NewMicroVM`, `Run`, `newInstance`, `boot`, `Delete`, teardown, `sweepLeftoverVMs`, checkpoint/restore). Revisit the current CPU-only `swiotlb=noforce` assumption for the qualified GPU DMA path. |
| Kernel/image/guest boot | `docker/Dockerfile.microvm`, `docker/microvm/guest-kernel.config`, `guest-kernel.fragment`; `hack/microvm-image-tag.sh`; `cmd/vminit/main.go`, `systemd.go`; `sdk/src/beta9/abstractions/vm/install`, `boot`, `beam-desktop.service`. Version and distribute kernel modules/firmware together. |
| Kiosk/display/diagnostics | `sdk/src/beta9/abstractions/vm/kiosk`, `kiosk_config.py`, `desktop_api.py`; `pkg/abstractions/vm/desktop.go`, `kiosk.html`, `kiosk-webrtc.js`, `webrtc.go`; `pkg/api/v1/mcp/vm_desktop.go`. Use status-driven backend health and preserve X11 helper compatibility. |
| SDK/CLI/MCP | `sdk/src/beta9/abstractions/vm/__init__.py`, `sdk/src/beta9/cli/vm.py`, `sdk/src/beta9/mcp/vm.py`; `pkg/api/v1/mcp/vm_catalog.go`, `vm.go`, `contract.go`; `sdk/bin/generate_vm_assets.py` for rebuilt assets. Keep CPU VM descriptions accurate while advertising optional qualified profiles. |
| State preservation | `pkg/abstractions/vm/pause.go`, `lifecycle.go`, `api.go`; `pkg/worker/criu.go`, `durable_disk_snapshot.go`, `durable_disk_lifecycle.go`; `pkg/runtime/microvm_linux.go` checkpoint/restore. Gate all RAM capture paths, preserve disk finalization and never copy a lease into a fork. |
| Qualification | `benchmarks/kiosk/README.md`, `probe.py`, `probe.js`, `run.py`; existing VM/SDK/worker/runtime test suites. Extend measured telemetry and fault cases rather than weakening release gates. |

## Questions to resolve before implementation or purchase

1. Which exact GPU/firmware/driver tuple supports both required graphics APIs
   and headless H.264 encoding, at the real physical capture resolution, under
   this CH direct-kernel boot? What vendor support and encode/capture licensing
   applies? No candidate is approved by this design.
2. Which host offering supplies usable KVM, IOMMU ownership and reset control
   for that GPU? Can its reset domain be dedicated without affecting host
   networking/storage or another VM? What is the measured user/gateway/worker
   geography and RTT?
3. Does GPU-backed Xorg provide stable headless resize/DPI and the desired
   capture backend with the pinned Selkies package, or is a separately pinned
   patched-Xvfb profile needed? What copies remain, and what is their measured
   latency at Retina resolution?
4. What vendor-defined sanitization establishes safe cross-workspace reuse,
   and which failure conditions require host power cycling? Who owns that
   recovery and how much spare qualified capacity is required?
5. What guest image manifest distribution/retention policy keeps custom kernel,
   modules, firmware and userspace compatible across stop/start and long-lived
   disk artifacts? Which upgrades require explicit profile replacement?
6. What is the accepted latency target on the actual user-facing network? If
   ≤20 ms RTT placement is unavailable, establish a separate honest WAN target
   before release rather than interpreting a GPU as a substitute for locality.
7. Is RAM-preserving GPU pause a necessary future product requirement? If so,
   identify a vendor/CH-supported migration-capable device path and fund its
   independent qualification; ordinary passthrough and disk snapshots do not
   provide it.
