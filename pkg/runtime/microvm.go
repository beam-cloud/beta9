package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// The microvm runtime boots each container as a Cloud Hypervisor virtual
// machine. The container rootfs (already an overlay merged directory holding
// config.json) is shared read-only over virtio-fs; the guest init layers a
// writable block device over it, applies the spec's mounts, and runs the
// container process. Network is the container's own namespace: a tap in that
// namespace is L2-redirected to the veth so the guest owns the veth's
// addresses and MAC and every host-side rule keyed on them keeps working.
//
// This file holds the portable planning logic (what to mount, which disks,
// how many vCPUs, the hypervisor command line); microvm_linux.go drives the
// processes, netlink, and mounts.

const (
	DefaultMicroVMKernelPath = "/usr/local/share/beam/microvm/vmlinux"
	DefaultMicroVMInitPath   = "/usr/local/bin/beam-vminit"
	DefaultMicroVMStateRoot  = "/run/beam/microvm"

	// Annotations the worker (or a test harness) sets on the OCI spec.
	MicroVMDockerAnnotation     = "com.beam.microvm.docker"      // "true" binds the disk's docker dir over /var/lib/docker
	MicroVMRootDiskAnnotation   = "com.beam.microvm.disk.root"   // vhost-user-blk socket serving the writable root disk
	MicroVMDiskAnnotationPrefix = "com.beam.microvm.disk."       // .<n> = "<socket>:<mount path>[:ro]"
	MicroVMMemoryMiBAnnotation  = "com.beam.microvm.memory-mib"  // guest RAM when the spec carries no memory limit
	MicroVMVCPUAnnotation       = "com.beam.microvm.vcpus"       // vCPU count when the spec carries no CPU quota
	MicroVMScratchGiBAnnotation = "com.beam.microvm.scratch-gib" // sparse scratch disk size for ephemeral sandboxes

	microVMSysfsCgroupRoot       = "/sys/fs/cgroup"
	microVMTapName               = "b9tap0"
	microVMBootTimeout           = 90 * time.Second
	microVMPowerOffTimeout       = 15 * time.Second
	microVMControlRequestTimeout = 30 * time.Second
	microVMDefaultMemoryMiB      = 512
	microVMDefaultScratchGiB     = 32
	// microVMMemoryHeadroom is charged to the VM's cgroup on top of guest RAM
	// for the VMM, virtiofsd, and their queues. Guest RAM itself is fixed by
	// --memory, so this does not let the workload exceed its request.
	microVMMemoryHeadroom = 256 << 20
	microVMMemoryAlign    = 2 << 20
	microVMConsoleTail    = 64
)

// DiskFreezer is implemented by runtimes that own the container's writable
// block devices. The worker freezes the guest filesystem through it before a
// durable-disk snapshot pivots the backing chain. mountPath is the guest
// mount of the disk; empty means the root disk. A guest that is not running
// has nothing to flush, so freezing it succeeds with a no-op thaw.
type DiskFreezer interface {
	FreezeDisk(ctx context.Context, containerID, mountPath string) (thaw func(), err error)
}

// GuestFilesystem is implemented by runtimes whose writable layer the host
// cannot read directly (a VM's block device). The worker's sandbox file RPCs
// go through it instead of the host overlay. Reads stream the file bytes into
// sink; writes stream req.Length bytes from payload. Everything else is
// answered in the response header.
type GuestFilesystem interface {
	GuestFS(ctx context.Context, containerID string, req microvm.FSRequest, payload io.Reader, sink io.Writer) (*microvm.FSResponse, error)
	// ExportGuestTree replays the guest directory guestPath into hostDir as
	// an overlay-style tree: whiteout devices, opaque xattrs, ownership and
	// modes intact, so the image and checkpoint code can read it exactly
	// like a host overlay upper directory. exclude lists paths relative to
	// guestPath to leave out.
	ExportGuestTree(ctx context.Context, containerID, guestPath, hostDir string, exclude []string) error
}

// GuestUpperDir is where a guest keeps the writable layer of its root
// filesystem, the counterpart of a host overlay's upper directory.
var GuestUpperDir = path.Join(microvm.DiskMount, microvm.DiskOverlayUpper)

type microVMDisk struct {
	arg       string
	device    string
	mountPath string
	readOnly  bool
	// Exactly one of path (a raw image the runtime owns) or socket (a
	// vhost-user-blk export) is set; a restore rewrites these into the
	// snapshot's device config.
	path   string
	socket string
}

// Checkpoint layout under CheckpointOpts.ImagePath.
const (
	checkpointVMDir    = "vm"       // Cloud Hypervisor snapshot: config.json, state.json, memory-ranges
	checkpointRootDisk = "root.img" // the scratch root disk, when the VM has one
)

// --- bundle -----------------------------------------------------------------

func readBundleSpec(bundlePath string) (*specs.Spec, error) {
	data, err := os.ReadFile(filepath.Join(bundlePath, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("read bundle config: %w", err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("decode bundle config: %w", err)
	}
	if spec.Root == nil || spec.Linux == nil || spec.Process == nil {
		return nil, fmt.Errorf("bundle config is missing root, linux, or process")
	}
	return &spec, nil
}

func networkNamespacePath(spec *specs.Spec) string {
	if spec == nil || spec.Linux == nil {
		return ""
	}
	for _, ns := range spec.Linux.Namespaces {
		if ns.Type == specs.NetworkNamespace {
			return ns.Path
		}
	}
	return ""
}

// --- resources ---------------------------------------------------------------

func microVMVCPUs(spec *specs.Spec) int {
	if n, ok := annotationInt(spec, MicroVMVCPUAnnotation); ok && n > 0 {
		return int(n)
	}
	if spec.Linux != nil && spec.Linux.Resources != nil && spec.Linux.Resources.CPU != nil {
		cpu := spec.Linux.Resources.CPU
		if cpu.Quota != nil && *cpu.Quota > 0 && cpu.Period != nil && *cpu.Period > 0 {
			return int((*cpu.Quota + int64(*cpu.Period) - 1) / int64(*cpu.Period))
		}
		if n := cpusetSize(cpu.Cpus); n > 0 {
			return n
		}
	}
	return 1
}

func microVMMemoryBytes(spec *specs.Spec) int64 {
	var size int64
	if mib, ok := annotationInt(spec, MicroVMMemoryMiBAnnotation); ok && mib > 0 {
		size = mib << 20
	} else if spec.Linux != nil && spec.Linux.Resources != nil && spec.Linux.Resources.Memory != nil && spec.Linux.Resources.Memory.Limit != nil && *spec.Linux.Resources.Memory.Limit > 0 {
		size = *spec.Linux.Resources.Memory.Limit
	} else {
		size = microVMDefaultMemoryMiB << 20
	}
	return (size + microVMMemoryAlign - 1) / microVMMemoryAlign * microVMMemoryAlign
}

func annotationInt(spec *specs.Spec, key string) (int64, bool) {
	if spec == nil || spec.Annotations == nil {
		return 0, false
	}
	value, ok := spec.Annotations[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func annotationBool(spec *specs.Spec, key string) bool {
	if spec == nil || spec.Annotations == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(spec.Annotations[key]), "true")
}

// cpusetSize counts the CPUs in a cpuset list such as "0-3,8".
func cpusetSize(cpus string) int {
	count := 0
	for _, part := range strings.Split(strings.TrimSpace(cpus), ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		start, err := strconv.Atoi(strings.TrimSpace(lo))
		if err != nil {
			return 0
		}
		end := start
		if isRange {
			if end, err = strconv.Atoi(strings.TrimSpace(hi)); err != nil || end < start {
				return 0
			}
		}
		count += end - start + 1
	}
	return count
}

func microVMCgroupPath(spec *specs.Spec, containerID string) string {
	name := containerID
	if spec != nil && spec.Linux != nil && strings.TrimSpace(spec.Linux.CgroupsPath) != "" {
		name = strings.TrimPrefix(spec.Linux.CgroupsPath, "/")
	}
	return filepath.Join(microVMSysfsCgroupRoot, name)
}

func microVMCPUMax(spec *specs.Spec) string {
	if spec == nil || spec.Linux == nil || spec.Linux.Resources == nil || spec.Linux.Resources.CPU == nil {
		return ""
	}
	cpu := spec.Linux.Resources.CPU
	if cpu.Quota == nil || *cpu.Quota <= 0 {
		return ""
	}
	period := uint64(100000)
	if cpu.Period != nil && *cpu.Period > 0 {
		period = *cpu.Period
	}
	return fmt.Sprintf("%d %d", *cpu.Quota, period)
}

// --- disks -------------------------------------------------------------------

// microVMDiskPlan decides the VM's block devices from the spec: the root disk
// is either a vhost-user-blk export the worker attached (durable qcow) or a
// scratch image the runtime creates at scratchPath; extra disks are further
// exports with a guest mount path. Devices are named in --disk order.
func microVMDiskPlan(spec *specs.Spec, scratchPath string) (root microVMDisk, extra []microVMDisk, err error) {
	if socket := strings.TrimSpace(spec.Annotations[MicroVMRootDiskAnnotation]); socket != "" {
		root = microVMDisk{arg: vhostUserDiskArg(socket), device: "/dev/vda", socket: socket}
	} else {
		// An explicit image type: Cloud Hypervisor blocks sector 0 writes on
		// auto-detected raw images, which breaks the ext4 superblock update.
		root = microVMDisk{arg: "path=" + scratchPath + ",image_type=raw", device: "/dev/vda", path: scratchPath}
	}

	keys := make([]string, 0)
	for key := range spec.Annotations {
		if strings.HasPrefix(key, MicroVMDiskAnnotationPrefix) && key != MicroVMRootDiskAnnotation {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for i, key := range keys {
		socket, mountPath, ok := strings.Cut(spec.Annotations[key], ":")
		if !ok || strings.TrimSpace(socket) == "" || strings.TrimSpace(mountPath) == "" {
			return root, nil, fmt.Errorf("annotation %s must be <socket>:<mount path>[:ro]", key)
		}
		readOnly := false
		if path, flag, hasFlag := strings.Cut(mountPath, ":"); hasFlag {
			mountPath = path
			readOnly = flag == "ro"
		}
		if !filepath.IsAbs(mountPath) {
			return root, nil, fmt.Errorf("annotation %s mount path %q must be absolute", key, mountPath)
		}
		socket = strings.TrimSpace(socket)
		extra = append(extra, microVMDisk{
			arg:       vhostUserDiskArg(socket),
			device:    fmt.Sprintf("/dev/vd%c", 'b'+i),
			mountPath: filepath.Clean(mountPath),
			readOnly:  readOnly,
			socket:    socket,
		})
	}
	return root, extra, nil
}

func vhostUserDiskArg(socket string) string {
	return "vhost_user=on,socket=" + socket + ",num_queues=1,queue_size=128"
}

// --- canvas ------------------------------------------------------------------

// microVMMountPlan splits the spec's mounts into what the host binds into the
// canvas (the guest re-binds them over the overlay) and the tmpfs mounts the
// guest creates itself. Pseudo filesystems and anything under /dev, /proc,
// /sys are the guest's own business.
func microVMMountPlan(mounts []specs.Mount) (binds []specs.Mount, tmpfs []specs.Mount) {
	for _, mount := range mounts {
		dest := filepath.Clean(mount.Destination)
		if !filepath.IsAbs(dest) || dest == "/" {
			continue
		}
		if underAny(dest, "/dev", "/proc", "/sys") {
			continue
		}
		switch mount.Type {
		case "proc", "sysfs", "devpts", "mqueue", "cgroup", "cgroup2":
			continue
		case "tmpfs":
			tmpfs = append(tmpfs, mount)
			continue
		}
		if mount.Type == "bind" || mount.Type == "none" || mount.Type == "" || hasOption(mount.Options, "bind") || hasOption(mount.Options, "rbind") {
			if strings.TrimSpace(mount.Source) == "" || mount.Source == "none" {
				continue
			}
			binds = append(binds, mount)
		}
	}
	return binds, tmpfs
}

func underAny(path string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func hasOption(options []string, name string) bool {
	for _, option := range options {
		if option == name {
			return true
		}
	}
	return false
}

// microVMGuestSpec is the vm.json the guest init reads: everything the host
// decided that the guest has to act on.
func microVMGuestSpec(spec *specs.Spec, network microvm.Network, root microVMDisk, extra []microVMDisk, binds []microvm.Bind, tmpfs []specs.Mount) *microvm.Spec {
	vmSpec := &microvm.Spec{
		Hostname:    spec.Hostname,
		Network:     network,
		RootDisk:    root.device,
		Docker:      annotationBool(spec, MicroVMDockerAnnotation),
		Binds:       binds,
		ControlPort: microvm.ControlPort,
	}
	for _, disk := range extra {
		vmSpec.Disks = append(vmSpec.Disks, microvm.Disk{Device: disk.device, MountPath: disk.mountPath, ReadOnly: disk.readOnly})
	}
	for _, mount := range tmpfs {
		vmSpec.Tmpfs = append(vmSpec.Tmpfs, microvm.Tmpfs{Destination: filepath.Clean(mount.Destination), Options: append([]string(nil), mount.Options...)})
	}
	return vmSpec
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	_ = os.Remove(dst)
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func waitForPath(ctx context.Context, path string, timeout time.Duration, gaveUp func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if gaveUp != nil && gaveUp() {
			return fmt.Errorf("process exited before creating %s", path)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not appear within %s", path, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// --- hypervisor ------------------------------------------------------------------

func microVMKernelCmdline() string {
	return strings.Join([]string{
		"console=ttyS0",
		"root=" + microvm.VirtiofsTag,
		"rootfstype=virtiofs",
		// rw: the share carries the spec's writable bind mounts (volumes,
		// uploads). The image itself is only ever read as an overlay lower.
		"rw",
		"init=" + microvm.InitPath,
		"reboot=k",
		"panic=0",
		"loglevel=4",
		"random.trust_cpu=on",
		"i8042.noaux",
		"i8042.nomux",
		"i8042.nopnp",
		"i8042.nokbd",
	}, " ")
}

func microVMHypervisorArgs(stateDir, kernel, cmdline string, vcpus int, memory int64, mac string, root microVMDisk, extra []microVMDisk) []string {
	args := []string{
		"--api-socket", "path=" + filepath.Join(stateDir, "api.sock"),
		"--kernel", kernel,
		"--cmdline", cmdline,
		"--cpus", fmt.Sprintf("boot=%d", vcpus),
		"--memory", fmt.Sprintf("size=%d,shared=on", memory),
		"--fs", fmt.Sprintf("tag=%s,socket=%s,num_queues=1,queue_size=1024", microvm.VirtiofsTag, filepath.Join(stateDir, "virtiofs.sock")),
		"--disk", root.arg,
	}
	for _, disk := range extra {
		args = append(args, disk.arg)
	}
	args = append(args,
		"--net", fmt.Sprintf("tap=%s,mac=%s", microVMTapName, mac),
		"--vsock", fmt.Sprintf("cid=%d,socket=%s", microvm.GuestCID, filepath.Join(stateDir, "vsock.sock")),
		"--rng", "src=/dev/urandom",
		"--serial", "tty",
		"--console", "off",
	)
	return args
}

// microVMRestoreArgs launches Cloud Hypervisor from a snapshot directory. The
// device configuration comes from the snapshot's config.json, so nothing
// else from microVMHypervisorArgs is passed.
func microVMRestoreArgs(stateDir, snapshotDir string) []string {
	return []string{
		"--api-socket", "path=" + filepath.Join(stateDir, "api.sock"),
		"--restore", "source_url=file://" + snapshotDir + ",resume=true",
	}
}

// rewriteSnapshotConfig points a snapshot's device config at this VM's
// sockets and disk image. Cloud Hypervisor documents config.json as editable
// between snapshot and restore for exactly this. Devices are matched by
// position: our --disk order is root first, then the extra disks in
// annotation order, and there is one fs and one vsock device.
func rewriteSnapshotConfig(config []byte, stateDir string, root microVMDisk, extra []microVMDisk) ([]byte, error) {
	var cfg map[string]any
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("decode snapshot config: %w", err)
	}
	disks, _ := cfg["disks"].([]any)
	want := append([]microVMDisk{root}, extra...)
	if len(disks) != len(want) {
		return nil, fmt.Errorf("snapshot has %d disks, this VM has %d", len(disks), len(want))
	}
	for i, entry := range disks {
		disk, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("snapshot disk %d is not an object", i)
		}
		switch {
		case want[i].socket != "":
			if _, vhost := disk["vhost_socket"]; !vhost {
				return nil, fmt.Errorf("snapshot disk %d is not a vhost-user export", i)
			}
			disk["vhost_socket"] = want[i].socket
		case want[i].path != "":
			if _, raw := disk["path"]; !raw {
				return nil, fmt.Errorf("snapshot disk %d is not an image-backed disk", i)
			}
			disk["path"] = want[i].path
		}
	}
	if fs, _ := cfg["fs"].([]any); len(fs) == 1 {
		if entry, ok := fs[0].(map[string]any); ok {
			entry["socket"] = filepath.Join(stateDir, "virtiofs.sock")
		}
	}
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		vsock["socket"] = filepath.Join(stateDir, "vsock.sock")
	}
	return json.Marshal(cfg)
}

// --- console -----------------------------------------------------------------------

// lineWriter reassembles the serial console into whole lines before handing
// them to the worker's output writer, which treats each Write as one log
// record; the guest writes the console a few bytes at a time. A partial line
// is flushed once it exceeds maxLine or on Close.
type lineWriter struct {
	mu  sync.Mutex
	dst io.Writer
	buf []byte
}

const lineWriterMaxLine = 16 << 10

func newLineWriter(dst io.Writer) *lineWriter {
	return &lineWriter{dst: dst}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		if _, err := w.dst.Write(w.buf[:i+1]); err != nil {
			return 0, err
		}
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) >= lineWriterMaxLine {
		if _, err := w.dst.Write(w.buf); err != nil {
			return 0, err
		}
		w.buf = w.buf[:0]
	}
	return len(p), nil
}

// Close flushes any trailing partial line.
func (w *lineWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.dst.Write(w.buf)
	w.buf = nil
	return err
}

// tailWriter keeps the last n lines written to it for error reporting.
type tailWriter struct {
	mu    sync.Mutex
	limit int
	lines []string
	cur   bytes.Buffer
}

func newTailWriter(limit int) *tailWriter {
	return &tailWriter{limit: limit}
}

func (t *tailWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, b := range p {
		if b == '\n' {
			t.push(t.cur.String())
			t.cur.Reset()
			continue
		}
		t.cur.WriteByte(b)
	}
	return len(p), nil
}

func (t *tailWriter) push(line string) {
	t.lines = append(t.lines, line)
	if len(t.lines) > t.limit {
		t.lines = t.lines[len(t.lines)-t.limit:]
	}
}

func (t *tailWriter) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	lines := append([]string(nil), t.lines...)
	if t.cur.Len() > 0 {
		lines = append(lines, t.cur.String())
	}
	return strings.Join(lines, "\n")
}
