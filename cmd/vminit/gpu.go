//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// gpuCheckpointer takes a passed-through NVIDIA GPU out of use so the host
// can unplug it for a snapshot, and puts the processes back on a GPU after.
// NVIDIA's cuda-checkpoint does the work in each CUDA process: it copies the
// device's memory into the process and closes the device, then the reverse.
// What was suspended is remembered here, in guest memory, so it is still
// known in a VM restored from that snapshot.
//
// Between suspends it keeps each GPU open, as nvidia-persistenced does. The
// driver initialises a GPU for its first client and shuts it down after its
// last, so a process that starts on an idle GPU waits for that, and one that
// starts while it is open does not.
type gpuCheckpointer struct {
	mu   sync.Mutex
	proc string // procfs
	tool string // cuda-checkpoint

	run    func(ctx context.Context, name string, args ...string) (string, error)
	rescan func() error
	// devices returns a test for whether a /proc/<pid>/fd entry is an
	// NVIDIA device, valid for one scan.
	devices   func() func(fd string) bool
	available func() int64 // bytes of guest memory a process can still take
	// open opens each GPU the driver has: none until it has bound one.
	open func() ([]io.Closer, error)

	suspended []int       // processes whose CUDA state is in their own memory
	uuid      string      // the GPU that state came from
	held      []io.Closer // the GPUs, open
	suspends  int         // a GPU found after a later suspend is not held
}

const (
	gpuLockTimeout = 30 * time.Second
	gpuProbeEvery  = 100 * time.Millisecond
	// Copying device memory into a process takes that much guest memory.
	gpuMemoryMargin = 256 << 20
	// udev loads the driver for a GPU that was plugged in within a second.
	// An image that leaves that to the first process to use it holds nothing.
	gpuDriverWait = 30 * time.Second
)

func newGPUCheckpointer() *gpuCheckpointer {
	return &gpuCheckpointer{
		proc:      "/proc",
		tool:      microvm.GPUCheckpointPath,
		run:       runGPUTool,
		rescan:    func() error { return os.WriteFile("/sys/bus/pci/rescan", []byte("1"), 0) },
		devices:   func() func(string) bool { return nvidiaDeviceMatcher("/proc/devices") },
		available: availableWorkloadMemory,
		open:      func() ([]io.Closer, error) { return openNVIDIADevices("/proc", "/dev") },
	}
}

// The guest agent may run without a PATH; these are where a driver installs.
var gpuToolDirs = []string{"/usr/bin", "/usr/local/bin", "/usr/local/nvidia/bin", "/bin"}

func runGPUTool(ctx context.Context, name string, args ...string) (string, error) {
	path := name
	if !strings.Contains(name, "/") {
		path = ""
		for _, dir := range gpuToolDirs {
			if info, err := os.Stat(filepath.Join(dir, name)); err == nil && info.Mode().IsRegular() {
				path = filepath.Join(dir, name)
				break
			}
		}
		if path == "" {
			return "", fmt.Errorf("%s: %w", name, exec.ErrNotFound)
		}
	}
	out, err := helperOutput(exec.CommandContext(ctx, path, args...))
	text := strings.TrimSpace(string(out))
	if err != nil {
		if len(text) > 400 {
			text = text[:400]
		}
		return text, fmt.Errorf("%s: %w: %s", name, err, text)
	}
	return text, nil
}

// nvidiaDeviceMatcher recognises the driver's character devices by major
// number, whatever a container calls them.
func nvidiaDeviceMatcher(devices string) func(string) bool {
	majors := nvidiaMajors(devices)
	return func(fd string) bool {
		var stat unix.Stat_t
		if len(majors) == 0 || unix.Stat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFCHR {
			return false
		}
		return slices.Contains(majors, unix.Major(uint64(stat.Rdev)))
	}
}

// nvidiaMajors reads the character device numbers the driver registered
// from /proc/devices.
func nvidiaMajors(devices string) []uint32 {
	data, err := os.ReadFile(devices)
	if err != nil {
		return nil
	}
	var majors []uint32
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "Block") {
			break
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[1], "nvidia") {
			continue
		}
		if major, err := strconv.ParseUint(fields[0], 10, 32); err == nil && !slices.Contains(majors, uint32(major)) {
			majors = append(majors, uint32(major))
		}
	}
	return majors
}

// openNVIDIADevices opens each GPU the driver has bound. It finds them by
// device number: /dev/nvidia0 is made by the first program to use the GPU,
// and may not be there.
func openNVIDIADevices(proc, dev string) ([]io.Closer, error) {
	gpus, err := filepath.Glob(filepath.Join(proc, "driver/nvidia/gpus/*/information"))
	if err != nil || len(gpus) == 0 {
		return nil, err
	}
	major, err := nvidiaMajor(filepath.Join(proc, "devices"))
	if err != nil {
		return nil, err
	}
	var files []io.Closer
	for _, gpu := range gpus {
		minor, err := nvidiaMinor(gpu)
		if err == nil {
			var file *os.File
			if file, err = openCharDevice(dev, major, minor); err == nil {
				files = append(files, file)
				continue
			}
		}
		for _, file := range files {
			_ = file.Close()
		}
		return nil, fmt.Errorf("open %s: %w", filepath.Base(filepath.Dir(gpu)), err)
	}
	return files, nil
}

// nvidiaMajor is the character device number of the driver's GPUs.
func nvidiaMajor(devices string) (uint32, error) {
	data, err := os.ReadFile(devices)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "Block") {
			break
		}
		// Older drivers register as nvidia-frontend.
		if fields := strings.Fields(line); len(fields) == 2 && (fields[1] == "nvidia" || fields[1] == "nvidia-frontend") {
			major, err := strconv.ParseUint(fields[0], 10, 32)
			return uint32(major), err
		}
	}
	return 0, errors.New("the NVIDIA driver has registered no device number")
}

// nvidiaMinor reads a GPU's device number from the driver's description of
// it, /proc/driver/nvidia/gpus/<address>/information.
func nvidiaMinor(information string) (uint32, error) {
	data, err := os.ReadFile(information)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if name, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(name) == "Device Minor" {
			minor, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
			return uint32(minor), err
		}
	}
	return 0, errors.New("the driver does not say which device it is")
}

// openCharDevice opens a character device that may have no name, through
// one in dir that lasts as long as the open takes.
func openCharDevice(dir string, major, minor uint32) (*os.File, error) {
	node := filepath.Join(dir, fmt.Sprintf(".beam-vm-%d-%d-%d", major, minor, os.Getpid()))
	_ = os.Remove(node)
	if err := unix.Mknod(node, unix.S_IFCHR|0o600, int(unix.Mkdev(major, minor))); err != nil {
		return nil, err
	}
	defer os.Remove(node)
	return os.OpenFile(node, os.O_RDWR, 0)
}

func availableWorkloadMemory() int64 {
	free := int64(-1)
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "MemAvailable:" {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					free = kb << 10
				}
			}
		}
	}
	limit, limitErr := readCgroupBytes(filepath.Join(microvm.WorkloadCgroup, "memory.max"))
	used, usedErr := readCgroupBytes(filepath.Join(microvm.WorkloadCgroup, "memory.current"))
	if limitErr == nil && usedErr == nil && (free < 0 || limit-used < free) {
		free = max(limit-used, 0)
	}
	return free
}

func readCgroupBytes(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// holders lists the processes with an NVIDIA device open. The driver does
// not let go of a GPU until every one of them has closed it.
func (g *gpuCheckpointer) holders() ([]int, error) {
	entries, err := os.ReadDir(g.proc)
	if err != nil {
		return nil, err
	}
	var pids []int
	isDevice := g.devices()
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		dir := filepath.Join(g.proc, entry.Name(), "fd")
		fds, err := os.ReadDir(dir)
		if err != nil {
			continue // exited, or a kernel thread
		}
		for _, fd := range fds {
			if isDevice(filepath.Join(dir, fd.Name())) {
				pids = append(pids, pid)
				break
			}
		}
	}
	slices.Sort(pids)
	return pids, nil
}

func (g *gpuCheckpointer) describe(pids []int) string {
	names := make([]string, len(pids))
	for i, pid := range pids {
		comm, _ := os.ReadFile(filepath.Join(g.proc, strconv.Itoa(pid), "comm"))
		names[i] = fmt.Sprintf("%s (pid %d)", firstNonEmpty(strings.TrimSpace(string(comm)), "process"), pid)
	}
	return strings.Join(names, ", ")
}

func (g *gpuCheckpointer) cuda(ctx context.Context, pid int, args ...string) (string, error) {
	return g.run(ctx, g.tool, append(args, "--pid", strconv.Itoa(pid))...)
}

// gpuUUID names the GPU the way cuda-checkpoint's device map does. It is
// empty when the image has no nvidia-smi; restoring then needs the same GPU.
func (g *gpuCheckpointer) gpuUUID(ctx context.Context) (string, error) {
	out, err := g.run(ctx, "nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader")
	if err != nil {
		return "", err
	}
	uuid, _, _ := strings.Cut(out, "\n")
	if uuid = strings.TrimSpace(uuid); !strings.HasPrefix(uuid, "GPU-") {
		return "", fmt.Errorf("nvidia-smi reported no GPU: %s", out)
	}
	return uuid, nil
}

// checkMemory refuses a suspend that would run the guest out of memory: the
// kernel would kill the very process being preserved.
func (g *gpuCheckpointer) checkMemory(ctx context.Context, pids []int) error {
	out, err := g.run(ctx, "nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits")
	if err != nil {
		logf("gpu suspend: cannot size GPU memory: %v", err)
		return nil
	}
	var need int64
	for _, line := range strings.Split(out, "\n") {
		pid, used, ok := strings.Cut(line, ",")
		if !ok {
			continue
		}
		id, idErr := strconv.Atoi(strings.TrimSpace(pid))
		mib, usedErr := strconv.ParseInt(strings.TrimSpace(used), 10, 64)
		if idErr == nil && usedErr == nil && slices.Contains(pids, id) {
			need += mib << 20
		}
	}
	free := g.available()
	if need == 0 || free < 0 || need+gpuMemoryMargin <= free {
		return nil
	}
	return fmt.Errorf("saving GPU memory needs %d MiB of free guest memory and %d MiB is free", (need+gpuMemoryMargin)>>20, free>>20)
}

// hold opens the GPUs the driver has, and reports whether it has any. The
// caller holds g.mu.
func (g *gpuCheckpointer) hold() (bool, error) {
	if len(g.held) > 0 {
		return true, nil
	}
	held, err := g.open()
	g.held = held
	return len(held) > 0, err
}

// release closes the GPUs: the driver does not give up a device that is
// open. The caller holds g.mu.
func (g *gpuCheckpointer) release() {
	for _, gpu := range g.held {
		_ = gpu.Close()
	}
	g.held = nil
}

// keep holds the GPUs once the driver has them, which for one that was just
// plugged in is after udev has loaded it. A suspend ends the wait: the GPU is
// about to leave.
func (g *gpuCheckpointer) keep(wait time.Duration) {
	g.mu.Lock()
	suspends := g.suspends
	g.mu.Unlock()
	for deadline := time.Now().Add(wait); ; time.Sleep(gpuProbeEvery) {
		g.mu.Lock()
		if g.suspends != suspends {
			g.mu.Unlock()
			return
		}
		held, err := g.hold()
		g.mu.Unlock()
		if held {
			return
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				logf("gpu: not kept initialised: %v", err)
			}
			return
		}
	}
}

// suspend leaves no process with the GPU open, or changes nothing.
func (g *gpuCheckpointer) suspend(ctx context.Context) (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.suspends++
	g.release()
	defer func() {
		if err != nil {
			go g.keep(0)
		}
	}()
	pids, err := g.holders()
	if err != nil {
		return err
	}
	if len(pids) == 0 {
		return nil
	}
	if _, err := os.Stat(g.tool); err != nil {
		return fmt.Errorf("the GPU is in use by %s and cuda-checkpoint is not installed on this host", g.describe(pids))
	}
	if err := g.checkMemory(ctx, pids); err != nil {
		return err
	}
	uuid, err := g.gpuUUID(ctx)
	if err != nil {
		logf("gpu suspend: %v; the state can only return to this GPU", err)
	}

	steps := microvm.StartSteps()
	var done []int
	undo := func(cause error) error {
		// The caller has stopped waiting by now if ctx is why this failed.
		restore, cancel := context.WithTimeout(context.WithoutCancel(ctx), microvm.GPUResumeLimit)
		defer cancel()
		// A process that is not given its state back here is still without
		// it: the resume that follows a refusal asks again, and fails too
		// rather than leave it so.
		left, err := g.restore(restore, done, "")
		g.remember(left, uuid)
		return errors.Join(cause, err)
	}
	for _, pid := range pids {
		name := g.describe([]int{pid})
		state, err := g.cuda(ctx, pid, "--get-state")
		if err != nil {
			return undo(fmt.Errorf("%s has the GPU open and cannot be suspended: %w", name, err))
		}
		switch state {
		case "checkpointed":
			continue // someone else's to restore
		case "running":
		default:
			return undo(fmt.Errorf("%s is in CUDA checkpoint state %q", name, state))
		}
		if _, err := g.cuda(ctx, pid, "--action", "lock", "--timeout", strconv.FormatInt(gpuLockTimeout.Milliseconds(), 10)); err != nil {
			return undo(fmt.Errorf("lock %s: %w", name, err))
		}
		if _, err := g.cuda(ctx, pid, "--action", "checkpoint"); err != nil {
			_, unlockErr := g.cuda(context.WithoutCancel(ctx), pid, "--action", "unlock")
			return undo(errors.Join(fmt.Errorf("save GPU state of %s: %w", name, err), unlockErr))
		}
		done = append(done, pid)
	}
	left, err := g.holders()
	if err != nil {
		return undo(err)
	}
	if len(left) > 0 {
		return undo(fmt.Errorf("the GPU is still open in %s", g.describe(left)))
	}
	g.remember(done, uuid)
	steps.Took("save")
	logf("gpu suspend: %d processes: %s", len(done), steps)
	return nil
}

// remember adds processes whose CUDA state is in their own memory. Those of
// a suspend the host never resumed keep their place, and their GPU.
func (g *gpuCheckpointer) remember(pids []int, uuid string) {
	if len(pids) == 0 {
		return
	}
	g.suspended = append(g.suspended, pids...)
	if g.uuid == "" {
		g.uuid = uuid
	}
}

// resume finds the GPU the host attached and returns each suspended process
// to it. The GPU may be a different one of the same model. The processes it
// could not return are asked for again by the next.
func (g *gpuCheckpointer) resume(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	steps := microvm.StartSteps()
	if err := g.rescan(); err != nil {
		return fmt.Errorf("rescan PCI bus: %w", err)
	}
	steps.Took("rescan")
	pids, uuid := g.suspended, g.uuid
	g.suspended, g.uuid = nil, ""
	if len(pids) == 0 {
		// Nothing waits for the GPU yet, so neither does the host.
		go g.keep(gpuDriverWait)
		return nil
	}
	// Once here rather than in nvidia-smi and again in each process.
	if held, err := g.hold(); !held {
		logf("gpu resume: not initialised ahead of its processes: %v", err)
		go g.keep(gpuDriverWait)
	}
	steps.Took("initialise")
	left, err := g.restore(ctx, pids, uuid)
	g.remember(left, uuid)
	steps.Took("restore")
	logf("gpu resume: %d processes: %s", len(pids), steps)
	return err
}

// restore returns to the GPU the CUDA state each process saved from the GPU
// named from, and lists the processes that are still without theirs.
func (g *gpuCheckpointer) restore(ctx context.Context, pids []int, from string) ([]int, error) {
	if len(pids) == 0 {
		return nil, nil
	}
	// The driver binds the new device during the rescan, but is not ready
	// for clients until it has initialised the GPU.
	var to string
	for {
		uuid, err := g.gpuUUID(ctx)
		if err == nil || errors.Is(err, exec.ErrNotFound) {
			to = uuid
			break
		}
		select {
		case <-ctx.Done():
			return pids, fmt.Errorf("no GPU to restore CUDA state to: %w", err)
		case <-time.After(gpuProbeEvery):
		}
	}
	var deviceMap []string
	if from != "" && to != "" && from != to {
		deviceMap = []string{"--device-map", from + "=" + to}
	}
	var left []int
	var errs []error
	for _, pid := range pids {
		if _, err := os.Stat(filepath.Join(g.proc, strconv.Itoa(pid))); err != nil {
			continue // exited while suspended
		}
		if err := g.restoreProcess(ctx, pid, deviceMap); err != nil {
			left = append(left, pid)
			errs = append(errs, err)
		}
	}
	return left, errors.Join(errs...)
}

// restoreProcess gives a process its CUDA state back and lets it run. Asked
// again after it failed, it goes on from where the process was left.
func (g *gpuCheckpointer) restoreProcess(ctx context.Context, pid int, deviceMap []string) error {
	name := g.describe([]int{pid})
	if _, err := g.cuda(ctx, pid, append([]string{"--action", "restore"}, deviceMap...)...); err != nil {
		switch state, _ := g.cuda(ctx, pid, "--get-state"); state {
		case "running":
			return nil
		case "locked":
		default:
			return fmt.Errorf("restore GPU state of %s: %w", name, err)
		}
	}
	if _, err := g.cuda(ctx, pid, "--action", "unlock"); err != nil {
		return fmt.Errorf("unlock %s: %w", name, err)
	}
	return nil
}

const pciDevicesPath = "/sys/bus/pci/devices"

// waitUnplugged returns once none of the PCI devices is on the guest's bus.
// The kernel ejects a device right after removing it, so the hypervisor has
// let go of it too by the time the host hears of it.
func waitUnplugged(ctx context.Context, root string, devices []string) error {
	for _, device := range devices {
		if device == "" || filepath.Base(device) != device {
			return fmt.Errorf("invalid PCI address %q", device)
		}
	}
	for {
		var left []string
		for _, device := range devices {
			if _, err := os.Lstat(filepath.Join(root, device)); err == nil {
				left = append(left, device)
			}
		}
		if len(left) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s still attached: a driver has not released it", strings.Join(left, ", "))
		case <-time.After(2 * time.Millisecond):
		}
	}
}
