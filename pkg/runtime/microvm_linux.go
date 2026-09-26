//go:build linux

package runtime

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netlink/nl"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// MicroVM implements Runtime with Cloud Hypervisor. See microvm.go.
type MicroVM struct {
	cfg Config
	mu  sync.Mutex
	vms map[string]*microVMInstance
}

var (
	_ DiskFreezer         = (*MicroVM)(nil)
	_ NetworkSlotPreparer = (*MicroVM)(nil)
)

type microVMInstance struct {
	id        string
	stateDir  string
	canvas    string
	scratch   string
	cgroup    string
	netnsPath string
	tapIndex  int
	submounts []string

	virtiofsd      *exec.Cmd
	hypervisor     *exec.Cmd
	ctrl           *microVMControl
	console        *tailWriter
	prepareTimings string // per-step durations of prepare, for the start log line

	mu     sync.Mutex
	exited bool
	paused bool // vCPUs stopped by Checkpoint; nothing in the guest can run
	// killed is set when the host SIGKILLs the hypervisor (a forced stop, a
	// signal the guest would not take, a terminal checkpoint); Run reports
	// that as the container's exit, not a VM failure.
	killed bool
	waited bool // Wait reaped the hypervisor; its pid must not be signalled again
}

// NewMicroVM validates that the hypervisor, virtiofsd, guest kernel, guest
// init, and KVM are all present. Unlike gVisor the worker does not fall back
// to runc when this fails: a pool declared as microvm must be one.
func NewMicroVM(cfg Config) (Runtime, error) {
	if cfg.MicroVMHypervisorPath == "" {
		cfg.MicroVMHypervisorPath = "cloud-hypervisor"
	}
	if cfg.MicroVMVirtiofsdPath == "" {
		cfg.MicroVMVirtiofsdPath = "virtiofsd"
	}
	if cfg.MicroVMKernelPath == "" {
		cfg.MicroVMKernelPath = DefaultMicroVMKernelPath
	}
	if cfg.MicroVMInitPath == "" {
		cfg.MicroVMInitPath = DefaultMicroVMInitPath
	}
	if cfg.MicroVMStateRoot == "" {
		cfg.MicroVMStateRoot = DefaultMicroVMStateRoot
	}

	unavailable := func(reason string) error {
		return ErrRuntimeNotAvailable{Runtime: types.ContainerRuntimeMicroVM.String(), Reason: reason}
	}
	for _, bin := range []string{cfg.MicroVMHypervisorPath, cfg.MicroVMVirtiofsdPath, "mkfs.ext4"} {
		if _, err := exec.LookPath(bin); err != nil {
			return nil, unavailable(fmt.Sprintf("%s not found in PATH", bin))
		}
	}
	for _, path := range []string{cfg.MicroVMKernelPath, cfg.MicroVMInitPath} {
		if _, err := os.Stat(path); err != nil {
			return nil, unavailable(fmt.Sprintf("%s: %v", path, err))
		}
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		return nil, unavailable("/dev/kvm is not available")
	}
	if err := os.MkdirAll(cfg.MicroVMStateRoot, 0o755); err != nil {
		return nil, unavailable(fmt.Sprintf("create state root: %v", err))
	}
	sweepLeftoverVMs(cfg.MicroVMStateRoot)
	return &MicroVM{cfg: cfg, vms: map[string]*microVMInstance{}}, nil
}

// sweepLeftoverVMs kills the hypervisors and virtiofsd daemons a previous
// worker left behind and removes their state; VMs are not adopted.
func sweepLeftoverVMs(stateRoot string) {
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		return
	}
	procs, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(stateRoot, entry.Name())
		for _, proc := range procs {
			pid, err := strconv.Atoi(proc.Name())
			if err != nil {
				continue
			}
			cmdline, err := os.ReadFile(filepath.Join("/proc", proc.Name(), "cmdline"))
			if err != nil || !bytes.Contains(cmdline, []byte(dir+"/")) {
				continue
			}
			log.Warn().Str("container_id", entry.Name()).Int("pid", pid).Msg("killing microvm process left by a previous worker")
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		_ = os.RemoveAll(dir)
	}
}

func (m *MicroVM) Name() string {
	return types.ContainerRuntimeMicroVM.String()
}

func (m *MicroVM) Capabilities() Capabilities {
	return Capabilities{JoinExistingNetNS: true, BlockRoot: true, CheckpointRestore: true}
}

// Prepare strips what only a shared-kernel runtime needs. The VM is the
// isolation boundary; seccomp and device cgroup rules do not apply to it.
func (m *MicroVM) Prepare(ctx context.Context, spec *specs.Spec) error {
	if spec == nil || spec.Linux == nil {
		return fmt.Errorf("spec is nil")
	}
	spec.Linux.Seccomp = nil
	spec.Linux.Devices = nil
	return nil
}

func (m *MicroVM) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	if opts == nil {
		opts = &RunOpts{}
	}
	inst, spec, err := m.newInstance(containerID, bundlePath)
	if err != nil {
		return -1, err
	}
	// The processes must be gone when Run returns; Delete, which the worker
	// calls after every Run, releases the rest.
	defer inst.stopProcesses()

	network, root, extra, err := m.prepare(ctx, inst, spec, "")
	if err != nil {
		return -1, err
	}
	args := microVMHypervisorArgs(inst.stateDir, m.cfg.MicroVMKernelPath, microVMKernelCmdline(), microVMVCPUs(spec), microVMMemoryBytes(spec), network.MAC, root, extra)
	return m.boot(ctx, inst, spec, args, opts.OutputWriter, opts.ErrorWriter, opts.Started)
}

// Restore boots a VM from a Checkpoint and, like runsc's, returns once the
// restored guest has reconnected and taken this container's network; the VM
// is then supervised in the background and State reports its exit.
func (m *MicroVM) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	if opts == nil || opts.ImagePath == "" {
		return -1, fmt.Errorf("restore requires a checkpoint path")
	}
	inst, spec, err := m.newInstance(containerID, opts.BundlePath)
	if err != nil {
		return -1, err
	}
	network, root, extra, err := m.prepare(ctx, inst, spec, opts.ImagePath)
	if err != nil {
		inst.stopProcesses()
		return -1, err
	}
	inst.ctrl.network = &network
	snapshotDir, err := m.stageSnapshot(inst, opts.ImagePath, network, root, extra)
	if err != nil {
		inst.stopProcesses()
		return -1, fmt.Errorf("stage snapshot: %w", err)
	}

	booted := make(chan error, 1)
	go func() {
		defer inst.stopProcesses()
		code, err := m.boot(ctx, inst, spec, microVMRestoreArgs(inst.stateDir, snapshotDir), opts.OutputWriter, nil, nil)
		log.Info().Str("container_id", containerID).Int("exit_code", code).Err(err).Msg("restored microvm exited")
		booted <- err
	}()
	select {
	case err := <-inst.ctrl.networkApplied:
		if err != nil {
			inst.killHypervisor()
			return -1, fmt.Errorf("restored guest did not take its network: %w", err)
		}
	case err := <-booted:
		if err == nil {
			err = errors.New("microvm exited before the restored guest reported in")
		}
		return -1, err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
	if opts.Started != nil {
		select {
		case opts.Started <- inst.pid():
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	return 0, nil
}

// Checkpoint pauses the VM and writes its memory, device state and scratch
// root disk under ImagePath. Snapshotting stops virtiofsd's queues for good,
// so LeaveRunning restores in place instead of resuming; any failure past
// the pause ends the VM rather than leaving a guest hung.
func (m *MicroVM) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
	if opts == nil || opts.ImagePath == "" {
		return fmt.Errorf("checkpoint requires an image path")
	}
	inst, ok := m.instance(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}
	if !inst.alive() {
		return fmt.Errorf("microvm %s is not running", containerID)
	}
	snapshotDir := filepath.Join(opts.ImagePath, checkpointVMDir)
	if err := os.RemoveAll(snapshotDir); err != nil {
		return err
	}
	if err := os.MkdirAll(snapshotDir, 0o700); err != nil {
		return err
	}
	api := inst.api()
	if err := api.put(ctx, "vm.pause", nil); err != nil {
		return fmt.Errorf("pause vm: %w", err)
	}
	inst.setPaused(true)
	err := api.put(ctx, "vm.snapshot", map[string]string{"destination_url": "file://" + snapshotDir})
	if err == nil && inst.scratch != "" {
		if err = copySparse(inst.scratch, filepath.Join(opts.ImagePath, checkpointRootDisk)); err != nil {
			err = fmt.Errorf("copy root disk: %w", err)
		}
	}
	var hookErr error
	if err == nil && opts.WhilePaused != nil {
		hookErr = opts.WhilePaused(ctx)
	}
	if err == nil && opts.LeaveRunning {
		err = m.restoreInPlace(ctx, inst, snapshotDir)
	}
	if err != nil || !opts.LeaveRunning {
		inst.killHypervisor()
	}
	return errors.Join(err, hookErr)
}

// restoreInPlace restores snapshotDir inside the same hypervisor process
// (same pid, sockets, tap and cgroup), so the worker sees only a pause and
// the guest keeps its addresses.
func (m *MicroVM) restoreInPlace(ctx context.Context, inst *microVMInstance, snapshotDir string) error {
	api := inst.api()
	if err := api.put(ctx, "vm.delete", nil); err != nil {
		return err
	}
	// Deleting the VM leaves the hypervisor's vsock listener behind and
	// takes virtiofsd down with its connection; the restore needs both fresh.
	if err := os.Remove(filepath.Join(inst.stateDir, microVMVsockSocket)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := m.startVirtiofsd(ctx, inst); err != nil {
		return fmt.Errorf("start virtiofsd: %w", err)
	}
	if err := api.put(ctx, "vm.restore", map[string]any{"source_url": "file://" + snapshotDir, "resume": true}); err != nil {
		return err
	}
	inst.setPaused(false)
	return nil
}

// newInstance reads the bundle and registers the VM under its container id.
func (m *MicroVM) newInstance(containerID, bundlePath string) (*microVMInstance, *specs.Spec, error) {
	spec, err := readBundleSpec(bundlePath)
	if err != nil {
		return nil, nil, err
	}
	canvas := spec.Root.Path
	if canvas == "" {
		return nil, nil, fmt.Errorf("spec has no root path")
	}
	if !filepath.IsAbs(canvas) {
		canvas = filepath.Join(bundlePath, canvas)
	}
	inst := &microVMInstance{
		id:       containerID,
		stateDir: filepath.Join(m.cfg.MicroVMStateRoot, containerID),
		canvas:   canvas,
		console:  newTailWriter(microVMConsoleTail),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.vms[containerID]; exists {
		return nil, nil, fmt.Errorf("microvm %s already exists", containerID)
	}
	m.vms[containerID] = inst
	return inst, spec, nil
}

// prepare builds everything on the host side of the VM: state dir, network,
// disks, canvas, cgroup, virtiofsd and the control listener. restoreFrom is
// a checkpoint path whose root disk replaces a fresh scratch image.
func (m *MicroVM) prepare(ctx context.Context, inst *microVMInstance, spec *specs.Spec, restoreFrom string) (microvm.Network, microVMDisk, []microVMDisk, error) {
	var none microvm.Network
	if err := os.RemoveAll(inst.stateDir); err != nil {
		return none, microVMDisk{}, nil, err
	}
	if err := os.MkdirAll(inst.stateDir, 0o700); err != nil {
		return none, microVMDisk{}, nil, err
	}
	var steps strings.Builder
	step := time.Now()
	took := func(name string) {
		fmt.Fprintf(&steps, "%s=%dms ", name, time.Since(step).Milliseconds())
		step = time.Now()
	}
	network, err := m.setupNetwork(inst, spec)
	if err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("setup microvm network: %w", err)
	}
	took("network")
	root, extra, err := m.prepareDisks(ctx, inst, spec, restoreFrom)
	if err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("prepare microvm disks: %w", err)
	}
	took("disks")
	if err := m.prepareCanvas(inst, spec, network, root, extra); err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("prepare microvm rootfs: %w", err)
	}
	took("canvas")
	if err := m.setupCgroup(inst, spec, microVMMemoryBytes(spec)); err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("setup microvm cgroup: %w", err)
	}
	took("cgroup")
	if err := m.startVirtiofsd(ctx, inst); err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("start virtiofsd: %w", err)
	}
	took("virtiofsd")
	ctrl, err := listenMicroVMControl(filepath.Join(inst.stateDir, microVMVsockSocket))
	if err != nil {
		return none, microVMDisk{}, nil, fmt.Errorf("listen on vsock control socket: %w", err)
	}
	inst.ctrl = ctrl
	inst.prepareTimings = strings.TrimSpace(steps.String())
	return network, root, extra, nil
}

// stageSnapshot lays out the restore source: the checkpoint's memory and
// state files (linked, not copied) next to a config.json rewritten for this
// VM's sockets, disks and MAC.
func (m *MicroVM) stageSnapshot(inst *microVMInstance, imagePath string, network microvm.Network, root microVMDisk, extra []microVMDisk) (string, error) {
	src := filepath.Join(imagePath, checkpointVMDir)
	dst := filepath.Join(inst.stateDir, "restore")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return "", err
	}
	config, err := os.ReadFile(filepath.Join(src, "config.json"))
	if err != nil {
		return "", err
	}
	config, err = rewriteSnapshotConfig(config, inst.stateDir, network, root, extra)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dst, "config.json"), config, 0o600); err != nil {
		return "", err
	}
	for _, name := range []string{"state.json", "memory-ranges"} {
		if err := os.Symlink(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
			return "", err
		}
	}
	return dst, nil
}

// boot launches Cloud Hypervisor with args and supervises it until the guest
// reports the container's exit or the VM dies.
func (m *MicroVM) boot(ctx context.Context, inst *microVMInstance, spec *specs.Spec, args []string, outputWriter, errorWriter io.Writer, started chan<- int) (int, error) {
	ctrl := inst.ctrl
	cmd := exec.Command(m.cfg.MicroVMHypervisorPath, args...)
	cgroupFD, err := inst.cgroupAttr(cmd)
	if err != nil {
		return -1, err
	}
	defer cgroupFD.Close()
	var out io.Writer = inst.console
	var lines []*lineWriter
	if outputWriter != nil {
		lw := newLineWriter(outputWriter)
		lines = append(lines, lw)
		out = io.MultiWriter(lw, inst.console)
	}
	cmd.Stdout = out
	if errorWriter != nil {
		lw := newLineWriter(errorWriter)
		lines = append(lines, lw)
		cmd.Stderr = io.MultiWriter(lw, inst.console)
	} else {
		cmd.Stderr = out
	}
	defer func() {
		for _, lw := range lines {
			_ = lw.Close()
		}
	}()

	// Cloud Hypervisor opens the tap by name, so it must start in the container's netns.
	spawnStart := time.Now()
	if err := inNetworkNamespace(inst.netnsPath, cmd.Start); err != nil {
		return -1, fmt.Errorf("start cloud-hypervisor: %w", err)
	}
	inst.mu.Lock()
	inst.hypervisor = cmd
	inst.mu.Unlock()
	log.Info().Str("container_id", inst.id).Int("pid", cmd.Process.Pid).Int("vcpus", microVMVCPUs(spec)).Int64("memory_bytes", microVMMemoryBytes(spec)).
		Str("prepare", inst.prepareTimings).Dur("spawn", time.Since(spawnStart)).Msg("microvm started")

	if started != nil {
		select {
		case started <- cmd.Process.Pid:
		case <-ctx.Done():
		}
	}

	waitDone := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		inst.mu.Lock()
		inst.waited = true
		inst.mu.Unlock()
		waitDone <- err
	}()

	bootTimer := time.NewTimer(microVMBootTimeout)
	defer bootTimer.Stop()
	var powerOff <-chan time.Time

	exitCode, exitReported := -1, false
	for {
		select {
		case <-ctx.Done():
			inst.killHypervisor()
			<-waitDone
			return -1, ctx.Err()
		case <-bootTimer.C:
			if !ctrl.connected() {
				inst.killHypervisor()
				<-waitDone
				return -1, fmt.Errorf("guest did not report in within %s: %s", microVMBootTimeout, inst.console.String())
			}
		case code := <-ctrl.exit:
			exitCode, exitReported = code, true
			// The guest powers off right after reporting; give the VMM a
			// moment to notice before forcing it.
			timer := time.NewTimer(microVMPowerOffTimeout)
			defer timer.Stop()
			powerOff = timer.C
		case <-powerOff:
			inst.killHypervisor()
		case err := <-waitDone:
			if !exitReported {
				select {
				case code := <-ctrl.exit:
					exitCode, exitReported = code, true
				default:
				}
			}
			if exitReported {
				return exitCode, nil
			}
			inst.mu.Lock()
			killed := inst.killed
			inst.mu.Unlock()
			if killed {
				// Reported the way runc reports a SIGKILLed init.
				return 128 + int(syscall.SIGKILL), nil
			}
			return -1, fmt.Errorf("microvm exited before the container process reported: %w: %s", err, inst.console.String())
		}
	}
}

func (m *MicroVM) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	return fmt.Errorf("microvm runtime does not support exec; sandboxes use the in-guest process manager")
}

// UpdateResources applies a CPU limit to a running VM's cgroup, so the worker
// can defer the quota until the guest is up. Guest RAM is fixed by --memory.
func (m *MicroVM) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	inst, ok := m.instance(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}
	if inst.cgroup == "" || resources == nil {
		return nil
	}
	cpuMax := cpuMaxString(resources.CPU)
	if cpuMax == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(inst.cgroup, "cpu.max"), []byte(cpuMax), 0o644); err != nil {
		return fmt.Errorf("set cpu.max: %w", err)
	}
	return nil
}

func (m *MicroVM) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	inst, ok := m.instance(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}
	if !inst.alive() {
		return nil
	}
	if sig == syscall.SIGKILL || !inst.ctrl.connected() {
		inst.killHypervisor()
		return nil
	}
	if _, err := inst.ctrl.request(ctx, microvm.Message{Type: microvm.MsgSignal, Signal: int(sig)}); err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("guest did not accept signal; killing hypervisor")
		inst.killHypervisor()
	}
	return nil
}

func (m *MicroVM) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	m.mu.Lock()
	inst, ok := m.vms[containerID]
	delete(m.vms, containerID)
	m.mu.Unlock()
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}
	return inst.teardown()
}

func (m *MicroVM) State(ctx context.Context, containerID string) (State, error) {
	inst, ok := m.instance(containerID)
	if !ok {
		return State{}, ErrContainerNotFound{ContainerID: containerID}
	}
	state := State{ID: containerID, Status: "stopped", Pid: inst.pid()}
	if inst.alive() {
		state.Status = "running"
	}
	return state, nil
}

func (m *MicroVM) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

func (m *MicroVM) Close() error {
	m.mu.Lock()
	instances := make([]*microVMInstance, 0, len(m.vms))
	for id, inst := range m.vms {
		instances = append(instances, inst)
		delete(m.vms, id)
	}
	m.mu.Unlock()
	var errs []error
	for _, inst := range instances {
		errs = append(errs, inst.teardown())
	}
	return errors.Join(errs...)
}

// FreezeDisk FIFREEZEs the guest filesystem at mountPath. A VM that is gone,
// not yet up, or paused mid-checkpoint has no I/O in flight (a paused one
// cannot even answer), so it gets a no-op thaw.
func (m *MicroVM) FreezeDisk(ctx context.Context, containerID, mountPath string) (func(), error) {
	noop := func() {}
	inst, ok := m.instance(containerID)
	if !ok || !inst.alive() || !inst.ctrl.connected() || inst.isPaused() {
		return noop, nil
	}
	thaw := func() error {
		_, err := inst.ctrl.request(context.Background(), microvm.Message{Type: microvm.MsgThaw, Text: mountPath})
		return err
	}
	if _, err := inst.ctrl.request(ctx, microvm.Message{Type: microvm.MsgFreeze, Text: mountPath}); err != nil {
		if !inst.alive() {
			return noop, nil
		}
		// The freeze may still land after the host gave up on it.
		_ = thaw()
		return nil, fmt.Errorf("freeze guest filesystem: %w", err)
	}
	return func() {
		if err := thaw(); err != nil && inst.alive() {
			log.Error().Err(err).Str("container_id", containerID).Msg("failed to thaw guest filesystem")
		}
	}, nil
}

func (m *MicroVM) instance(containerID string) (*microVMInstance, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.vms[containerID]
	return inst, ok
}

var _ GuestFilesystem = (*MicroVM)(nil)

// GuestFS runs one filesystem operation inside the guest over a dedicated
// vsock connection: JSON header line out, raw payload for writes, JSON reply
// line back, raw bytes for reads. See microvm.FSPort.
func (m *MicroVM) GuestFS(ctx context.Context, containerID string, req microvm.FSRequest, payload io.Reader, sink io.Writer) (*microvm.FSResponse, error) {
	inst, ok := m.instance(containerID)
	if !ok {
		return nil, ErrContainerNotFound{ContainerID: containerID}
	}
	if !inst.alive() {
		return nil, fmt.Errorf("microvm %s is not running", containerID)
	}
	conn, err := dialGuestVsock(ctx, filepath.Join(inst.stateDir, microVMVsockSocket), microvm.FSPort)
	if err != nil {
		return nil, fmt.Errorf("connect to guest filesystem: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
	}

	header, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(header, '\n')); err != nil {
		return nil, fmt.Errorf("send fs request: %w", err)
	}
	if req.Op == microvm.FSOpWrite {
		if payload == nil {
			payload = bytes.NewReader(nil)
		}
		if _, err := io.CopyN(conn, payload, req.Length); err != nil {
			return nil, fmt.Errorf("send fs payload: %w", err)
		}
	}

	reader := bufio.NewReaderSize(conn, microvm.MaxLineBytes)
	line, err := microvm.ReadLine(reader)
	if err != nil {
		return nil, fmt.Errorf("read fs reply: %w", err)
	}
	var reply microvm.FSResponse
	if err := json.Unmarshal(line, &reply); err != nil {
		return nil, fmt.Errorf("decode fs reply: %w", err)
	}
	if !reply.OK {
		return &reply, errors.New(reply.Error)
	}
	if sink == nil {
		sink = io.Discard
	}
	// The guest chooses the reply length; hold it to what was asked for.
	switch {
	case reply.Length == microvm.FSStreamUntilEOF:
		if req.Op != microvm.FSOpArchive {
			return nil, fmt.Errorf("fs reply to %s streams without a length", req.Op)
		}
		if _, err := io.Copy(sink, reader); err != nil {
			return nil, fmt.Errorf("read fs stream: %w", err)
		}
	case reply.Length < 0, req.Op == microvm.FSOpRead && req.Length > 0 && reply.Length > req.Length, req.Op == microvm.FSOpRead && reply.Length > microVMMaxGuestRead:
		return nil, fmt.Errorf("fs reply length %d is out of range for %s", reply.Length, req.Op)
	case req.Op == microvm.FSOpRead && reply.Length > 0:
		if _, err := io.CopyN(sink, reader, reply.Length); err != nil {
			return nil, fmt.Errorf("read fs payload: %w", err)
		}
	}
	return &reply, nil
}

// ExportGuestTree streams a PAX tar of guestPath out of the guest into
// hostDir; mknod, lchown and lsetxattr need the worker's privileges.
func (m *MicroVM) ExportGuestTree(ctx context.Context, containerID, guestPath, hostDir string, exclude []string) error {
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	extracted := make(chan error, 1)
	go func() {
		err := extractTree(pr, hostDir)
		// Drain so a guest still streaming is not blocked on a dead pipe.
		_, _ = io.Copy(io.Discard, pr)
		extracted <- err
	}()
	req := microvm.FSRequest{Op: microvm.FSOpArchive, Path: guestPath, Exclude: exclude}
	_, err := m.GuestFS(ctx, containerID, req, nil, pw)
	pw.CloseWithError(err)
	if xerr := <-extracted; err == nil && xerr != nil {
		return fmt.Errorf("materialize guest tree: %w", xerr)
	}
	if err != nil {
		return fmt.Errorf("export guest tree %s: %w", guestPath, err)
	}
	return nil
}

// extractTree replays the guest's tar under dst. The guest controls every
// name, type and link target, so nothing below dst is resolved through a
// symlink: each entry's parent is walked one component at a time with
// O_NOFOLLOW, and the entry is made relative to that directory's fd.
func extractTree(r io.Reader, dst string) error {
	root, err := unix.Open(dst, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", dst, err)
	}
	x := &treeExtractor{root: root, parentFD: -1}
	defer x.close()
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := x.extract(hdr, tr); err != nil {
			return fmt.Errorf("extract %s: %w", hdr.Name, err)
		}
	}
	// Creating children clobbers a directory's mtime, so directories go last.
	for i := len(x.dirs) - 1; i >= 0; i-- {
		dir := x.dirs[i]
		if parent, err := x.parent(dir.path, false); err == nil {
			ts := unix.NsecToTimespec(dir.modTime.UnixNano())
			_ = unix.UtimesNanoAt(parent, dir.path[len(dir.path)-1], []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
		}
	}
	return nil
}

type treeExtractor struct {
	root int
	// parentFD is the directory of the previous entry; the next one usually
	// shares it.
	parentPath string
	parentFD   int
	dirs       []treeDir
}

type treeDir struct {
	path    []string
	modTime time.Time
}

func (x *treeExtractor) close() {
	if x.parentFD >= 0 {
		unix.Close(x.parentFD)
	}
	unix.Close(x.root)
}

// treePath splits a tar name into its components below the root; "." and
// ".." cannot survive the clean.
func treePath(name string) []string {
	clean := strings.Trim(filepath.Clean("/"+name), "/")
	if clean == "" {
		return nil
	}
	return strings.Split(clean, "/")
}

// parent returns the fd of the directory holding the last of components,
// creating missing directories when create is set. x owns the fd.
func (x *treeExtractor) parent(components []string, create bool) (int, error) {
	dirPath := strings.Join(components[:len(components)-1], "/")
	if x.parentFD >= 0 && dirPath == x.parentPath {
		return x.parentFD, nil
	}
	fd, err := x.walk(components[:len(components)-1], create)
	if err != nil {
		return -1, err
	}
	if x.parentFD >= 0 {
		unix.Close(x.parentFD)
	}
	x.parentPath, x.parentFD = dirPath, fd
	return fd, nil
}

// walk opens the directory at components below the root; a symlink or
// non-directory anywhere on the way is an error.
func (x *treeExtractor) walk(components []string, create bool) (int, error) {
	fd, err := unix.Openat(x.root, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, name := range components {
		if create {
			if err := unix.Mkdirat(fd, name, 0o755); err != nil && err != unix.EEXIST {
				unix.Close(fd)
				return -1, fmt.Errorf("mkdir %s: %w", name, err)
			}
		}
		next, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return -1, fmt.Errorf("open directory %s: %w", name, err)
		}
		fd = next
	}
	return fd, nil
}

func (x *treeExtractor) extract(hdr *tar.Header, content io.Reader) error {
	components := treePath(hdr.Name)
	switch {
	case len(components) == 0:
		return nil
	case hdr.Typeflag == tar.TypeDir, hdr.Typeflag == tar.TypeReg, hdr.Typeflag == tar.TypeSymlink,
		hdr.Typeflag == tar.TypeLink, hdr.Typeflag == tar.TypeChar, hdr.Typeflag == tar.TypeBlock, hdr.Typeflag == tar.TypeFifo:
	default:
		return nil
	}
	dir, err := x.parent(components, true)
	if err != nil {
		return err
	}
	name := components[len(components)-1]
	mode := uint32(hdr.Mode) & 0o7777

	if hdr.Typeflag == tar.TypeDir {
		if err := unix.Mkdirat(dir, name, 0o700); err != nil && err != unix.EEXIST {
			return err
		}
		fd, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		x.dirs = append(x.dirs, treeDir{path: components, modTime: hdr.ModTime})
		return setTreeMetadata(fd, hdr, mode)
	}

	if err := unix.Unlinkat(dir, name, 0); err != nil && err != unix.ENOENT {
		return err
	}
	switch hdr.Typeflag {
	case tar.TypeReg:
		fd, err := unix.Openat(dir, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(fd), name)
		defer file.Close()
		if _, err := io.Copy(file, content); err != nil {
			return err
		}
		if err := setTreeMetadata(fd, hdr, mode); err != nil {
			return err
		}
	case tar.TypeSymlink:
		if err := unix.Symlinkat(hdr.Linkname, dir, name); err != nil {
			return err
		}
		if err := setTreeMetadataAt(dir, name, hdr, mode, false); err != nil {
			return err
		}
	case tar.TypeLink:
		source := treePath(hdr.Linkname)
		if len(source) == 0 {
			return fmt.Errorf("hard link to %q", hdr.Linkname)
		}
		sourceDir, err := x.walk(source[:len(source)-1], false)
		if err != nil {
			return err
		}
		defer unix.Close(sourceDir)
		// Flags 0: a symlink source is linked itself, never followed.
		return unix.Linkat(sourceDir, source[len(source)-1], dir, name, 0)
	default:
		kind := uint32(unix.S_IFIFO)
		switch hdr.Typeflag {
		case tar.TypeChar:
			kind = unix.S_IFCHR
		case tar.TypeBlock:
			kind = unix.S_IFBLK
		}
		if err := unix.Mknodat(dir, name, kind|mode, int(unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor)))); err != nil {
			return fmt.Errorf("mknod: %w", err)
		}
		if err := setTreeMetadataAt(dir, name, hdr, mode, true); err != nil {
			return err
		}
	}
	ts := unix.NsecToTimespec(hdr.ModTime.UnixNano())
	_ = unix.UtimesNanoAt(dir, name, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
	return nil
}

// setTreeMetadata applies an entry's ownership, mode and xattrs through its fd.
func setTreeMetadata(fd int, hdr *tar.Header, mode uint32) error {
	if err := unix.Fchown(fd, hdr.Uid, hdr.Gid); err != nil {
		return fmt.Errorf("chown: %w", err)
	}
	if err := unix.Fchmod(fd, mode); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}
	for name, value := range treeXattrs(hdr) {
		if err := unix.Fsetxattr(fd, name, value, 0); err != nil && !errors.Is(err, unix.ENOTSUP) {
			return fmt.Errorf("set xattr %s: %w", name, err)
		}
	}
	return nil
}

// setTreeMetadataAt is setTreeMetadata for entries that cannot be opened: a
// symlink, or a device node or FIFO the extractor just created in dir.
func setTreeMetadataAt(dir int, name string, hdr *tar.Header, mode uint32, chmod bool) error {
	if err := unix.Fchownat(dir, name, hdr.Uid, hdr.Gid, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("chown: %w", err)
	}
	if chmod {
		if err := unix.Fchmodat(dir, name, mode, 0); err != nil {
			return fmt.Errorf("chmod: %w", err)
		}
	}
	// lsetxattr does not follow the last component; the fd path pins the parent.
	path := fmt.Sprintf("/proc/self/fd/%d/%s", dir, name)
	for key, value := range treeXattrs(hdr) {
		if err := unix.Lsetxattr(path, key, value, 0); err != nil && !errors.Is(err, unix.ENOTSUP) {
			return fmt.Errorf("set xattr %s: %w", key, err)
		}
	}
	return nil
}

func treeXattrs(hdr *tar.Header) map[string][]byte {
	xattrs := map[string][]byte{}
	for key, value := range hdr.PAXRecords {
		if name, ok := strings.CutPrefix(key, "SCHILY.xattr."); ok && name != "security.selinux" {
			xattrs[name] = []byte(value)
		}
	}
	return xattrs
}

// dialGuestVsock opens a host-initiated vsock connection through Cloud
// Hypervisor's unix socket: the VMM expects "CONNECT <port>\n" and answers
// "OK <port>\n" once the guest accepted.
func dialGuestVsock(ctx context.Context, socketPath string, port uint32) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		conn.Close()
		return nil, err
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("vsock connect handshake: %w", err)
	}
	if !strings.HasPrefix(reply, "OK ") {
		conn.Close()
		return nil, fmt.Errorf("vsock connect to port %d refused: %s", port, strings.TrimSpace(reply))
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// --- disks -------------------------------------------------------------------

func (m *MicroVM) prepareDisks(ctx context.Context, inst *microVMInstance, spec *specs.Spec, restoreFrom string) (microVMDisk, []microVMDisk, error) {
	scratch := filepath.Join(filepath.Dir(inst.canvas), "scratch.ext4")
	root, extra, err := microVMDiskPlan(spec, scratch)
	if err != nil {
		return root, nil, err
	}
	if root.path == "" {
		return root, extra, nil
	}
	if restoreFrom != "" {
		// The memory image expects the filesystem exactly as it was at the
		// checkpoint; that is the copied disk, not a fresh one.
		err = copySparse(filepath.Join(restoreFrom, checkpointRootDisk), scratch)
	} else {
		sizeGiB := int64(microVMDefaultScratchGiB)
		if n, ok := annotationInt(spec, MicroVMScratchGiBAnnotation); ok && n > 0 {
			sizeGiB = n
		}
		err = createScratchDisk(ctx, scratch, sizeGiB<<30)
	}
	if err != nil {
		return root, nil, err
	}
	inst.scratch = scratch
	return root, extra, nil
}

// copySparse copies only the allocated extents of src, so a mostly empty
// multi-GiB scratch image costs as much as the data it holds.
func copySparse(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	if err := out.Truncate(info.Size()); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	for off := int64(0); off < info.Size(); {
		data, err := unix.Seek(int(in.Fd()), off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break // only holes remain
		}
		if err != nil {
			return err
		}
		hole, err := unix.Seek(int(in.Fd()), data, unix.SEEK_HOLE)
		if err != nil {
			return err
		}
		for pos := data; pos < hole; {
			n, err := in.ReadAt(buf[:min(int64(len(buf)), hole-pos)], pos)
			if err != nil && err != io.EOF {
				return err
			}
			if n == 0 {
				break
			}
			if _, err := out.WriteAt(buf[:n], pos); err != nil {
				return err
			}
			pos += int64(n)
		}
		off = hole
	}
	return out.Sync()
}

// createScratchDisk makes a sparse ext4 image. Lazy inode-table and journal
// init keep mkfs time independent of the virtual size.
func createScratchDisk(ctx context.Context, path string, size int64) error {
	_ = os.Remove(path)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	out, err := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-E", "lazy_itable_init=1,lazy_journal_init=1", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.ext4 %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// --- canvas ------------------------------------------------------------------

func (m *MicroVM) prepareCanvas(inst *microVMInstance, spec *specs.Spec, network microvm.Network, root microVMDisk, extra []microVMDisk) error {
	// The canvas is image content: a symlink at any of these paths would
	// redirect the host's writes and mounts below, so they are created
	// without following one, and .beam is recreated empty.
	for _, dir := range []string{"dev", "proc", "sys", "run", "tmp"} {
		if err := inst.canvasDir(dir); err != nil {
			return err
		}
	}
	beam := filepath.Join(inst.canvas, microvm.CanvasDir)
	if err := removeCanvasEntry(beam); err != nil {
		return err
	}
	for _, dir := range []string{microvm.CanvasDir, microvm.DiskMount, microvm.ImageMount, microvm.NewRoot, microvm.BindsDir} {
		if err := inst.canvasDir(dir); err != nil {
			return err
		}
	}
	if err := copyFile(m.cfg.MicroVMInitPath, filepath.Join(inst.canvas, microvm.InitPath), 0o755); err != nil {
		return fmt.Errorf("install guest init: %w", err)
	}

	plan := microVMMountPlan(spec.Mounts)
	mounts := make([]microvm.Mount, 0, len(plan))
	for i, mount := range plan {
		if mount.Type == "tmpfs" {
			mounts = append(mounts, tmpfsMount(mount))
			continue
		}
		bind, err := inst.bindIntoCanvas(i, mount)
		if err != nil {
			return err
		}
		mounts = append(mounts, bind)
	}
	data, err := json.MarshalIndent(microVMGuestSpec(spec, network, root, extra, mounts), "", "  ")
	if err != nil {
		return err
	}
	return writeFileNoFollow(filepath.Join(inst.canvas, microvm.SpecFile), data, 0o644)
}

// canvasDir makes rel a real directory directly under the canvas root (or
// under .beam), replacing a symlink or file the image put there.
func (inst *microVMInstance) canvasDir(rel string) error {
	path := filepath.Join(inst.canvas, rel)
	info, err := os.Lstat(path)
	switch {
	case err == nil && info.IsDir():
		return nil
	case err == nil:
		if err := removeCanvasEntry(path); err != nil {
			return err
		}
	case !os.IsNotExist(err):
		return err
	}
	return os.Mkdir(path, 0o755)
}

// removeCanvasEntry removes a symlink, file or directory at path without
// following a symlink.
func removeCanvasEntry(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		return os.RemoveAll(path)
	}
	return os.Remove(path)
}

func writeFileNoFollow(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_NOFOLLOW, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// bindIntoCanvas mounts one OCI bind mount at a numbered entry under
// microvm.BindsDir; the guest binds it to the destination.
func (inst *microVMInstance) bindIntoCanvas(index int, mount specs.Mount) (microvm.Mount, error) {
	dest := filepath.Clean(mount.Destination)
	if !filepath.IsAbs(dest) || dest == "/" {
		return microvm.Mount{}, fmt.Errorf("mount destination %q is not an absolute path inside the rootfs", mount.Destination)
	}
	info, err := os.Stat(mount.Source)
	if err != nil {
		return microvm.Mount{}, fmt.Errorf("bind source %s: %w", mount.Source, err)
	}
	isFile := !info.IsDir()
	guestSource := filepath.Join(microvm.BindsDir, strconv.Itoa(index))
	target := filepath.Join(inst.canvas, guestSource)
	if err := removeCanvasEntry(target); err != nil {
		return microvm.Mount{}, err
	}
	if isFile {
		if err := writeFileNoFollow(target, nil, 0o644); err != nil {
			return microvm.Mount{}, err
		}
	} else if err := os.Mkdir(target, 0o755); err != nil {
		return microvm.Mount{}, err
	}

	flags := uintptr(unix.MS_BIND)
	if slices.Contains(mount.Options, "rbind") {
		flags |= unix.MS_REC
	}
	if err := unix.Mount(mount.Source, target, "", flags, ""); err != nil {
		return microvm.Mount{}, fmt.Errorf("bind %s to %s: %w", mount.Source, target, err)
	}
	inst.submounts = append(inst.submounts, target)
	readOnly := slices.Contains(mount.Options, "ro")
	if readOnly {
		if err := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return microvm.Mount{}, fmt.Errorf("remount %s read-only: %w", target, err)
		}
	}
	return microvm.Mount{Type: microvm.MountBind, Source: guestSource, Destination: dest, File: isFile, ReadOnly: readOnly}, nil
}

// --- virtiofsd -----------------------------------------------------------------

// startVirtiofsd serves the canvas over vhost-user, replacing any earlier
// daemon of this VM (an in-place restore needs a fresh one).
func (m *MicroVM) startVirtiofsd(ctx context.Context, inst *microVMInstance) error {
	inst.mu.Lock()
	previous := inst.virtiofsd
	inst.mu.Unlock()
	if previous != nil && previous.Process != nil {
		_ = previous.Process.Kill()
		waitProcessGone(previous.Process, 5*time.Second)
	}
	socket := filepath.Join(inst.stateDir, microVMVirtiofsSocket)
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	// --killpriv-v2 lets the guest mark the share SB_NOSEC. Without it every
	// write costs a host open/getxattr/close, and on geesefs each such close
	// uploads a half-written file.
	cmd := exec.Command(m.cfg.MicroVMVirtiofsdPath,
		"--socket-path="+socket,
		"--shared-dir="+inst.canvas,
		"--announce-submounts",
		"--xattr",
		"--killpriv-v2",
		"--cache=auto",
		"--inode-file-handles=never",
		"--sandbox=chroot",
		"--log-level=warn",
	)
	cgroupFD, err := inst.cgroupAttr(cmd)
	if err != nil {
		return err
	}
	defer cgroupFD.Close()
	cmd.Stdout = inst.console
	cmd.Stderr = inst.console
	if err := cmd.Start(); err != nil {
		return err
	}
	inst.mu.Lock()
	inst.virtiofsd = cmd
	inst.mu.Unlock()
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	return waitForPath(ctx, socket, 10*time.Second, func() bool {
		select {
		case <-exited:
			return true
		default:
			return false
		}
	})
}

// --- hypervisor ------------------------------------------------------------------

// inNetworkNamespace runs fn on a thread switched into nsPath, so a process
// fn starts there inherits it. Only the network namespace changes; mounts,
// pids, and cgroups stay the worker's.
func inNetworkNamespace(nsPath string, fn func() error) error {
	goruntime.LockOSThread()
	defer goruntime.UnlockOSThread()

	hostNS, err := netns.Get()
	if err != nil {
		return err
	}
	defer hostNS.Close()
	targetNS, err := netns.GetFromPath(nsPath)
	if err != nil {
		return fmt.Errorf("open network namespace %s: %w", nsPath, err)
	}
	defer targetNS.Close()
	if err := netns.Set(targetNS); err != nil {
		return err
	}
	fnErr := fn()
	if err := netns.Set(hostNS); err != nil {
		// The thread is now unusable for the rest of the process; keep it
		// locked forever so the scheduler never reuses it.
		log.Error().Err(err).Msg("failed to return to the host network namespace; parking thread")
		goruntime.LockOSThread()
	}
	return fnErr
}

// --- cgroup ----------------------------------------------------------------------

func (m *MicroVM) setupCgroup(inst *microVMInstance, spec *specs.Spec, guestMemory int64) error {
	if _, err := os.Stat(filepath.Join(microVMSysfsCgroupRoot, "cgroup.controllers")); err != nil {
		log.Warn().Err(err).Str("container_id", inst.id).Msg("cgroup v2 is not mounted; microvm runs without cgroup limits")
		return nil
	}
	path := microVMCgroupPath(spec, inst.id)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	inst.cgroup = path
	if err := os.WriteFile(filepath.Join(path, "memory.max"), []byte(strconv.FormatInt(guestMemory+microVMMemoryHeadroom, 10)), 0o644); err != nil {
		return fmt.Errorf("set memory.max: %w", err)
	}
	if cpuMax := microVMCPUMax(spec); cpuMax != "" {
		if err := os.WriteFile(filepath.Join(path, "cpu.max"), []byte(cpuMax), 0o644); err != nil {
			return fmt.Errorf("set cpu.max: %w", err)
		}
	}
	return nil
}

// cgroupAttr starts cmd in its own process group inside the VM's cgroup
// (CLONE_INTO_CGROUP), so it never runs a moment without its limits. The
// returned fd must stay open until cmd has started.
func (inst *microVMInstance) cgroupAttr(cmd *exec.Cmd) (*os.File, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if inst.cgroup == "" {
		return nil, nil // (*os.File)(nil).Close is a harmless ErrInvalid
	}
	fd, err := os.OpenFile(inst.cgroup, os.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open vm cgroup: %w", err)
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(fd.Fd())
	return fd, nil
}

// --- network ---------------------------------------------------------------------

// preparedNetwork is what plumbNetwork leaves behind in a namespace: the
// guest's addresses and the tap the hypervisor attaches to.
type preparedNetwork struct {
	Network  microvm.Network `json:"network"`
	TapIndex int             `json:"tap_index"`
}

// preparedNetworkPath is PrepareNetworkSlot's record for a namespace. A new
// worker never reuses old slots, so sweepLeftoverVMs drops the directory.
func (m *MicroVM) preparedNetworkPath(nsPath string) string {
	return filepath.Join(m.cfg.MicroVMStateRoot, "netns", filepath.Base(nsPath)+".json")
}

// PrepareNetworkSlot plumbs a pooled namespace before a VM is assigned to it,
// keeping plumbNetwork's rtnl-serialized netlink calls off Run's path.
func (m *MicroVM) PrepareNetworkSlot(nsPath string) error {
	prepared, err := plumbNetwork(nsPath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	path := m.preparedNetworkPath(nsPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func (m *MicroVM) setupNetwork(inst *microVMInstance, spec *specs.Spec) (microvm.Network, error) {
	nsPath := networkNamespacePath(spec)
	if nsPath == "" {
		return microvm.Network{}, errors.New("spec has no network namespace path; the microvm runtime needs the container's namespace")
	}
	inst.netnsPath = nsPath

	prepared, ok := m.loadPreparedNetwork(nsPath)
	if !ok {
		var err error
		if prepared, err = plumbNetwork(nsPath); err != nil {
			return microvm.Network{}, err
		}
	}
	inst.tapIndex = prepared.TapIndex
	return prepared.Network, nil
}

// loadPreparedNetwork consumes PrepareNetworkSlot's record for nsPath without
// a netlink call: nothing else touches a pooled namespace before its VM.
func (m *MicroVM) loadPreparedNetwork(nsPath string) (preparedNetwork, bool) {
	path := m.preparedNetworkPath(nsPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return preparedNetwork{}, false
	}
	_ = os.Remove(path)
	var prepared preparedNetwork
	if err := json.Unmarshal(data, &prepared); err != nil || prepared.TapIndex == 0 {
		return preparedNetwork{}, false
	}
	if _, err := os.Stat(nsPath); err != nil {
		return preparedNetwork{}, false
	}
	return prepared, true
}

// plumbNetwork hands the namespace's veth to a guest: records its addresses
// and gateways, removes them from the namespace kernel, creates the tap, and
// wires tap and veth together behind the anti-spoof filters.
func plumbNetwork(nsPath string) (preparedNetwork, error) {
	var prepared preparedNetwork
	network := &prepared.Network
	err := inNetworkNamespace(nsPath, func() error {
		veth, err := findVeth()
		if err != nil {
			return err
		}
		attrs := veth.Attrs()
		network.MAC = attrs.HardwareAddr.String()
		network.MTU = attrs.MTU

		v4, err := netlink.AddrList(veth, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		v6, err := netlink.AddrList(veth, netlink.FAMILY_V6)
		if err != nil {
			return err
		}
		var ip4, ip6 net.IP
		if len(v4) > 0 {
			network.IPv4 = v4[0].IPNet.String()
			ip4 = v4[0].IP
		}
		for _, addr := range v6 {
			if addr.IP.IsLinkLocalUnicast() {
				continue
			}
			network.IPv6 = addr.IPNet.String()
			ip6 = addr.IP
			break
		}
		if network.IPv4 == "" {
			return fmt.Errorf("veth %s has no IPv4 address", attrs.Name)
		}
		network.Gateway4 = defaultGateway(veth, netlink.FAMILY_V4)
		if network.IPv6 != "" {
			network.Gateway6 = defaultGateway(veth, netlink.FAMILY_V6)
		}

		// The namespace kernel must not answer ARP/NDP for addresses the
		// guest now owns: drop every address and stop the kernel deriving a
		// new link-local. That is done over netlink, not the disable_ipv6
		// sysctl, whose handler spins on rtnl_trylock under contention.
		_ = netlink.LinkSetIP6AddrGenMode(veth, nl.IN6_ADDR_GEN_MODE_NONE)
		for _, addr := range append(v4, v6...) {
			if err := netlink.AddrDel(veth, &addr); err != nil && !errors.Is(err, unix.EADDRNOTAVAIL) {
				return fmt.Errorf("remove %s from %s: %w", addr.IPNet, attrs.Name, err)
			}
		}

		// Queues stays 0 so netlink does not keep the tap's fd open: the
		// hypervisor's TUNSETIFF on an attached single-queue tap fails with EBUSY.
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: microVMTapName, MTU: attrs.MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
		}
		if err := netlink.LinkAdd(tap); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create tap: %w", err)
		}
		tapLink, err := netlink.LinkByName(microVMTapName)
		if err != nil {
			return err
		}
		_ = netlink.LinkSetIP6AddrGenMode(tapLink, nl.IN6_ADDR_GEN_MODE_NONE)
		if err := netlink.LinkSetUp(tapLink); err != nil {
			return fmt.Errorf("bring tap up: %w", err)
		}
		if err := netlink.LinkSetUp(veth); err != nil {
			return fmt.Errorf("bring %s up: %w", attrs.Name, err)
		}
		prepared.TapIndex = tapLink.Attrs().Index

		if err := redirectAll(veth, tapLink); err != nil {
			return fmt.Errorf("redirect %s to tap: %w", attrs.Name, err)
		}
		if err := redirectGuestFrames(tapLink, veth, attrs.HardwareAddr, ip4, ip6); err != nil {
			return fmt.Errorf("redirect tap to %s: %w", attrs.Name, err)
		}
		return nil
	})
	return prepared, err
}

// findVeth returns the container side of the veth pair: the only non-loopback
// link in a namespace network.go has just set up.
func findVeth() (netlink.Link, error) {
	links, err := netlink.LinkList()
	if err != nil {
		return nil, err
	}
	var candidates []netlink.Link
	for _, link := range links {
		attrs := link.Attrs()
		if attrs.Name == "lo" || attrs.Name == microVMTapName || attrs.Flags&net.FlagLoopback != 0 {
			continue
		}
		if link.Type() == "veth" {
			return link, nil
		}
		candidates = append(candidates, link)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return nil, fmt.Errorf("could not identify the container veth among %d links", len(links))
}

func defaultGateway(link netlink.Link, family int) string {
	routes, err := netlink.RouteList(link, family)
	if err != nil {
		return ""
	}
	for _, route := range routes {
		if route.Dst == nil || route.Dst.IP.IsUnspecified() {
			if route.Gw != nil {
				return route.Gw.String()
			}
		}
	}
	return ""
}

func ensureIngress(link netlink.Link) error {
	qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
		LinkIndex: link.Attrs().Index,
		Handle:    netlink.MakeHandle(0xffff, 0),
		Parent:    netlink.HANDLE_INGRESS,
	}}
	if err := netlink.QdiscAdd(qdisc); err != nil && !errors.Is(err, unix.EEXIST) {
		return fmt.Errorf("add ingress qdisc to %s: %w", link.Attrs().Name, err)
	}
	return nil
}

func ingressAttrs(link netlink.Link, priority uint16, protocol uint16) netlink.FilterAttrs {
	return netlink.FilterAttrs{
		LinkIndex: link.Attrs().Index,
		Parent:    netlink.HANDLE_INGRESS,
		Priority:  priority,
		Protocol:  protocol,
	}
}

// redirectAll sends every frame arriving on from out through to.
func redirectAll(from, to netlink.Link) error {
	if err := ensureIngress(from); err != nil {
		return err
	}
	return netlink.FilterAdd(&netlink.MatchAll{
		FilterAttrs: ingressAttrs(from, 1, unix.ETH_P_ALL),
		Actions:     []netlink.Action{netlink.NewMirredAction(to.Attrs().Index)},
	})
}

// redirectGuestFrames wires tap ingress to the veth behind an allow-list of
// what the guest may source: frames from its own MAC only, carrying ARP that
// names its MAC and IPv4, IPv4 from ip4, IPv6 from ip6, and from link-local
// or the unspecified address only the neighbour discovery it needs (RS, NS,
// NA for its own addresses). Everything else is dropped before the bridge
// sees it.
func redirectGuestFrames(tap, veth netlink.Link, mac net.HardwareAddr, ip4, ip6 net.IP) error {
	if err := ensureIngress(tap); err != nil {
		return err
	}
	redirect := func() []netlink.Action { return []netlink.Action{netlink.NewMirredAction(veth.Attrs().Index)} }
	// Ethernet source at link offsets -8..-3 (the header sits before the
	// network header the offsets are relative to).
	fromMAC := []netlink.TcU32Key{
		u32Key(-8, 0xffffffff, binary.BigEndian.Uint32(mac[0:4])),
		u32Key(-4, 0xffff0000, binary.BigEndian.Uint32([]byte{mac[4], mac[5], 0, 0})),
	}
	allow := func(priority uint16, protocol uint16, keys ...netlink.TcU32Key) netlink.Filter {
		return &netlink.U32{
			FilterAttrs: ingressAttrs(tap, priority, protocol),
			Sel:         u32Selector(append(append([]netlink.TcU32Key(nil), fromMAC...), keys...)),
			Actions:     redirect(),
		}
	}
	// ARP: sender hardware address at 8, sender protocol address at 14.
	arpSender := func(spa net.IP) []netlink.TcU32Key {
		return []netlink.TcU32Key{
			u32Key(8, 0xffffffff, binary.BigEndian.Uint32(mac[0:4])),
			u32Key(12, 0xffffffff, binary.BigEndian.Uint32([]byte{mac[4], mac[5], spa[0], spa[1]})),
			u32Key(16, 0xffff0000, binary.BigEndian.Uint32([]byte{spa[2], spa[3], 0, 0})),
		}
	}
	// ICMPv6: next header at 6, type at 40, neighbour advertisement target at 48.
	icmp6 := func(icmpType byte) []netlink.TcU32Key {
		return []netlink.TcU32Key{
			u32Key(4, 0x0000ff00, uint32(unix.IPPROTO_ICMPV6)<<8),
			u32Key(40, 0xff000000, uint32(icmpType)<<24),
		}
	}
	linkLocal := u32Keys(net.ParseIP("fe80::").To16(), net.CIDRMask(10, 128), 8)
	unspecified := u32Keys(net.IPv6unspecified.To16(), net.CIDRMask(128, 128), 8)
	const (
		icmp6RS = 133
		icmp6NS = 135
		icmp6NA = 136
	)

	var filters []netlink.Filter
	prio := uint16(1)
	add := func(protocol uint16, keys ...netlink.TcU32Key) {
		filters = append(filters, allow(prio, protocol, keys...))
		prio++
	}
	if ip4 != nil {
		add(unix.ETH_P_ARP, arpSender(ip4.To4())...)
		add(unix.ETH_P_ARP, arpSender(net.IPv4zero.To4())...) // address probe
		add(unix.ETH_P_IP, u32Keys(ip4.To4(), net.CIDRMask(32, 32), 12)...)
	}
	if ip6 != nil {
		add(unix.ETH_P_IPV6, u32Keys(ip6.To16(), net.CIDRMask(128, 128), 8)...)
	}
	for _, t := range []byte{icmp6RS, icmp6NS} {
		add(unix.ETH_P_IPV6, append(append([]netlink.TcU32Key(nil), linkLocal...), icmp6(t)...)...)
	}
	for _, target := range []net.IP{ip6, linkLocalFromMAC(mac)} {
		if target == nil {
			continue
		}
		keys := append(append([]netlink.TcU32Key(nil), linkLocal...), icmp6(icmp6NA)...)
		add(unix.ETH_P_IPV6, append(keys, u32Keys(target.To16(), net.CIDRMask(128, 128), 48)...)...)
	}
	add(unix.ETH_P_IPV6, append(append([]netlink.TcU32Key(nil), unspecified...), icmp6(icmp6NS)...)...) // duplicate address detection
	filters = append(filters, &netlink.MatchAll{
		FilterAttrs: ingressAttrs(tap, 100, unix.ETH_P_ALL),
		Actions:     []netlink.Action{&netlink.GenericAction{ActionAttrs: netlink.ActionAttrs{Action: netlink.TC_ACT_SHOT}}},
	})
	for _, filter := range filters {
		if err := netlink.FilterAdd(filter); err != nil {
			return fmt.Errorf("add tap filter prio %d: %w", filter.Attrs().Priority, err)
		}
	}
	return nil
}

func u32Key(off int32, mask, val uint32) netlink.TcU32Key {
	return netlink.TcU32Key{Off: off, Mask: mask, Val: val & mask}
}

// linkLocalFromMAC is the EUI-64 link-local address the guest kernel derives
// from its MAC, the one it advertises for itself.
func linkLocalFromMAC(mac net.HardwareAddr) net.IP {
	if len(mac) != 6 {
		return nil
	}
	ip := net.ParseIP("fe80::").To16()
	copy(ip[8:], []byte{mac[0] ^ 0x02, mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]})
	return ip
}

// u32Keys builds u32 selector keys matching addr/mask at a byte offset into
// the network header, four bytes per key. Values are the numeric (host
// order) form of each 32-bit word; the netlink library converts them to the
// __be32 the kernel compares against.
func u32Keys(addr []byte, mask net.IPMask, offset int32) []netlink.TcU32Key {
	keys := make([]netlink.TcU32Key, 0, len(addr)/4)
	for i := 0; i+4 <= len(addr); i += 4 {
		m := binary.BigEndian.Uint32(mask[i : i+4])
		if m == 0 {
			continue
		}
		keys = append(keys, netlink.TcU32Key{
			Mask: m,
			Val:  binary.BigEndian.Uint32(addr[i:i+4]) & m,
			Off:  offset + int32(i),
		})
	}
	return keys
}

func u32Selector(keys []netlink.TcU32Key) *netlink.TcU32Sel {
	return &netlink.TcU32Sel{
		Flags: netlink.TC_U32_TERMINAL,
		// netlink sends cap(Keys) keys; spare capacity would add
		// match-anything keys.
		Keys: keys[:len(keys):len(keys)],
	}
}

func (inst *microVMInstance) removeTap() {
	if inst.netnsPath == "" || inst.tapIndex == 0 {
		return
	}
	_ = inNetworkNamespace(inst.netnsPath, func() error {
		if link, err := netlink.LinkByIndex(inst.tapIndex); err == nil {
			_ = netlink.LinkDel(link)
		}
		return nil
	})
}

// --- control channel -------------------------------------------------------------

// microVMControl is the host end of the guest init's vsock connection. The
// guest dials in once its root is assembled; from then on the stream carries
// the container process's pid and exit code out, and signals and freeze
// requests in.
type microVMControl struct {
	listener *net.UnixListener
	exit     chan int
	ready    chan struct{}
	// network, when set, is pushed to the guest as soon as it reports in: a
	// restored guest still carries the checkpointed container's identity.
	// networkApplied receives the outcome of the first push.
	network        *microvm.Network
	networkApplied chan error

	mu        sync.Mutex
	conn      net.Conn
	enc       *microvm.Encoder
	nextID    uint64
	pending   map[uint64]chan microvm.Message
	readyOnce sync.Once
}

func listenMicroVMControl(vsockPath string) (*microVMControl, error) {
	path := fmt.Sprintf("%s_%d", vsockPath, microvm.ControlPort)
	_ = os.Remove(path)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	ctrl := &microVMControl{
		listener:       listener,
		exit:           make(chan int, 1),
		ready:          make(chan struct{}),
		networkApplied: make(chan error, 1),
		pending:        map[uint64]chan microvm.Message{},
	}
	go ctrl.accept()
	return ctrl, nil
}

// accept serves guest connections until the listener closes. The guest
// reconnects whenever its vsock device is replaced under it, as an in-place
// restore does.
func (c *microVMControl) accept() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return
		}
		c.serve(conn)
	}
}

func (c *microVMControl) serve(conn net.Conn) {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = conn
	c.enc = microvm.NewEncoder(conn)
	c.mu.Unlock()
	c.readyOnce.Do(func() { close(c.ready) })

	dec := microvm.NewDecoder(conn)
	for {
		msg, err := dec.Decode()
		if err != nil {
			c.failPending(err)
			return
		}
		switch msg.Type {
		case microvm.MsgStarted:
			if c.network != nil {
				go c.pushNetwork()
			}
		case microvm.MsgPing:
		case microvm.MsgExit:
			select {
			case c.exit <- msg.Code:
			default:
			}
		case microvm.MsgAck:
			c.mu.Lock()
			ch := c.pending[msg.ID]
			delete(c.pending, msg.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- msg
			}
		}
	}
}

func (c *microVMControl) pushNetwork() {
	_, err := c.request(context.Background(), microvm.Message{Type: microvm.MsgNetwork, Network: c.network})
	if err != nil {
		log.Error().Err(err).Msg("restored microvm did not take its network configuration")
	}
	select {
	case c.networkApplied <- err:
	default:
	}
}

func (c *microVMControl) connected() bool {
	if c == nil {
		return false
	}
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

func (c *microVMControl) request(ctx context.Context, msg microvm.Message) (microvm.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, microVMControlRequestTimeout)
	defer cancel()
	select {
	case <-c.ready:
	case <-ctx.Done():
		return microvm.Message{}, fmt.Errorf("guest control channel not connected: %w", ctx.Err())
	}

	ch := make(chan microvm.Message, 1)
	c.mu.Lock()
	c.nextID++
	msg.ID = c.nextID
	c.pending[msg.ID] = ch
	enc, conn := c.enc, c.conn
	c.mu.Unlock()

	deadline, _ := ctx.Deadline()
	_ = conn.SetWriteDeadline(deadline)
	err := enc.Encode(msg)
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		c.mu.Lock()
		delete(c.pending, msg.ID)
		c.mu.Unlock()
		return microvm.Message{}, err
	}
	select {
	case reply := <-ch:
		if !reply.OK {
			return reply, fmt.Errorf("guest rejected %s: %s", msg.Type, reply.Error)
		}
		return reply, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, msg.ID)
		c.mu.Unlock()
		return microvm.Message{}, ctx.Err()
	}
}

func (c *microVMControl) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		ch <- microvm.Message{Type: microvm.MsgAck, ID: id, Error: err.Error()}
		delete(c.pending, id)
	}
}

func (c *microVMControl) close() {
	if c == nil {
		return
	}
	_ = c.listener.Close()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.mu.Unlock()
}

// --- lifecycle ---------------------------------------------------------------------

func (inst *microVMInstance) setPaused(paused bool) {
	inst.mu.Lock()
	inst.paused = paused
	inst.mu.Unlock()
}

func (inst *microVMInstance) isPaused() bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return inst.paused
}

// api is Cloud Hypervisor's HTTP API over the VM's unix socket.
func (inst *microVMInstance) api() *hypervisorAPI {
	return &hypervisorAPI{socket: filepath.Join(inst.stateDir, microVMAPISocket)}
}

type hypervisorAPI struct {
	socket string
}

// put issues PUT /api/v1/<action>; body, when non-nil, is sent as JSON.
func (a *hypervisorAPI) put(ctx context.Context, action string, body any) error {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost/api/v1/"+action, payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", a.socket)
	}}}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s: %s", action, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

func (inst *microVMInstance) alive() bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.exited || inst.hypervisor == nil || inst.hypervisor.Process == nil {
		return false
	}
	return inst.hypervisor.Process.Signal(syscall.Signal(0)) == nil
}

// pid is the hypervisor's, or 0 before it has started.
func (inst *microVMInstance) pid() int {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.hypervisor == nil || inst.hypervisor.Process == nil {
		return 0
	}
	return inst.hypervisor.Process.Pid
}

func (inst *microVMInstance) killHypervisor() {
	inst.mu.Lock()
	cmd, waited := inst.hypervisor, inst.waited
	inst.killed = true
	inst.mu.Unlock()
	// Once Wait has reaped it the pid may belong to someone else.
	if cmd == nil || cmd.Process == nil || waited {
		return
	}
	if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil && pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	_ = cmd.Process.Kill()
}

func (inst *microVMInstance) stopProcesses() {
	inst.killHypervisor()
	inst.mu.Lock()
	inst.exited = true
	virtiofsd := inst.virtiofsd
	inst.mu.Unlock()
	if virtiofsd != nil && virtiofsd.Process != nil {
		_ = virtiofsd.Process.Kill()
	}
	inst.ctrl.close()
}

// teardown releases everything Run set up. Canvas submounts must be gone
// before the worker unmounts the overlay under them.
func (inst *microVMInstance) teardown() error {
	inst.stopProcesses()
	inst.mu.Lock()
	hypervisor, virtiofsd := inst.hypervisor, inst.virtiofsd
	inst.mu.Unlock()
	if hypervisor != nil {
		waitProcessGone(hypervisor.Process, 5*time.Second)
	}
	if virtiofsd != nil {
		waitProcessGone(virtiofsd.Process, 5*time.Second)
	}

	var errs []error
	for i := len(inst.submounts) - 1; i >= 0; i-- {
		target := inst.submounts[i]
		if err := unix.Unmount(target, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			errs = append(errs, fmt.Errorf("unmount %s: %w", target, err))
		}
	}
	inst.submounts = nil
	inst.removeTap()
	if inst.scratch != "" {
		if err := os.Remove(inst.scratch); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if inst.cgroup != "" {
		if err := removeCgroup(inst.cgroup, 5*time.Second); err != nil {
			errs = append(errs, err)
		}
	}
	if err := os.RemoveAll(inst.stateDir); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// waitProcessGone polls, bounded by timeout, until the process has exited; a zombie counts.
func waitProcessGone(proc *os.Process, timeout time.Duration) {
	if proc == nil {
		return
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := proc.Signal(syscall.Signal(0)); err != nil {
			return
		}
		// A zombie still answers signal 0; check its state directly.
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", proc.Pid)); err != nil || strings.Contains(string(data), ") Z ") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func removeCgroup(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := unix.Rmdir(path)
		if err == nil || errors.Is(err, unix.ENOENT) {
			return nil
		}
		if !errors.Is(err, unix.EBUSY) || time.Now().After(deadline) {
			// Kill stragglers so the directory can go away.
			if procs, readErr := os.ReadFile(filepath.Join(path, "cgroup.procs")); readErr == nil {
				for _, pid := range strings.Fields(string(procs)) {
					if n, convErr := strconv.Atoi(pid); convErr == nil {
						_ = syscall.Kill(n, syscall.SIGKILL)
					}
				}
			}
			if err := unix.Rmdir(path); err == nil || errors.Is(err, unix.ENOENT) {
				return nil
			}
			procs, _ := os.ReadFile(filepath.Join(path, "cgroup.procs"))
			return fmt.Errorf("remove cgroup %s (pids %s): %w", path, strings.Join(strings.Fields(string(procs)), ","), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
