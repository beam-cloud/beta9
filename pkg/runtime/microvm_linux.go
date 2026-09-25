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
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
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
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// MicroVM implements Runtime with Cloud Hypervisor. See microvm.go.
type MicroVM struct {
	cfg Config
	mu  sync.Mutex
	vms map[string]*microVMInstance
}

var _ DiskFreezer = (*MicroVM)(nil)

type microVMInstance struct {
	id        string
	stateDir  string
	canvas    string
	scratch   string
	cgroup    string
	netnsPath string
	tapIndex  int
	submounts []string

	virtiofsd  *exec.Cmd
	hypervisor *exec.Cmd
	ctrl       *microVMControl
	console    *tailWriter

	mu     sync.Mutex
	exited bool
	// killed is set when the host itself SIGKILLs the hypervisor (a forced
	// stop, or a signal the guest would not take); Run then reports the
	// kill as the container's exit instead of a VM failure.
	killed bool
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
	return &MicroVM{cfg: cfg, vms: map[string]*microVMInstance{}}, nil
}

func (m *MicroVM) Name() string {
	return types.ContainerRuntimeMicroVM.String()
}

func (m *MicroVM) Capabilities() Capabilities {
	return Capabilities{JoinExistingNetNS: true, BlockRoot: true}
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
	spec, err := readBundleSpec(bundlePath)
	if err != nil {
		return -1, err
	}
	canvas := spec.Root.Path
	if canvas == "" {
		return -1, fmt.Errorf("spec has no root path")
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
	if _, exists := m.vms[containerID]; exists {
		m.mu.Unlock()
		return -1, fmt.Errorf("microvm %s already exists", containerID)
	}
	m.vms[containerID] = inst
	m.mu.Unlock()

	// Whatever happens, the processes must be gone when Run returns. Mounts,
	// the tap, the cgroup, and state files are released by Delete, which the
	// worker calls after every Run, so the overlay under the canvas can be
	// torn down in order.
	defer inst.stopProcesses()

	if err := os.RemoveAll(inst.stateDir); err != nil {
		return -1, err
	}
	if err := os.MkdirAll(inst.stateDir, 0o700); err != nil {
		return -1, err
	}

	network, err := m.setupNetwork(inst, spec)
	if err != nil {
		return -1, fmt.Errorf("setup microvm network: %w", err)
	}
	root, extra, err := m.prepareDisks(ctx, inst, spec)
	if err != nil {
		return -1, fmt.Errorf("prepare microvm disks: %w", err)
	}
	vmSpec, err := m.prepareCanvas(inst, spec, network, root, extra)
	if err != nil {
		return -1, fmt.Errorf("prepare microvm rootfs: %w", err)
	}
	if err := m.startVirtiofsd(ctx, inst); err != nil {
		return -1, fmt.Errorf("start virtiofsd: %w", err)
	}

	memory := microVMMemoryBytes(spec)
	if err := m.setupCgroup(inst, spec, memory); err != nil {
		return -1, fmt.Errorf("setup microvm cgroup: %w", err)
	}
	ctrl, err := listenMicroVMControl(filepath.Join(inst.stateDir, "vsock.sock"), vmSpec.ControlPort)
	if err != nil {
		return -1, fmt.Errorf("listen on vsock control socket: %w", err)
	}
	inst.ctrl = ctrl

	vcpus := microVMVCPUs(spec)
	args := microVMHypervisorArgs(inst.stateDir, m.cfg.MicroVMKernelPath, microVMKernelCmdline(), vcpus, memory, network.MAC, root, extra)
	cmd := exec.Command(m.cfg.MicroVMHypervisorPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// The serial console arrives a few bytes at a time; the worker's writers
	// log one record per Write, so hand them whole lines.
	var out io.Writer = inst.console
	var lines []*lineWriter
	if opts != nil && opts.OutputWriter != nil {
		lw := newLineWriter(opts.OutputWriter)
		lines = append(lines, lw)
		out = io.MultiWriter(lw, inst.console)
	}
	cmd.Stdout = out
	if opts != nil && opts.ErrorWriter != nil {
		lw := newLineWriter(opts.ErrorWriter)
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

	if err := startInNetworkNamespace(cmd, inst.netnsPath); err != nil {
		return -1, fmt.Errorf("start cloud-hypervisor: %w", err)
	}
	inst.mu.Lock()
	inst.hypervisor = cmd
	inst.mu.Unlock()
	if err := inst.addToCgroup(cmd.Process.Pid); err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("failed to place hypervisor in its cgroup")
	}
	if inst.virtiofsd != nil && inst.virtiofsd.Process != nil {
		if err := inst.addToCgroup(inst.virtiofsd.Process.Pid); err != nil {
			log.Debug().Err(err).Str("container_id", containerID).Msg("failed to place virtiofsd in the vm cgroup")
		}
	}
	log.Info().Str("container_id", containerID).Int("pid", cmd.Process.Pid).Int("vcpus", vcpus).Int64("memory_bytes", memory).Msg("microvm started")

	if opts != nil && opts.Started != nil {
		select {
		case opts.Started <- cmd.Process.Pid:
		case <-ctx.Done():
		}
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

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
				// The host stopped the VM (Kill with SIGKILL, or a signal the
				// guest did not acknowledge). That is the container's exit,
				// reported the way runc reports a SIGKILLed init.
				return 128 + int(syscall.SIGKILL), nil
			}
			return -1, fmt.Errorf("microvm exited before the container process reported: %v: %s", err, inst.console.String())
		}
	}
}

func (m *MicroVM) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	return fmt.Errorf("microvm runtime does not support exec; sandboxes use the in-guest process manager")
}

func (m *MicroVM) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	inst, ok := m.instance(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}
	if !inst.alive() {
		return nil
	}
	if sig == syscall.SIGKILL || inst.ctrl == nil || !inst.ctrl.connected() {
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
	if ok {
		delete(m.vms, containerID)
	}
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
	state := State{ID: containerID, Status: "stopped"}
	inst.mu.Lock()
	if inst.hypervisor != nil && inst.hypervisor.Process != nil {
		state.Pid = inst.hypervisor.Process.Pid
	}
	inst.mu.Unlock()
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

func (m *MicroVM) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
	return fmt.Errorf("microvm runtime does not support checkpoint")
}

func (m *MicroVM) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	return -1, fmt.Errorf("microvm runtime does not support restore")
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

// FreezeDisk implements DiskFreezer: the guest runs FIFREEZE on the mounted
// filesystem so a host-side qcow pivot captures a consistent image. A VM
// that is gone or not yet up has no dirty state, so that case is a no-op:
// the final sync after a sandbox stops must still be able to seal.
func (m *MicroVM) FreezeDisk(ctx context.Context, containerID, mountPath string) (func(), error) {
	noop := func() {}
	inst, ok := m.instance(containerID)
	if !ok || !inst.alive() || inst.ctrl == nil || !inst.ctrl.connected() {
		return noop, nil
	}
	if _, err := inst.ctrl.request(ctx, microvm.Message{Type: microvm.MsgFreeze, Text: mountPath}); err != nil {
		if !inst.alive() {
			return noop, nil
		}
		return nil, fmt.Errorf("freeze guest filesystem: %w", err)
	}
	return func() {
		thawCtx, cancel := context.WithTimeout(context.Background(), microVMControlRequestTimeout)
		defer cancel()
		if _, err := inst.ctrl.request(thawCtx, microvm.Message{Type: microvm.MsgThaw, Text: mountPath}); err != nil && inst.alive() {
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
	conn, err := dialGuestVsock(ctx, filepath.Join(inst.stateDir, "vsock.sock"), microvm.FSPort)
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

	reader := bufio.NewReaderSize(conn, 64<<10)
	line, err := reader.ReadBytes('\n')
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
	switch {
	case reply.Length == microvm.FSStreamUntilEOF:
		if _, err := io.Copy(sink, reader); err != nil {
			return nil, fmt.Errorf("read fs stream: %w", err)
		}
	case req.Op == microvm.FSOpRead && reply.Length > 0:
		if _, err := io.CopyN(sink, reader, reply.Length); err != nil {
			return nil, fmt.Errorf("read fs payload: %w", err)
		}
	}
	return &reply, nil
}

// ExportGuestTree streams a PAX tar of guestPath out of the guest and
// materializes it under hostDir with mknod, lchown and lsetxattr, which is
// why it needs the worker's privileges. Directory mtimes are restored last
// since creating children would clobber them.
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

func extractTree(r io.Reader, dst string) error {
	dst = filepath.Clean(dst)
	tr := tar.NewReader(r)
	type dirTime struct {
		path string
		when time.Time
	}
	var dirs []dirTime
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dst, filepath.Clean("/"+hdr.Name))
		if target != dst && !strings.HasPrefix(target, dst+string(filepath.Separator)) {
			return fmt.Errorf("entry %q escapes the export directory", hdr.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := os.FileMode(hdr.Mode) & 0o7777
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(target, 0o700); err != nil && !os.IsExist(err) {
				return err
			}
			dirs = append(dirs, dirTime{target, hdr.ModTime})
		case tar.TypeReg:
			file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			_ = os.Remove(target)
			if err := os.Link(filepath.Join(dst, filepath.Clean("/"+hdr.Linkname)), target); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			kind := uint32(unix.S_IFCHR)
			if hdr.Typeflag == tar.TypeBlock {
				kind = unix.S_IFBLK
			} else if hdr.Typeflag == tar.TypeFifo {
				kind = unix.S_IFIFO
			}
			_ = os.Remove(target)
			if err := unix.Mknod(target, kind|uint32(mode), int(unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor)))); err != nil {
				return fmt.Errorf("mknod %s: %w", hdr.Name, err)
			}
		default:
			continue
		}
		if err := os.Lchown(target, hdr.Uid, hdr.Gid); err != nil {
			return fmt.Errorf("chown %s: %w", hdr.Name, err)
		}
		if hdr.Typeflag != tar.TypeSymlink {
			if err := os.Chmod(target, mode); err != nil {
				return err
			}
		}
		for key, value := range hdr.PAXRecords {
			name, ok := strings.CutPrefix(key, "SCHILY.xattr.")
			if !ok || name == "security.selinux" {
				continue
			}
			if err := unix.Lsetxattr(target, name, []byte(value), 0); err != nil && !errors.Is(err, unix.ENOTSUP) {
				return fmt.Errorf("set xattr %s on %s: %w", name, hdr.Name, err)
			}
		}
		if hdr.Typeflag != tar.TypeDir {
			ts := unix.NsecToTimespec(hdr.ModTime.UnixNano())
			_ = unix.UtimesNanoAt(unix.AT_FDCWD, target, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		ts := unix.NsecToTimespec(dirs[i].when.UnixNano())
		_ = unix.UtimesNanoAt(unix.AT_FDCWD, dirs[i].path, []unix.Timespec{ts, ts}, 0)
	}
	return nil
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

func (m *MicroVM) prepareDisks(ctx context.Context, inst *microVMInstance, spec *specs.Spec) (microVMDisk, []microVMDisk, error) {
	scratch := filepath.Join(filepath.Dir(inst.canvas), "scratch.ext4")
	root, extra, err := microVMDiskPlan(spec, scratch)
	if err != nil {
		return root, nil, err
	}
	if strings.HasPrefix(root.arg, "path=") {
		sizeGiB := int64(microVMDefaultScratchGiB)
		if n, ok := annotationInt(spec, MicroVMScratchGiBAnnotation); ok && n > 0 {
			sizeGiB = n
		}
		if err := createScratchDisk(ctx, scratch, sizeGiB<<30); err != nil {
			return root, nil, err
		}
		inst.scratch = scratch
	}
	return root, extra, nil
}

// createScratchDisk makes a sparse ext4 image. Lazy initialisation keeps
// mkfs to tens of milliseconds regardless of the virtual size.
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

func (m *MicroVM) prepareCanvas(inst *microVMInstance, spec *specs.Spec, network microvm.Network, root microVMDisk, extra []microVMDisk) (*microvm.Spec, error) {
	for _, dir := range []string{"dev", "proc", "sys", "run", "tmp", microvm.CanvasDir, microvm.DiskMount, microvm.ImageMount, microvm.NewRoot} {
		if err := os.MkdirAll(filepath.Join(inst.canvas, dir), 0o755); err != nil {
			return nil, err
		}
	}
	if err := copyFile(m.cfg.MicroVMInitPath, filepath.Join(inst.canvas, microvm.InitPath), 0o755); err != nil {
		return nil, fmt.Errorf("install guest init: %w", err)
	}

	bindMounts, tmpfs := microVMMountPlan(spec.Mounts)
	binds := make([]microvm.Bind, 0, len(bindMounts))
	for i, mount := range bindMounts {
		bind, err := inst.bindIntoCanvas(i, mount)
		if err != nil {
			return nil, err
		}
		binds = append(binds, bind)
	}
	vmSpec := microVMGuestSpec(spec, network, root, extra, binds, tmpfs)

	data, err := json.MarshalIndent(vmSpec, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(inst.canvas, microvm.SpecFile), data, 0o644); err != nil {
		return nil, err
	}
	return vmSpec, nil
}

// bindIntoCanvas mounts one OCI bind mount under the canvas's binds directory
// rather than at its destination. virtiofsd announces each as a submount, and
// overlayfs refuses to look through a submount in its lower layer, so the
// guest binds the entry from the virtiofs root to the destination in the
// assembled root instead.
func (inst *microVMInstance) bindIntoCanvas(index int, mount specs.Mount) (microvm.Bind, error) {
	dest := filepath.Clean(mount.Destination)
	if !filepath.IsAbs(dest) || dest == "/" {
		return microvm.Bind{}, fmt.Errorf("mount destination %q is not an absolute path inside the rootfs", mount.Destination)
	}
	info, err := os.Stat(mount.Source)
	if err != nil {
		return microvm.Bind{}, fmt.Errorf("bind source %s: %w", mount.Source, err)
	}
	isFile := !info.IsDir()
	guestSource := filepath.Join(microvm.BindsDir, strconv.Itoa(index))
	target := filepath.Join(inst.canvas, guestSource)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return microvm.Bind{}, err
	}
	if isFile {
		if err := os.WriteFile(target, nil, 0o644); err != nil {
			return microvm.Bind{}, err
		}
	} else if err := os.MkdirAll(target, 0o755); err != nil {
		return microvm.Bind{}, err
	}

	flags := uintptr(unix.MS_BIND)
	if hasOption(mount.Options, "rbind") {
		flags |= unix.MS_REC
	}
	if err := unix.Mount(mount.Source, target, "", flags, ""); err != nil {
		return microvm.Bind{}, fmt.Errorf("bind %s to %s: %w", mount.Source, target, err)
	}
	inst.submounts = append(inst.submounts, target)
	readOnly := hasOption(mount.Options, "ro")
	if readOnly {
		if err := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return microvm.Bind{}, fmt.Errorf("remount %s read-only: %w", target, err)
		}
	}
	return microvm.Bind{Source: guestSource, Destination: dest, File: isFile, ReadOnly: readOnly}, nil
}

// --- virtiofsd -----------------------------------------------------------------

func (m *MicroVM) startVirtiofsd(ctx context.Context, inst *microVMInstance) error {
	socket := filepath.Join(inst.stateDir, "virtiofs.sock")
	// --killpriv-v2 makes the guest mark the share SB_NOSEC, so it stops
	// asking for security.capability before every write. Without it each
	// write costs virtiofsd an extra open/getxattr/close of the file on the
	// host, and on FUSE-backed volumes (geesefs with fsync-on-close) every
	// one of those closes uploads a half-written snapshot to the bucket.
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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

// startInNetworkNamespace starts cmd with the calling thread switched into
// the container's network namespace so the child inherits it. Only the
// network namespace changes; mounts, pids, and cgroups stay the worker's.
func startInNetworkNamespace(cmd *exec.Cmd, nsPath string) error {
	return inNetworkNamespace(nsPath, cmd.Start)
}

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

func (inst *microVMInstance) addToCgroup(pid int) error {
	if inst.cgroup == "" {
		return nil
	}
	return os.WriteFile(filepath.Join(inst.cgroup, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644)
}

// --- network ---------------------------------------------------------------------

// setupNetwork runs inside the container's network namespace. It records the
// veth's MAC, MTU, addresses, and default routes for the guest, removes the
// addresses from the veth so the namespace kernel stops answering for them,
// creates the tap, and wires the two together with tc mirred redirects. The
// tap ingress additionally drops any frame the guest sources from an address
// that is not its own.
func (m *MicroVM) setupNetwork(inst *microVMInstance, spec *specs.Spec) (microvm.Network, error) {
	nsPath := networkNamespacePath(spec)
	if nsPath == "" {
		return microvm.Network{}, errors.New("spec has no network namespace path; the microvm runtime needs the container's namespace")
	}
	inst.netnsPath = nsPath

	var network microvm.Network
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
		for _, addr := range v4 {
			network.IPv4 = addr.IPNet.String()
			ip4 = addr.IP
			break
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
		// guest now owns.
		_ = writeSysctl(filepath.Join("/proc/sys/net/ipv6/conf", attrs.Name, "disable_ipv6"), "1")
		for _, addr := range append(v4, v6...) {
			a := addr
			if err := netlink.AddrDel(veth, &a); err != nil && !errors.Is(err, unix.EADDRNOTAVAIL) {
				return fmt.Errorf("remove %s from %s: %w", addr.IPNet, attrs.Name, err)
			}
		}

		// Queues stays 0: with a queue count netlink keeps the tap's fd open
		// in this process, and a single-queue tap that is already attached
		// makes the hypervisor's TUNSETIFF fail with EBUSY. The persistent
		// device outlives the creating fd; Cloud Hypervisor attaches to it by
		// name.
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: microVMTapName, MTU: attrs.MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
		}
		if err := netlink.LinkAdd(tap); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create tap: %w", err)
		}
		for _, fd := range tap.Fds {
			_ = fd.Close()
		}
		tapLink, err := netlink.LinkByName(microVMTapName)
		if err != nil {
			return err
		}
		_ = writeSysctl(filepath.Join("/proc/sys/net/ipv6/conf", microVMTapName, "disable_ipv6"), "1")
		if err := netlink.LinkSetUp(tapLink); err != nil {
			return fmt.Errorf("bring tap up: %w", err)
		}
		if err := netlink.LinkSetUp(veth); err != nil {
			return fmt.Errorf("bring %s up: %w", attrs.Name, err)
		}
		inst.tapIndex = tapLink.Attrs().Index

		if err := redirectAll(veth, tapLink); err != nil {
			return fmt.Errorf("redirect %s to tap: %w", attrs.Name, err)
		}
		if err := redirectGuestFrames(tapLink, veth, ip4, ip6); err != nil {
			return fmt.Errorf("redirect tap to %s: %w", attrs.Name, err)
		}
		return nil
	})
	return network, err
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

func writeSysctl(path, value string) error {
	return os.WriteFile(path, []byte(value), 0o644)
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

// redirectGuestFrames wires tap ingress to the veth with an allow-list on the
// source address: ARP, IPv4 from ip4, IPv6 from ip6, link-local, or the
// unspecified address (duplicate address detection). Everything else is
// dropped before it can reach the bridge.
func redirectGuestFrames(tap, veth netlink.Link, ip4, ip6 net.IP) error {
	if err := ensureIngress(tap); err != nil {
		return err
	}
	redirect := func() []netlink.Action { return []netlink.Action{netlink.NewMirredAction(veth.Attrs().Index)} }

	filters := []netlink.Filter{
		&netlink.MatchAll{FilterAttrs: ingressAttrs(tap, 1, unix.ETH_P_ARP), Actions: redirect()},
	}
	if ip4 != nil {
		filters = append(filters, &netlink.U32{
			FilterAttrs: ingressAttrs(tap, 2, unix.ETH_P_IP),
			Sel:         u32Selector(u32Keys(ip4.To4(), net.CIDRMask(32, 32), 12)),
			Actions:     redirect(),
		})
	}
	if ip6 != nil {
		filters = append(filters, &netlink.U32{
			FilterAttrs: ingressAttrs(tap, 3, unix.ETH_P_IPV6),
			Sel:         u32Selector(u32Keys(ip6.To16(), net.CIDRMask(128, 128), 8)),
			Actions:     redirect(),
		})
	}
	linkLocal := net.ParseIP("fe80::")
	filters = append(filters,
		&netlink.U32{
			FilterAttrs: ingressAttrs(tap, 4, unix.ETH_P_IPV6),
			Sel:         u32Selector(u32Keys(linkLocal.To16(), net.CIDRMask(10, 128), 8)),
			Actions:     redirect(),
		},
		&netlink.U32{
			FilterAttrs: ingressAttrs(tap, 5, unix.ETH_P_IPV6),
			Sel:         u32Selector(u32Keys(net.IPv6unspecified.To16(), net.CIDRMask(128, 128), 8)),
			Actions:     redirect(),
		},
		&netlink.MatchAll{
			FilterAttrs: ingressAttrs(tap, 100, unix.ETH_P_ALL),
			Actions:     []netlink.Action{&netlink.GenericAction{ActionAttrs: netlink.ActionAttrs{Action: netlink.TC_ACT_SHOT}}},
		},
	)
	for _, filter := range filters {
		if err := netlink.FilterAdd(filter); err != nil {
			return fmt.Errorf("add tap filter prio %d: %w", filter.Attrs().Priority, err)
		}
	}
	return nil
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
	// The library copies cap(), not len(); spare capacity would become
	// match-anything keys.
	return keys[:len(keys):len(keys)]
}

func u32Selector(keys []netlink.TcU32Key) *netlink.TcU32Sel {
	return &netlink.TcU32Sel{
		Flags: netlink.TC_U32_TERMINAL,
		Nkeys: uint8(len(keys)),
		Keys:  keys,
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
	started  chan int
	exit     chan int
	ready    chan struct{}

	mu      sync.Mutex
	conn    net.Conn
	enc     *microvm.Encoder
	nextID  uint64
	pending map[uint64]chan microvm.Message
}

func listenMicroVMControl(vsockPath string, port uint32) (*microVMControl, error) {
	if port == 0 {
		port = microvm.ControlPort
	}
	path := fmt.Sprintf("%s_%d", vsockPath, port)
	_ = os.Remove(path)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	ctrl := &microVMControl{
		listener: listener,
		started:  make(chan int, 1),
		exit:     make(chan int, 1),
		ready:    make(chan struct{}),
		pending:  map[uint64]chan microvm.Message{},
	}
	go ctrl.accept()
	return ctrl, nil
}

func (c *microVMControl) accept() {
	conn, err := c.listener.Accept()
	if err != nil {
		return
	}
	c.mu.Lock()
	c.conn = conn
	c.enc = microvm.NewEncoder(conn)
	c.mu.Unlock()
	close(c.ready)

	dec := microvm.NewDecoder(conn)
	for {
		msg, err := dec.Decode()
		if err != nil {
			c.failPending(err)
			return
		}
		switch msg.Type {
		case microvm.MsgStarted:
			select {
			case c.started <- msg.Pid:
			default:
			}
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
		case microvm.MsgLog:
			log.Debug().Str("guest", msg.Text).Msg("microvm init")
		}
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
	enc := c.enc
	c.mu.Unlock()

	if err := enc.Encode(msg); err != nil {
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

func (inst *microVMInstance) alive() bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.exited || inst.hypervisor == nil || inst.hypervisor.Process == nil {
		return false
	}
	return inst.hypervisor.Process.Signal(syscall.Signal(0)) == nil
}

func (inst *microVMInstance) killHypervisor() {
	inst.mu.Lock()
	cmd := inst.hypervisor
	inst.killed = true
	inst.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
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

// teardown releases everything Run set up, in reverse: processes, control
// sockets, canvas submounts (before the worker unmounts the overlay under
// them), the tap, the scratch disk, the cgroup, and the state directory.
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

// waitProcessGone polls until the process no longer exists or has been
// reaped by whoever called Wait on it, bounded by timeout.
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
			// Move stragglers out so the directory can go away.
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
			return fmt.Errorf("remove cgroup %s: %w", path, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
