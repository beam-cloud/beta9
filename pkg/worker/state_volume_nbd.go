package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrStateVolumeNBDUnavailable = errors.New("no free NBD device")

type StateVolumeCommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type OSStateVolumeCommandRunner struct{}

func (OSStateVolumeCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("run %s: %w: %s", name, err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

type StateVolumeNBDAllocator struct {
	SysBlockRoot  string
	DevRoot       string
	LockRoot      string
	MountInfoPath string
	MaxDevices    int
	Kernel        StateVolumeNBDKernel

	mu     sync.Mutex
	leased map[int]struct{}
}

// StateVolumeNBDKernel is the kernel-truth boundary for an NBD lease. A
// successful userspace command is never sufficient: the worker does not mark
// a volume ready, or release its node-global flock, until these postconditions
// prove the exact block device, size, mount, and detach state.
type StateVolumeNBDKernel interface {
	ValidateDevice(sysDevicePath, devicePath string) error
	WaitConnected(ctx context.Context, sysDevicePath string, expectedSizeBytes int64) error
	VerifyMounted(sysDevicePath, devicePath, mountPath string, readOnly bool) error
	WaitUnmounted(ctx context.Context, devicePath, mountPath string) error
	WaitDisconnected(ctx context.Context, sysDevicePath string) error
}

type linuxStateVolumeNBDKernel struct {
	MountInfoPath string
}

func (a *StateVolumeNBDAllocator) kernel() StateVolumeNBDKernel {
	if a.Kernel != nil {
		return a.Kernel
	}
	return linuxStateVolumeNBDKernel{MountInfoPath: a.MountInfoPath}
}

func (a *StateVolumeNBDAllocator) sysDevicePath(devicePath string) string {
	sysRoot, _, _ := a.normalizedRoots()
	return filepath.Join(sysRoot, filepath.Base(filepath.Clean(devicePath)))
}

func parseStateVolumeDeviceNumber(value string) (uint64, uint64, error) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid device number %q", value)
	}
	major, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid device major %q: %w", parts[0], err)
	}
	minor, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid device minor %q: %w", parts[1], err)
	}
	return major, minor, nil
}

// linuxDeviceMajorMinor implements Linux's new_encode_dev layout. The
// production worker is Linux-only; keeping this arithmetic local lets Darwin
// unit tests compile while injecting a fake kernel seam.
func linuxDeviceMajorMinor(rdev uint64) (uint64, uint64) {
	major := ((rdev >> 8) & 0xfff) | ((rdev >> 32) & 0xfffff000)
	minor := (rdev & 0xff) | ((rdev >> 12) & 0xffffff00)
	return major, minor
}

func (k linuxStateVolumeNBDKernel) ValidateDevice(sysDevicePath, devicePath string) error {
	info, err := os.Lstat(devicePath)
	if err != nil {
		return fmt.Errorf("lstat NBD device %q: %w", devicePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		return fmt.Errorf("NBD device %q is not a block-special device", devicePath)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("NBD device %q has no kernel stat identity", devicePath)
	}
	sysDev, err := os.ReadFile(filepath.Join(sysDevicePath, "dev"))
	if err != nil {
		return fmt.Errorf("read NBD sysfs identity for %q: %w", devicePath, err)
	}
	wantMajor, wantMinor, err := parseStateVolumeDeviceNumber(string(sysDev))
	if err != nil {
		return fmt.Errorf("parse NBD sysfs identity for %q: %w", devicePath, err)
	}
	major, minor := linuxDeviceMajorMinor(uint64(stat.Rdev))
	if major != wantMajor || minor != wantMinor {
		return fmt.Errorf("NBD device %q identity %d:%d does not match sysfs %d:%d", devicePath, major, minor, wantMajor, wantMinor)
	}
	return nil
}

func readStateVolumeSysfsUint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func waitStateVolumeKernelCondition(ctx context.Context, condition func() (bool, error)) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (k linuxStateVolumeNBDKernel) WaitConnected(ctx context.Context, sysDevicePath string, expectedSizeBytes int64) error {
	if expectedSizeBytes <= 0 || expectedSizeBytes%512 != 0 {
		return fmt.Errorf("NBD expected size %d is not a positive 512-byte multiple", expectedSizeBytes)
	}
	expectedSectors := uint64(expectedSizeBytes / 512)
	err := waitStateVolumeKernelCondition(ctx, func() (bool, error) {
		pid, err := readStateVolumeSysfsUint(filepath.Join(sysDevicePath, "pid"))
		if err != nil {
			return false, err
		}
		sectors, err := readStateVolumeSysfsUint(filepath.Join(sysDevicePath, "size"))
		if err != nil {
			return false, err
		}
		if pid == 0 || sectors == 0 {
			return false, nil
		}
		if sectors != expectedSectors {
			return false, fmt.Errorf("NBD sysfs size %d sectors does not match expected %d", sectors, expectedSectors)
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("wait for connected NBD %q: %w", sysDevicePath, err)
	}
	return nil
}

type stateVolumeMountRecord struct {
	major, minor uint64
	target       string
	options      map[string]struct{}
	filesystem   string
	source       string
}

func stateVolumeMountOptions(values ...string) map[string]struct{} {
	options := make(map[string]struct{})
	for _, value := range values {
		for _, option := range strings.Split(value, ",") {
			if option = strings.TrimSpace(option); option != "" {
				options[option] = struct{}{}
			}
		}
	}
	return options
}

func readStateVolumeMountRecords(path string) ([]stateVolumeMountRecord, error) {
	if path == "" {
		path = "/proc/self/mountinfo"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	records := make([]stateVolumeMountRecord, 0)
	for _, line := range strings.Split(string(data), "\n") {
		separator := strings.Index(line, " - ")
		if separator < 0 {
			continue
		}
		pre := strings.Fields(line[:separator])
		post := strings.Fields(line[separator+3:])
		if len(pre) < 6 || len(post) < 3 {
			return nil, fmt.Errorf("invalid mountinfo record %q", line)
		}
		major, minor, err := parseStateVolumeDeviceNumber(pre[2])
		if err != nil {
			return nil, err
		}
		records = append(records, stateVolumeMountRecord{
			major: major, minor: minor, target: decodeStateVolumeMountInfoPath(pre[4]),
			options: stateVolumeMountOptions(pre[5], post[2]), filesystem: post[0],
			source: decodeStateVolumeMountInfoPath(post[1]),
		})
	}
	return records, nil
}

func (k linuxStateVolumeNBDKernel) VerifyMounted(sysDevicePath, devicePath, mountPath string, readOnly bool) error {
	sysDev, err := os.ReadFile(filepath.Join(sysDevicePath, "dev"))
	if err != nil {
		return err
	}
	wantMajor, wantMinor, err := parseStateVolumeDeviceNumber(string(sysDev))
	if err != nil {
		return err
	}
	records, err := readStateVolumeMountRecords(k.MountInfoPath)
	if err != nil {
		return err
	}
	for _, record := range records {
		if filepath.Clean(record.target) != filepath.Clean(mountPath) {
			continue
		}
		if record.major != wantMajor || record.minor != wantMinor || filepath.Clean(record.source) != filepath.Clean(devicePath) || record.filesystem != "ext4" {
			return fmt.Errorf("mount %q does not match exact NBD source, identity, and ext4 filesystem", mountPath)
		}
		_, hasNoatime := record.options["noatime"]
		_, hasRO := record.options["ro"]
		_, hasRW := record.options["rw"]
		_, hasNoload := record.options["noload"]
		_, hasNoRecovery := record.options["norecovery"]
		if !hasNoatime || (readOnly && (!hasRO || hasRW || (!hasNoload && !hasNoRecovery))) || (!readOnly && (!hasRW || hasRO)) {
			return fmt.Errorf("mount %q has unsafe options", mountPath)
		}
		return nil
	}
	return fmt.Errorf("mount %q is absent from kernel mountinfo", mountPath)
}

func (k linuxStateVolumeNBDKernel) WaitUnmounted(ctx context.Context, devicePath, mountPath string) error {
	return waitStateVolumeKernelCondition(ctx, func() (bool, error) {
		records, err := readStateVolumeMountRecords(k.MountInfoPath)
		if err != nil {
			return false, err
		}
		for _, record := range records {
			if filepath.Clean(record.source) == filepath.Clean(devicePath) || filepath.Clean(record.target) == filepath.Clean(mountPath) {
				return false, nil
			}
		}
		return true, nil
	})
}

func (k linuxStateVolumeNBDKernel) WaitDisconnected(ctx context.Context, sysDevicePath string) error {
	return waitStateVolumeKernelCondition(ctx, func() (bool, error) {
		pid, err := readStateVolumeSysfsUint(filepath.Join(sysDevicePath, "pid"))
		if os.IsNotExist(err) {
			pid = 0
		} else if err != nil {
			return false, err
		}
		sectors, err := readStateVolumeSysfsUint(filepath.Join(sysDevicePath, "size"))
		if os.IsNotExist(err) {
			sectors = 0
		} else if err != nil {
			return false, err
		}
		return pid == 0 && sectors == 0, nil
	})
}

func (a *StateVolumeNBDAllocator) deviceIndices() ([]int, error) {
	sysRoot, devRoot, _ := a.normalizedRoots()
	entries, err := os.ReadDir(sysRoot)
	if err != nil {
		return nil, fmt.Errorf("read NBD sysfs root %s: %w", sysRoot, err)
	}
	indices := make([]int, 0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "nbd") {
			continue
		}
		index, parseErr := strconv.Atoi(strings.TrimPrefix(entry.Name(), "nbd"))
		info, statErr := os.Stat(filepath.Join(sysRoot, entry.Name()))
		devicePath := filepath.Join(devRoot, entry.Name())
		deviceErr := a.kernel().ValidateDevice(filepath.Join(sysRoot, entry.Name()), devicePath)
		if parseErr == nil && index >= 0 && statErr == nil && info.IsDir() && deviceErr == nil {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	if a.MaxDevices > 0 && len(indices) > a.MaxDevices {
		indices = indices[:a.MaxDevices]
	}
	return indices, nil
}

// Capacity returns the allocator's configured node-global device budget and
// a race-safe free count. It temporarily holds every available flock so the
// preflight view cannot double-count a device owned by another worker.
func (a *StateVolumeNBDAllocator) Capacity() (total, free int, err error) {
	indices, err := a.deviceIndices()
	if err != nil {
		return 0, 0, err
	}
	total = len(indices)
	leases := make([]*StateVolumeNBDLease, 0, total)
	defer func() {
		for _, lease := range leases {
			if releaseErr := lease.Release(); releaseErr != nil && err == nil {
				err = releaseErr
			}
		}
	}()
	for len(leases) < total {
		lease, acquireErr := a.Acquire()
		if errors.Is(acquireErr, ErrStateVolumeNBDUnavailable) {
			break
		}
		if acquireErr != nil {
			return total, len(leases), acquireErr
		}
		leases = append(leases, lease)
	}
	return total, len(leases), nil
}

type StateVolumeNBDLease struct {
	Index      int
	DevicePath string

	allocator *StateVolumeNBDAllocator
	lockFile  *os.File
	once      sync.Once
}

func (a *StateVolumeNBDAllocator) normalizedRoots() (string, string, string) {
	sysRoot := a.SysBlockRoot
	if sysRoot == "" {
		sysRoot = "/sys/block"
	}
	devRoot := a.DevRoot
	if devRoot == "" {
		devRoot = "/dev"
	}
	lockRoot := a.LockRoot
	if lockRoot == "" {
		// This is an invariant node-global hostPath shared by every worker pod;
		// graph/cache roots may be pool- or slot-specific and are never suitable
		// for coordinating allocation of the host's global /dev/nbd namespace.
		lockRoot = "/var/lib/beta9/state-volume-locks"
	}
	return sysRoot, devRoot, lockRoot
}

func (a *StateVolumeNBDAllocator) Acquire() (*StateVolumeNBDLease, error) {
	sysRoot, devRoot, lockRoot := a.normalizedRoots()
	indices, err := a.deviceIndices()
	if err != nil {
		return nil, err
	}
	if len(indices) == 0 {
		return nil, fmt.Errorf("%w: no nbd devices under %s", ErrStateVolumeNBDUnavailable, sysRoot)
	}
	if err := os.MkdirAll(lockRoot, 0755); err != nil {
		return nil, fmt.Errorf("create NBD lock directory %s: %w", lockRoot, err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.leased == nil {
		a.leased = make(map[int]struct{})
	}
	for _, index := range indices {
		if _, inUse := a.leased[index]; inUse {
			continue
		}
		lockPath := filepath.Join(lockRoot, fmt.Sprintf("nbd%d.lock", index))
		lockFile, openErr := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
		if openErr != nil {
			return nil, fmt.Errorf("open NBD lock %s: %w", lockPath, openErr)
		}
		if flockErr := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); flockErr != nil {
			_ = lockFile.Close()
			continue
		}
		busy, busyErr := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, fmt.Sprintf("nbd%d", index)))
		if busyErr != nil {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()
			return nil, busyErr
		}
		if busy {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()
			continue
		}
		devicePath := filepath.Join(devRoot, fmt.Sprintf("nbd%d", index))
		if identityErr := a.kernel().ValidateDevice(filepath.Join(sysRoot, fmt.Sprintf("nbd%d", index)), devicePath); identityErr != nil {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()
			continue
		}
		mounted, mountErr := a.deviceMounted(devicePath)
		if mountErr != nil {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()
			return nil, mountErr
		}
		if mounted {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()
			continue
		}
		a.leased[index] = struct{}{}
		return &StateVolumeNBDLease{
			Index:      index,
			DevicePath: devicePath,
			allocator:  a,
			lockFile:   lockFile,
		}, nil
	}
	return nil, ErrStateVolumeNBDUnavailable
}

// Adopt takes the node-global flock for a journal-owned NBD that is already
// connected. It is only valid after the caller has proved the journal's QSD
// PID, executable, and kernel start time; it never turns an arbitrary busy NBD
// into an owned lease.
func (a *StateVolumeNBDAllocator) Adopt(devicePath string) (*StateVolumeNBDLease, error) {
	sysRoot, devRoot, lockRoot := a.normalizedRoots()
	name := filepath.Base(filepath.Clean(devicePath))
	if filepath.Clean(devicePath) != filepath.Join(devRoot, name) || !strings.HasPrefix(name, "nbd") {
		return nil, fmt.Errorf("invalid journal NBD device %q", devicePath)
	}
	index, err := strconv.Atoi(strings.TrimPrefix(name, "nbd"))
	if err != nil || index < 0 {
		return nil, fmt.Errorf("invalid journal NBD device %q", devicePath)
	}
	indices, err := a.deviceIndices()
	if err != nil {
		return nil, err
	}
	valid := false
	for _, candidate := range indices {
		if candidate == index {
			valid = true
			break
		}
	}
	if !valid {
		return nil, fmt.Errorf("journal NBD device %q is outside configured capacity", devicePath)
	}
	if err := a.kernel().ValidateDevice(filepath.Join(sysRoot, name), devicePath); err != nil {
		return nil, fmt.Errorf("journal NBD device %q is missing or invalid: %w", devicePath, err)
	}
	busy, err := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, name))
	if err != nil {
		return nil, err
	}
	if !busy {
		return nil, fmt.Errorf("journal NBD device %q is not connected", devicePath)
	}
	if err := os.MkdirAll(lockRoot, 0755); err != nil {
		return nil, err
	}
	lockFile, err := os.OpenFile(filepath.Join(lockRoot, name+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("journal NBD device %q is locked by another worker: %w", devicePath, err)
	}
	a.mu.Lock()
	if a.leased == nil {
		a.leased = make(map[int]struct{})
	}
	if _, exists := a.leased[index]; exists {
		a.mu.Unlock()
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, fmt.Errorf("journal NBD device %q is already adopted", devicePath)
	}
	a.leased[index] = struct{}{}
	a.mu.Unlock()
	return &StateVolumeNBDLease{Index: index, DevicePath: devicePath, allocator: a, lockFile: lockFile}, nil
}

func (a *StateVolumeNBDAllocator) WaitConnected(ctx context.Context, devicePath string, expectedSizeBytes int64) error {
	return a.kernel().WaitConnected(ctx, a.sysDevicePath(devicePath), expectedSizeBytes)
}

func (a *StateVolumeNBDAllocator) VerifyMounted(devicePath, mountPath string, readOnly bool) error {
	return a.kernel().VerifyMounted(a.sysDevicePath(devicePath), devicePath, mountPath, readOnly)
}

func (a *StateVolumeNBDAllocator) WaitUnmounted(ctx context.Context, devicePath, mountPath string) error {
	return a.kernel().WaitUnmounted(ctx, devicePath, mountPath)
}

func (a *StateVolumeNBDAllocator) WaitDisconnected(ctx context.Context, devicePath string) error {
	return a.kernel().WaitDisconnected(ctx, a.sysDevicePath(devicePath))
}

func (a *StateVolumeNBDAllocator) deviceMounted(devicePath string) (bool, error) {
	mounted, _, err := a.deviceMount(devicePath)
	return mounted, err
}

func (a *StateVolumeNBDAllocator) deviceMount(devicePath string) (bool, string, error) {
	mountInfoPath := a.MountInfoPath
	if mountInfoPath == "" {
		mountInfoPath = "/proc/self/mountinfo"
	}
	data, err := os.ReadFile(mountInfoPath)
	if os.IsNotExist(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("read mountinfo %s: %w", mountInfoPath, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		separator := strings.Index(line, " - ")
		if separator < 0 {
			continue
		}
		post := strings.Fields(line[separator+3:])
		if len(post) < 2 {
			continue
		}
		source := strings.ReplaceAll(post[1], `\040`, " ")
		if source == devicePath || strings.HasPrefix(source, devicePath+"p") {
			pre := strings.Fields(line[:separator])
			if len(pre) < 5 {
				return false, "", fmt.Errorf("invalid mountinfo entry for %s", devicePath)
			}
			return true, decodeStateVolumeMountInfoPath(pre[4]), nil
		}
	}
	return false, "", nil
}

func decodeStateVolumeMountInfoPath(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

func stateVolumeNBDDeviceBusy(sysDevicePath string) (bool, error) {
	pid, err := os.ReadFile(filepath.Join(sysDevicePath, "pid"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read NBD pid for %s: %w", sysDevicePath, err)
	}
	value := strings.TrimSpace(string(pid))
	return value != "" && value != "0", nil
}

func (l *StateVolumeNBDLease) Release() error {
	if l == nil || l.allocator == nil || l.lockFile == nil {
		return nil
	}
	var releaseErr error
	l.once.Do(func() {
		l.allocator.mu.Lock()
		delete(l.allocator.leased, l.Index)
		l.allocator.mu.Unlock()
		if err := syscall.Flock(int(l.lockFile.Fd()), syscall.LOCK_UN); err != nil {
			releaseErr = err
		}
		if err := l.lockFile.Close(); err != nil && releaseErr == nil {
			releaseErr = err
		}
	})
	return releaseErr
}

type StateVolumeNBDConnector interface {
	Connect(ctx context.Context, socketPath, exportName, devicePath string) error
	Disconnect(ctx context.Context, devicePath string) error
}

type NBDClientConnector struct {
	Runner StateVolumeCommandRunner
}

func (c NBDClientConnector) runner() StateVolumeCommandRunner {
	if c.Runner == nil {
		return OSStateVolumeCommandRunner{}
	}
	return c.Runner
}

func (c NBDClientConnector) Connect(ctx context.Context, socketPath, exportName, devicePath string) error {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath ||
		exportName == "" || devicePath == "" || strings.ContainsAny(socketPath+exportName+devicePath, "\n\x00") {
		return fmt.Errorf("invalid NBD connection arguments")
	}
	_, err := c.runner().Run(ctx, "nbd-client", "-unix", socketPath, "-N", exportName, devicePath)
	if err != nil {
		return fmt.Errorf("connect %s to NBD export %q: %w", devicePath, exportName, err)
	}
	return nil
}

func verifyStateVolumeNBDSocket(socketPath string) error {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || filepath.Base(socketPath) != "nbd.sock" {
		return fmt.Errorf("invalid QSD NBD Unix socket path %q", socketPath)
	}
	// Linux sockaddr_un.sun_path is 108 bytes including the trailing NUL.
	if len([]byte(socketPath)) > 107 {
		return fmt.Errorf("QSD NBD Unix socket path is too long (%d bytes)", len([]byte(socketPath)))
	}
	info, err := os.Lstat(socketPath)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("QSD NBD endpoint %q is not a Unix socket", socketPath)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("QSD NBD endpoint %q has an unexpected owner", socketPath)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		return err
	}
	info, err = os.Lstat(socketPath)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
		return fmt.Errorf("QSD NBD endpoint %q does not have mode 0600", socketPath)
	}
	return nil
}

func (c NBDClientConnector) Disconnect(ctx context.Context, devicePath string) error {
	if devicePath == "" {
		return nil
	}
	_, err := c.runner().Run(ctx, "nbd-client", "-d", devicePath)
	if err != nil {
		return fmt.Errorf("disconnect NBD device %s: %w", devicePath, err)
	}
	return nil
}
