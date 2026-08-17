package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

const (
	stateVolumeQEMUJammyVersion = "1:6.2+dfsg-2ubuntu6.31"
	stateVolumeLocalNBDLimit    = 12
	stateVolumeProductionMinNBD = 64
)

func validateStateVolumeMachineIdentity(machineID string) error {
	if strings.TrimSpace(machineID) == "" {
		return fmt.Errorf("authoritative worker machine ID is required for node-global state volume capacity")
	}
	return nil
}

func stateVolumeNBDClientVersionArgs() []string { return []string{"-V"} }

func validateStateVolumeNBDBudget(total, free int, local bool) (uint32, uint32, error) {
	minimum := stateVolumeProductionMinNBD
	if local {
		minimum = stateVolumeLocalNBDLimit
	}
	if total < minimum {
		return 0, 0, fmt.Errorf("state volume NBD capacity %d is below required minimum %d", total, minimum)
	}
	return uint32(total), uint32(free), nil
}

func (s *Worker) preflightStateVolumes(ctx context.Context) (uint32, uint32, error) {
	if err := validateStateVolumeStartupCache(s.cacheManager); err != nil {
		return 0, 0, err
	}
	if err := validateStateVolumeMachineIdentity(s.machineID); err != nil {
		return 0, 0, err
	}
	if s.stateVolumeManager == nil {
		return 0, 0, fmt.Errorf("state volume manager is unavailable")
	}
	if err := s.stateVolumeManager.defaults(); err != nil {
		return 0, 0, err
	}
	if err := os.MkdirAll(stateVolumeHostRoot, 0700); err != nil {
		return 0, 0, fmt.Errorf("create state volume host root: %w", err)
	}
	if err := s.stateVolumeManager.securePathOps().Probe(stateVolumeHostRoot); err != nil {
		return 0, 0, fmt.Errorf("state volumes require openat2 dirfd path safety: %w", err)
	}
	for _, command := range []string{"qemu-storage-daemon", "qemu-img", "nbd-client", "mkfs.ext4", "fsfreeze", "mount", "umount"} {
		if _, err := exec.LookPath(command); err != nil {
			return 0, 0, fmt.Errorf("state volume dependency %s is unavailable: %w", command, err)
		}
	}
	runner := OSStateVolumeCommandRunner{}
	for _, pkg := range []string{"qemu-system-common", "qemu-utils"} {
		output, err := runner.Run(ctx, "dpkg-query", "-W", "-f=${Version}", pkg)
		if err != nil || strings.TrimSpace(string(output)) != stateVolumeQEMUJammyVersion {
			return 0, 0, fmt.Errorf("%s must be exactly %s (got %q): %w", pkg, stateVolumeQEMUJammyVersion, strings.TrimSpace(string(output)), err)
		}
	}
	if _, err := runner.Run(ctx, "qemu-img", "--version"); err != nil {
		return 0, 0, err
	}
	// Jammy's nbd-client accepts -V (also asserted by Dockerfile.worker); it
	// does not consistently accept the GNU-style --version spelling.
	if _, err := runner.Run(ctx, "nbd-client", stateVolumeNBDClientVersionArgs()...); err != nil {
		return 0, 0, err
	}
	if _, err := runner.Run(ctx, "mkfs.ext4", "-V"); err != nil {
		return 0, 0, err
	}
	if _, err := runner.Run(ctx, "fsfreeze", "--help"); err != nil {
		return 0, 0, err
	}
	if err := probeStateVolumeSparseSupport(stateVolumeHostRoot); err != nil {
		return 0, 0, err
	}
	if err := probeStateVolumeQMP(ctx, s.stateVolumeManager); err != nil {
		return 0, 0, err
	}
	if s.stateVolumeManager.NBD == nil {
		s.stateVolumeManager.NBD = &StateVolumeNBDAllocator{}
	}
	local := s.config.DebugMode || os.Getenv("BETA9_STATE_VOLUME_LOCAL") == "1"
	if local {
		s.stateVolumeManager.NBD.MaxDevices = stateVolumeLocalNBDLimit
	}
	total, free, err := s.stateVolumeManager.NBD.Capacity()
	if err != nil {
		return 0, 0, err
	}
	// Zero free slots is not a startup failure: every device may belong to a
	// journal that Reconcile can safely adopt or retire. Availability is only
	// advertised after reconciliation recomputes the node-global free count.
	return validateStateVolumeNBDBudget(total, free, local)
}

func validateStateVolumeStartupCache(manager *WorkerCacheManager) error {
	if manager == nil || manager.client == nil || manager.ContentReporter() == nil {
		return fmt.Errorf("state volumes require a live content cache and required-content reporter")
	}
	reporter := manager.ContentReporter()
	if reporter.eventRepo == nil || !reporter.eventRepo.HasDurableScopedStateSink() {
		return fmt.Errorf("state volumes require a durable scoped required-content event sink")
	}
	return nil
}

func probeStateVolumeSparseSupport(root string) error {
	file, err := os.CreateTemp(root, ".sparse-probe-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer os.Remove(path)
	const logicalSize = int64(64 << 20)
	if err := file.Truncate(logicalSize); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.WriteAt(make([]byte, 4096), logicalSize-4096); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	_ = file.Close()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int64(stat.Blocks)*512 >= logicalSize/4 {
		return fmt.Errorf("state volume host filesystem does not preserve sparse files")
	}
	return nil
}

func probeStateVolumeQMP(ctx context.Context, manager *StateVolumeManager) error {
	if err := manager.defaults(); err != nil {
		return err
	}
	runtimeDir, err := os.MkdirTemp(stateVolumeHostRoot, ".qsd-preflight-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(runtimeDir)
	qmpSocket := filepath.Join(runtimeDir, "qmp.sock")
	nbdSocket := filepath.Join(runtimeDir, "nbd.sock")
	args := []string{
		"--pidfile", filepath.Join(runtimeDir, "qsd.pid"),
		"--chardev", "socket,path=" + qmpSocket + ",server=on,wait=off,id=qmp0",
		"--monitor", "chardev=qmp0",
		"--nbd-server", "addr.type=unix,addr.path=" + nbdSocket,
	}
	process, err := manager.Launcher.Start(args, nil, filepath.Join(runtimeDir, "qsd.log"))
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	qmp, err := waitForStateVolumeQMP(probeCtx, manager.QMPDialer, qmpSocket)
	if err != nil {
		_ = process.Kill()
		return err
	}
	defer qmp.Close()
	if err := qmp.ProbeSnapshotSupport(probeCtx); err != nil {
		_ = process.Kill()
		return fmt.Errorf("QSD lacks required transaction/blockdev-snapshot-sync commands: %w", err)
	}
	if err := verifyStateVolumeNBDSocket(nbdSocket); err != nil {
		_ = process.Kill()
		return fmt.Errorf("QSD Unix NBD endpoint is unavailable: %w", err)
	}
	if err := qmp.Quit(probeCtx); err != nil {
		_ = process.Kill()
		return err
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := process.Wait(waitCtx); err != nil {
		_ = process.Kill()
		return err
	}
	return nil
}

func (s *Worker) advertiseStateVolumeCapacity(ctx context.Context, total, free uint32) error {
	response, err := s.workerRepoClient.SetWorkerStateVolumeCapacity(ctx, &pb.SetWorkerStateVolumeCapacityRequest{
		WorkerId: s.workerId, WorkerInstanceId: s.workerInstanceId, MachineId: s.machineID,
		TotalNbdDevices: total, FreeNbdDevices: free,
	})
	if err != nil {
		return err
	}
	if response == nil || !response.Ok {
		message := "worker state volume capacity was rejected"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return fmt.Errorf("%s", message)
	}
	return nil
}
