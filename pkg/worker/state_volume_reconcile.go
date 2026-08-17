package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Reconcile enumerates the node-persistent journal before the worker becomes
// schedulable. It adopts only a complete, exactly mounted group owned by the
// same QSD process identity. Ambiguous mount/NBD/process combinations fail the
// worker closed; an active NBD is never stolen or detached from an untrusted
// process.
func (m *StateVolumeManager) Reconcile(ctx context.Context) error {
	if err := m.defaults(); err != nil {
		return err
	}
	journals, err := m.Journals.List()
	if err != nil {
		return err
	}
	for _, journal := range journals {
		// State roots may be shared by multiple worker slots. A live foreign
		// owner's journal is not this worker's recovery obligation and must not
		// block its startup. Replacement recovery is scoped to the same stable
		// worker ID and node, with the process-instance epoch checked below.
		if journal.Release != nil {
			if m.StorageNodeID != "" && journal.StorageNodeID != m.StorageNodeID {
				continue
			}
		} else if m.WorkerID != "" && (journal.WorkerID != m.WorkerID || journal.StorageNodeID != m.StorageNodeID) {
			continue
		}
		if err := m.reconcileJournal(ctx, journal); err != nil {
			return fmt.Errorf("reconcile state volume group %q: %w", journal.ContainerID, err)
		}
	}
	return nil
}

func (m *StateVolumeManager) reconcileJournal(ctx context.Context, journal StateVolumeJournal) error {
	if journal.Release != nil {
		return m.reconcileReleaseJournal(ctx, journal)
	}
	if err := m.validateJournalPaths(journal); err != nil {
		quarantineErr := m.Journals.Quarantine(journal.ContainerID)
		return errors.Join(fmt.Errorf("unsafe state volume journal paths: %w", err), quarantineErr)
	}
	if stateVolumeJournalPreparationPhase(journal.Phase) {
		return m.quarantineInitializationJournal(journal)
	}
	trustedProcess, err := m.journalProcessMatches(journal)
	if err != nil {
		return err
	}
	allMounted, anyMounted, anyBusy, err := m.inspectJournalDevices(journal)
	if err != nil {
		return err
	}
	if !trustedProcess {
		if journal.Recovery != nil && journal.Recovery.Mode == string(StateSnapshotModeTerminal) && !anyMounted && !anyBusy {
			switch journal.Phase {
			case "terminal-quiesced", "pivot-indeterminate", "pivoted", "terminal-detach-intent", "detached-pending":
				return m.adoptOfflineTerminalRecoveryJournal(ctx, journal)
			case "terminal-rollback-intent":
				// A durable determinate rollback can never be published. Preserve it
				// as the existing pre-pivot failure/quarantine obligation.
				journal.Phase = "prepivot-quarantine"
				if err := m.Journals.Save(journal); err != nil {
					return err
				}
				return m.adoptOfflinePrePivotRecoveryJournal(journal)
			case "recovery-bound", "prepivot-quarantine":
				// No generation is publishable: the source died before the
				// all-writers-stopped consistency proof. Preserve the exact
				// journal/proof as a cleanup obligation so the worker layer can
				// claim+fail the armed DB escrow before quarantining local state.
				return m.adoptOfflinePrePivotRecoveryJournal(journal)
			case "running", "pivot-intent", "pivot-frozen":
				return fmt.Errorf("terminal recovery journal phase %q has no durable all-writers-stopped consistency proof", journal.Phase)
			case "writers-resumed-indeterminate":
				return fmt.Errorf("terminal recovery journal was tainted by post-pivot writer resume and is not publishable")
			}
		}
		if journal.Phase == "terminal-committed" && !anyMounted && !anyBusy {
			return m.adoptDetachedTerminalCommittedJournal(journal)
		}
		if (journal.Phase == "detached-pending" || journal.Phase == "terminal-detach-intent") && !anyMounted && !anyBusy {
			return m.adoptDetachedPendingJournal(journal)
		}
		if anyMounted || anyBusy {
			return fmt.Errorf("journal process identity is stale while NBD or mount state remains active")
		}
		return m.retireDeadJournal(journal)
	}
	if m.WorkerInstanceID != "" && (journal.WorkerID != m.WorkerID || journal.WorkerInstanceID == "" || journal.WorkerInstanceID != m.WorkerInstanceID || journal.StorageNodeID != m.StorageNodeID) {
		return fmt.Errorf("refuse to adopt live state volume QSD owned by worker epoch %q/%q on node %q", journal.WorkerID, journal.WorkerInstanceID, journal.StorageNodeID)
	}
	if anyMounted && !allMounted {
		return m.cleanupTrustedPartiallyMountedJournal(ctx, journal)
	}
	if !allMounted {
		return m.cleanupTrustedUnmountedJournal(ctx, journal)
	}
	if !anyBusy {
		return fmt.Errorf("journal mounts are active but their NBD devices are not connected")
	}
	return m.adoptJournalGroup(ctx, journal)
}

// adoptOfflinePrePivotRecoveryJournal retains an armed-before-quiesce crash as
// a cleanup obligation only. It never constructs a pending receipt and never
// publishes the active qcow2 layers. The worker must first terminalize the
// exact server-side escrow, then call QuarantinePrePivotRecovery.
func (m *StateVolumeManager) adoptOfflinePrePivotRecoveryJournal(journal StateVolumeJournal) error {
	if journal.Recovery == nil || journal.Recovery.Mode != string(StateSnapshotModeTerminal) ||
		journal.Recovery.OperationID == "" || (journal.Phase != "recovery-bound" && journal.Phase != "prepivot-quarantine") {
		return fmt.Errorf("offline pre-pivot recovery journal has no exact terminal operation identity")
	}
	group := stateVolumeGroupFromJournal(journal)
	group.process = nil
	group.qmp = nil
	group.pending = nil
	for _, volume := range group.volumes {
		volume.lease = nil
		volume.connected = false
		volume.mounted = false
		volume.frozen = false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.groups[journal.ContainerID]; exists {
		return ErrStateVolumeGroupExists
	}
	m.groups[journal.ContainerID] = group
	return nil
}

// cleanupTrustedPartiallyMountedJournal is the only recovery path for a crash
// between member attaches. The exact same worker process epoch, QSD
// executable/start-time, graph, mount source, and NBD identities are already
// authenticated by the caller. It acquires every device flock before mutation,
// unmounts the mounted subset, disconnects every export, stops the exact QSD,
// and quarantines only container-private writable graphs. No partially
// prepared group is ever adopted as Ready.
func (m *StateVolumeManager) cleanupTrustedPartiallyMountedJournal(ctx context.Context, journal StateVolumeJournal) (retErr error) {
	qmp, err := m.QMPDialer.Dial(ctx, journal.QMPSocket)
	if err != nil {
		return fmt.Errorf("dial partial-start QMP: %w", err)
	}
	defer qmp.Close()
	if err := qmp.ProbeSnapshotSupport(ctx); err != nil {
		return err
	}
	group := stateVolumeGroupFromJournal(journal)
	group.qmp = qmp
	group.process = newAdoptedStateVolumeProcess(journal.QSDPID, journal.QSDExecutable, journal.QSDStartTime, m.ProcessIdentity)
	for _, volume := range group.volumes {
		if verifier, ok := qmp.(stateVolumeRuntimeGraphVerifier); ok {
			err = verifier.VerifyStateVolumeRuntimeGraph(ctx, volume)
		} else {
			err = authenticateStateVolumeRuntimeGraph(ctx, qmp, volume)
		}
		if err != nil {
			return fmt.Errorf("authenticate partial-start export for volume %q: %w", volume.spec.ID, err)
		}
	}

	adopted := make([]*StateVolumeNBDLease, len(group.volumes))
	defer func() {
		retain := false
		for _, volume := range group.volumes {
			if volume.lease != nil {
				retain = retErr != nil
				break
			}
		}
		if retain {
			// A kernel detach or mount postcondition is still ambiguous. Keep the
			// exact adopted flocks reachable by this manager; releasing them in a
			// defer would open a device-steal window below a live QSD/mount.
			m.mu.Lock()
			if _, exists := m.groups[group.containerID]; !exists {
				m.groups[group.containerID] = group
			}
			m.mu.Unlock()
			return
		}
		for _, lease := range adopted {
			if lease != nil {
				_ = lease.Release()
			}
		}
	}()
	for index, volume := range group.volumes {
		sysRoot, _, _ := m.NBD.normalizedRoots()
		busy, busyErr := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, filepath.Base(volume.devicePath)))
		if busyErr != nil {
			return busyErr
		}
		mounted, mountPath, mountErr := m.NBD.deviceMount(volume.devicePath)
		if mountErr != nil {
			return mountErr
		}
		if !busy && !mounted {
			continue
		}
		if mounted && filepath.Clean(mountPath) != filepath.Clean(volume.spec.MountPath) {
			return fmt.Errorf("partial-start NBD %s is mounted at an unexpected path %q", volume.devicePath, mountPath)
		}
		lease, adoptErr := m.NBD.Adopt(volume.devicePath)
		if adoptErr != nil {
			return fmt.Errorf("lock partial-start NBD %s: %w", volume.devicePath, adoptErr)
		}
		adopted[index] = lease
		volume.lease = lease
		volume.connected = busy
		volume.mounted = mounted
	}
	for index := len(group.volumes) - 1; index >= 0; index-- {
		volume := group.volumes[index]
		if !volume.mounted {
			continue
		}
		if err := m.Mounts.Unmount(ctx, volume.spec.MountPath); err != nil {
			return fmt.Errorf("unmount partial-start volume %q: %w", volume.spec.ID, err)
		}
		if err := m.NBD.WaitUnmounted(ctx, volume.devicePath, volume.spec.MountPath); err != nil {
			return fmt.Errorf("verify partial-start unmount for volume %q: %w", volume.spec.ID, err)
		}
		volume.mounted = false
	}
	for index := len(group.volumes) - 1; index >= 0; index-- {
		volume := group.volumes[index]
		if volume.lease == nil {
			continue
		}
		if err := m.disconnectStateVolumeLease(ctx, volume); err != nil {
			return fmt.Errorf("clean partial-start volume %q: %w", volume.spec.ID, err)
		}
		adopted[index] = nil
	}
	if err := qmp.Quit(ctx); err != nil {
		return fmt.Errorf("stop partial-start QSD: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	waitErr := group.process.Wait(waitCtx)
	cancel()
	if waitErr != nil {
		if err := group.process.Kill(); err != nil {
			return fmt.Errorf("kill partial-start QSD: %w", err)
		}
		killCtx, killCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer killCancel()
		if err := group.process.Wait(killCtx); err != nil {
			return fmt.Errorf("wait for partial-start QSD exit: %w", err)
		}
	}
	return m.quarantinePartialStartJournal(group, journal)
}

func (m *StateVolumeManager) quarantinePartialStartJournal(group *stateVolumeGroup, journal StateVolumeJournal) error {
	quarantineRoot := filepath.Join(m.StateRoot, "quarantine",
		stateVolumeToken("partial-", journal.ContainerID+"\x00"+time.Now().UTC().String()))
	secure := m.securePathOps()
	if err := secure.MkdirAll(quarantineRoot, 0700); err != nil {
		return err
	}
	for _, volume := range group.volumes {
		if !volume.spec.ReadOnly && volume.spec.BackingDir != "" {
			if info, err := os.Lstat(volume.spec.BackingDir); err == nil {
				if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("refuse to quarantine partial-start non-directory graph %q", volume.spec.BackingDir)
				}
				destination := filepath.Join(quarantineRoot, stateVolumeToken("volume-", volume.spec.ID))
				if err := secure.Rename(volume.spec.BackingDir, destination, stateVolumeSecureDirectory, false); err != nil {
					return err
				}
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		if err := secure.Remove(volume.spec.MountPath, stateVolumeSecureDirectory); err != nil {
			return err
		}
	}
	if group.runtimeDir != "" {
		if err := secure.Rename(group.runtimeDir, filepath.Join(quarantineRoot, "runtime"), stateVolumeSecureDirectory, false); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return m.Journals.Quarantine(journal.ContainerID)
}

func (m *StateVolumeManager) reconcileReleaseJournal(ctx context.Context, journal StateVolumeJournal) error {
	if journal.Release == nil {
		return fmt.Errorf("state-volume release journal has no release envelope")
	}
	if m.StorageNodeID != "" && journal.StorageNodeID != m.StorageNodeID {
		return fmt.Errorf("state-volume release belongs to storage node %q, not %q", journal.StorageNodeID, m.StorageNodeID)
	}
	if journal.WorkerID == "" || journal.WorkerInstanceID == "" || journal.Release.SourceWorkerID == "" ||
		journal.Release.SourceWorkerInstanceID == "" || journal.Release.StorageNodeID != journal.StorageNodeID {
		return fmt.Errorf("state-volume release journal owner identity is incomplete or inconsistent")
	}
	if journal.Release.ReleaseClaimGeneration == 0 &&
		(journal.Release.SourceWorkerID != journal.WorkerID || journal.Release.SourceWorkerInstanceID != journal.WorkerInstanceID) {
		return fmt.Errorf("unclaimed state-volume release journal is not owned by its immutable source process")
	}
	if len(journal.Volumes) == 0 {
		// The graph was never installed, so the journal names no path or kernel
		// resource. A replacement Claim still proves source death, lease settling,
		// and exact repository tuples before any deletion.
		if journal.Phase == "release-armed" || journal.Phase == "release-detach-intent" {
			journal.Release.LocalCleanupVerified = true
			journal.Phase = "release-intent"
			return m.Journals.Save(journal)
		}
		return nil
	}
	if err := m.validateJournalPaths(journal); err != nil {
		quarantineErr := m.Journals.Quarantine(journal.ContainerID)
		return errors.Join(fmt.Errorf("unsafe state-volume release journal paths: %w", err), quarantineErr)
	}
	trustedProcess, err := m.journalProcessMatches(journal)
	if err != nil {
		return err
	}
	_, anyMounted, anyBusy, err := m.inspectJournalDevices(journal)
	if err != nil {
		return err
	}
	if trustedProcess || anyMounted || anyBusy {
		// Worker containers do not share mount/PID namespaces. Never adopt or
		// detach a live foreign owner based on a journal; remain unavailable until
		// the old process is authoritatively dead and sysfs is stably clear.
		return fmt.Errorf("state-volume release owner still has active QSD/NBD/mount resources")
	}
	journal.QSDPID = 0
	journal.QSDExecutable = ""
	journal.QSDStartTime = 0
	if journal.Phase == "release-detach-intent" || journal.Phase == "release-armed" {
		journal.Release.LocalCleanupVerified = true
		journal.Phase = "release-intent"
		if err := m.Journals.Save(journal); err != nil {
			return err
		}
	}
	return m.adoptDetachedReleaseJournal(journal)
}

func (m *StateVolumeManager) adoptDetachedReleaseJournal(journal StateVolumeJournal) error {
	if journal.Release == nil || !journal.Release.LocalCleanupVerified ||
		(journal.Phase != "release-intent" && journal.Phase != "release-completed") {
		return fmt.Errorf("state-volume release journal is not safely detached")
	}
	group := stateVolumeGroupFromJournal(journal)
	group.process = nil
	group.qmp = nil
	for _, volume := range group.volumes {
		volume.lease = nil
		volume.connected = false
		volume.mounted = false
		volume.frozen = false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.groups[journal.ContainerID]; exists {
		return ErrStateVolumeGroupExists
	}
	m.groups[journal.ContainerID] = group
	return nil
}

func stateVolumeJournalPathUnder(root, path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("path %q is not canonical and absolute", path)
	}
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path %q escapes trusted root %q", cleanPath, cleanRoot)
	}
	canonicalRoot, err := canonicalStateVolumePath(root)
	if err != nil {
		return "", err
	}
	canonicalPath, err := canonicalStateVolumePath(path)
	if err != nil {
		return "", err
	}
	expectedCanonical := filepath.Join(canonicalRoot, rel)
	if canonicalPath != expectedCanonical {
		return "", fmt.Errorf("path %q traverses a symlink to %q", path, canonicalPath)
	}
	return cleanPath, nil
}

func (m *StateVolumeManager) validateJournalPaths(journal StateVolumeJournal) error {
	if m.StrictLayout {
		if journal.WorkerID == "" || journal.WorkerInstanceID == "" || journal.StorageNodeID == "" {
			return fmt.Errorf("state volume journal has no authenticated worker owner epoch")
		}
		if m.StorageNodeID == "" || journal.StorageNodeID != m.StorageNodeID {
			return fmt.Errorf("state volume journal storage node %q does not match worker node %q", journal.StorageNodeID, m.StorageNodeID)
		}
	}
	runtimeDir := filepath.Join(m.RuntimeRoot, stateVolumeToken("container-", journal.ContainerID))
	expectedQMP := filepath.Join(runtimeDir, "qmp.sock")
	expectedNBD := filepath.Join(runtimeDir, "nbd.sock")
	qmp, err := stateVolumeJournalPathUnder(m.RuntimeRoot, journal.QMPSocket)
	if err != nil || qmp != expectedQMP {
		if err == nil {
			err = fmt.Errorf("QMP socket %q does not match container runtime path %q", qmp, expectedQMP)
		}
		return err
	}
	nbd, err := stateVolumeJournalPathUnder(m.RuntimeRoot, journal.NBDSocket)
	if err != nil || nbd != expectedNBD {
		if err == nil {
			err = fmt.Errorf("NBD socket %q does not match container runtime path %q", nbd, expectedNBD)
		}
		return err
	}
	_, devRoot, _ := m.NBD.normalizedRoots()
	devicePattern := regexp.MustCompile(`^nbd[0-9]+$`)
	for _, volume := range journal.Volumes {
		volumeToken := stateVolumeToken("volume-", volume.ID)
		containerToken := stateVolumeToken("container-", journal.ContainerID)
		backing, err := stateVolumeJournalPathUnder(m.StateRoot, volume.BackingDir)
		if err != nil {
			return fmt.Errorf("volume %q backing: %w", volume.ID, err)
		}
		mountPath, err := stateVolumeJournalPathUnder(m.StateRoot, volume.MountPath)
		if err != nil {
			return fmt.Errorf("volume %q mount: %w", volume.ID, err)
		}
		if m.StrictLayout {
			globalBacking := filepath.Join(m.StateRoot, "volumes", volumeToken, "graph")
			containerBacking := filepath.Join(m.StateRoot, "containers", containerToken, "volumes", volumeToken)
			if backing != globalBacking && backing != containerBacking {
				return fmt.Errorf("volume %q backing %q is outside its exact graph namespace", volume.ID, backing)
			}
			expectedMount := filepath.Join(m.StateRoot, "mounts", containerToken, volumeToken)
			if mountPath != expectedMount {
				return fmt.Errorf("volume %q mount %q does not match %q", volume.ID, mountPath, expectedMount)
			}
		}
		activeRoot := volume.BackingDir
		if volume.ReadOnly {
			activeRoot = m.StateRoot
			if m.StrictLayout {
				activeRoot = filepath.Join(m.StateRoot, "block-cache")
			}
		}
		if _, err := stateVolumeJournalPathUnder(activeRoot, volume.ActiveLayerPath); err != nil {
			return fmt.Errorf("volume %q active layer: %w", volume.ID, err)
		}
		for label, backingPath := range map[string]string{
			"active backing":  volume.ActiveBackingPath,
			"pending backing": volume.PendingBackingPath,
		} {
			if backingPath == "" {
				continue
			}
			if _, err := stateVolumeJournalPathUnder(activeRoot, backingPath); err != nil {
				cacheRoot := filepath.Join(m.StateRoot, "block-cache")
				if _, cacheErr := stateVolumeJournalPathUnder(cacheRoot, backingPath); cacheErr != nil {
					return fmt.Errorf("volume %q %s is outside its graph and authenticated block cache: %w", volume.ID, label, err)
				}
			}
		}
		for label, path := range map[string]string{
			"pending layer": volume.PendingLayerPath,
			"pivot layer":   volume.PivotLayerPath,
		} {
			if path == "" {
				continue
			}
			if _, err := stateVolumeJournalPathUnder(volume.BackingDir, path); err != nil {
				return fmt.Errorf("volume %q %s: %w", volume.ID, label, err)
			}
		}
		if volume.DevicePath == "" && stateVolumeJournalPreparationPhase(journal.Phase) {
			continue
		}
		deviceBase := filepath.Base(volume.DevicePath)
		if !devicePattern.MatchString(deviceBase) || filepath.Clean(volume.DevicePath) != filepath.Join(devRoot, deviceBase) {
			return fmt.Errorf("volume %q device %q is not an allocator NBD node", volume.ID, volume.DevicePath)
		}
		// Authenticate the held kernel identity before any reconciliation path
		// may inspect mounts, acquire the node-global lock, disconnect, or move
		// graph state. A regular/character/rebound node named /dev/nbdN is not
		// an NBD device and must be treated as forged journal data.
		if err := m.NBD.kernel().ValidateDevice(m.NBD.sysDevicePath(volume.DevicePath), volume.DevicePath); err != nil {
			return fmt.Errorf("volume %q device %q: %w", volume.ID, volume.DevicePath, err)
		}
	}
	return nil
}

// quarantineInitializationJournal handles a SIGKILL before QSD exec. No NBD
// may be active in this phase. Any freshly initialized layer or restored
// writable child and runtime tree are moved into a recoverable,
// container-scoped quarantine; immutable source/cache layers are never moved.
// A later explicit reaper can remove authenticated quarantine entries after
// the replacement group is healthy.
func (m *StateVolumeManager) quarantineInitializationJournal(journal StateVolumeJournal) error {
	quarantineRoot := filepath.Join(m.StateRoot, "quarantine", stateVolumeToken("init-", journal.ContainerID+"\x00"+time.Now().UTC().String()))
	secure := m.securePathOps()
	if err := secure.MkdirAll(quarantineRoot, 0700); err != nil {
		return err
	}
	for _, volume := range journal.Volumes {
		if volume.DevicePath != "" {
			mounted, _, err := m.NBD.deviceMount(volume.DevicePath)
			if err != nil {
				return err
			}
			sysRoot, _, _ := m.NBD.normalizedRoots()
			busy, err := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, filepath.Base(volume.DevicePath)))
			if err != nil {
				return err
			}
			if mounted || busy {
				return fmt.Errorf("initialization journal NBD %s unexpectedly became active", volume.DevicePath)
			}
		}
		if !volume.Initialize && !volume.CreateLayer {
			continue
		}
		info, err := os.Lstat(volume.ActiveLayerPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse to quarantine non-regular initialization layer %q", volume.ActiveLayerPath)
		}
		destination := filepath.Join(quarantineRoot, stateVolumeToken("volume-", volume.ID)+".qcow2")
		if err := secure.Rename(volume.ActiveLayerPath, destination, stateVolumeSecureRegular, false); err != nil {
			return fmt.Errorf("quarantine initialization layer %q: %w", volume.ActiveLayerPath, err)
		}
	}
	runtimeDir := filepath.Join(m.RuntimeRoot, stateVolumeToken("container-", journal.ContainerID))
	if info, err := os.Lstat(runtimeDir); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse to quarantine non-directory initialization runtime %q", runtimeDir)
		}
		if err := secure.Rename(runtimeDir, filepath.Join(quarantineRoot, "runtime"), stateVolumeSecureDirectory, false); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return m.Journals.Quarantine(journal.ContainerID)
}

// adoptOfflineTerminalRecoveryJournal handles pod replacement without
// pretending the old QSD or mount namespace can be adopted. Once sysfs and
// findmnt prove that every old device is quiescent, the qcow2 files are an
// offline crash-consistent boundary. A pre-pivot running journal is sealed as
// the already-escrowed next generation; pivot journals already contain that
// exact immutable receipt. No active device is ever stolen or reconnected.
func (m *StateVolumeManager) adoptOfflineTerminalRecoveryJournal(ctx context.Context, journal StateVolumeJournal) error {
	if journal.Recovery == nil || journal.Recovery.Mode != string(StateSnapshotModeTerminal) || journal.Recovery.OperationID == "" {
		return fmt.Errorf("offline terminal recovery journal has no exact operation identity")
	}
	journal.QSDPID = 0
	journal.QSDExecutable = ""
	journal.QSDStartTime = 0
	journal.OperationID = journal.Recovery.OperationID
	receipt := pendingReceiptFromJournal(journal)
	if receipt == nil {
		return fmt.Errorf("offline terminal recovery journal has no complete planned receipt")
	}
	for _, generation := range receipt.Generations {
		if generation.Reused {
			continue
		}
		if err := m.Images.Check(ctx, generation.LayerPath); err != nil {
			return fmt.Errorf("check offline pending layer for volume %q: %w", generation.VolumeID, err)
		}
		info, err := m.Images.Info(ctx, generation.LayerPath)
		if err != nil {
			return fmt.Errorf("inspect offline pending layer for volume %q: %w", generation.VolumeID, err)
		}
		if err := validateStateVolumeImageInfo(info, generation.VirtualSizeBytes, generation.BackingPath); err != nil {
			return fmt.Errorf("validate offline pending layer for volume %q: %w", generation.VolumeID, err)
		}
	}
	journal.Phase = "detached-pending"
	if err := m.Journals.Save(journal); err != nil {
		return err
	}
	return m.adoptDetachedPendingJournal(journal)
}

func (m *StateVolumeManager) adoptDetachedTerminalCommittedJournal(journal StateVolumeJournal) error {
	if journal.Recovery == nil || journal.OperationID == "" || journal.Recovery.OperationID != journal.OperationID {
		return fmt.Errorf("terminal committed journal has no exact recovery identity")
	}
	group := stateVolumeGroupFromJournal(journal)
	for _, volume := range group.volumes {
		volume.lease = nil
		volume.connected = false
		volume.mounted = false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.groups[journal.ContainerID]; exists {
		return ErrStateVolumeGroupExists
	}
	m.groups[journal.ContainerID] = group
	return nil
}

func (m *StateVolumeManager) adoptDetachedPendingJournal(journal StateVolumeJournal) error {
	group := stateVolumeGroupFromJournal(journal)
	group.pending = pendingReceiptFromJournal(journal)
	if group.pending == nil {
		return fmt.Errorf("detached pending journal has no complete receipt")
	}
	for _, volume := range group.volumes {
		volume.lease = nil
		volume.connected = false
		volume.mounted = false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.groups[journal.ContainerID]; exists {
		return ErrStateVolumeGroupExists
	}
	m.groups[journal.ContainerID] = group
	return nil
}

func (m *StateVolumeManager) journalProcessMatches(journal StateVolumeJournal) (bool, error) {
	if journal.QSDPID <= 0 || journal.QSDExecutable == "" || journal.QSDStartTime == 0 {
		return false, nil
	}
	executable, startTime, err := m.ProcessIdentity(journal.QSDPID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, nil
	}
	return executable == journal.QSDExecutable && startTime == journal.QSDStartTime, nil
}

func (m *StateVolumeManager) inspectJournalDevices(journal StateVolumeJournal) (allMounted, anyMounted, anyBusy bool, retErr error) {
	allMounted = true
	sysRoot, _, _ := m.NBD.normalizedRoots()
	for _, volume := range journal.Volumes {
		mounted, mountPath, err := m.NBD.deviceMount(volume.DevicePath)
		if err != nil {
			return false, false, false, err
		}
		if mounted && filepath.Clean(mountPath) != filepath.Clean(volume.MountPath) {
			return false, false, false, fmt.Errorf("NBD %s is mounted at %q, not journal path %q", volume.DevicePath, mountPath, volume.MountPath)
		}
		allMounted = allMounted && mounted
		anyMounted = anyMounted || mounted
		busy, err := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, filepath.Base(volume.DevicePath)))
		if err != nil {
			return false, false, false, err
		}
		if mounted && !busy {
			return false, false, false, fmt.Errorf("mounted journal NBD %s has no active kernel connection", volume.DevicePath)
		}
		anyBusy = anyBusy || busy
	}
	return allMounted, anyMounted, anyBusy, nil
}

func (m *StateVolumeManager) cleanupTrustedUnmountedJournal(ctx context.Context, journal StateVolumeJournal) (retErr error) {
	qmp, err := m.QMPDialer.Dial(ctx, journal.QMPSocket)
	if err != nil {
		return fmt.Errorf("trusted QSD has no reachable QMP socket: %w", err)
	}
	defer qmp.Close()
	group := stateVolumeGroupFromJournal(journal)
	group.qmp = qmp
	group.process = newAdoptedStateVolumeProcess(journal.QSDPID, journal.QSDExecutable, journal.QSDStartTime, m.ProcessIdentity)
	defer func() {
		if retErr == nil {
			return
		}
		for _, volume := range group.volumes {
			if volume.lease == nil {
				continue
			}
			m.mu.Lock()
			if _, exists := m.groups[group.containerID]; !exists {
				m.groups[group.containerID] = group
			}
			m.mu.Unlock()
			return
		}
	}()
	for _, volume := range group.volumes {
		sysRoot, _, _ := m.NBD.normalizedRoots()
		busy, err := stateVolumeNBDDeviceBusy(filepath.Join(sysRoot, filepath.Base(volume.devicePath)))
		if err != nil {
			return err
		}
		if !busy {
			continue
		}
		lease, err := m.NBD.Adopt(volume.devicePath)
		if err != nil {
			return err
		}
		volume.lease = lease
		volume.connected = true
		if err := m.disconnectStateVolumeLease(ctx, volume); err != nil {
			return err
		}
	}
	if err := qmp.Quit(ctx); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	waitErr := group.process.Wait(waitCtx)
	cancel()
	if waitErr != nil {
		if err := group.process.Kill(); err != nil {
			return fmt.Errorf("stop trusted unmounted QSD: %w", err)
		}
		killWaitCtx, killWaitCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer killWaitCancel()
		if err := group.process.Wait(killWaitCtx); err != nil {
			return fmt.Errorf("wait for trusted QSD exit: %w", err)
		}
	}
	return m.retireDeadJournal(journal)
}

// QuarantinePrePivotRecovery completes the local half of an armed terminal
// operation that was authoritatively failed after its source process died
// before quiesce/freeze/pivot. The operation has no publishable receipt. The
// deterministic quarantine phase makes a crash between member moves
// restartable; the journal is retained until every writable graph and runtime
// path is outside the active namespace.
func (m *StateVolumeManager) QuarantinePrePivotRecovery(ctx context.Context, containerID, operationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.recovery == nil || group.recovery.OperationID != operationID || group.pending != nil || group.process != nil || group.qmp != nil {
		return fmt.Errorf("state volume pre-pivot cleanup obligation %q is not safely offline", operationID)
	}
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return err
	}
	if journal.Recovery == nil || journal.Recovery.OperationID != operationID ||
		(journal.Phase != "recovery-bound" && journal.Phase != "prepivot-quarantine") {
		return fmt.Errorf("state volume pre-pivot cleanup journal does not match operation %q", operationID)
	}
	if err := m.validateJournalPaths(journal); err != nil {
		return err
	}
	trustedProcess, err := m.journalProcessMatches(journal)
	if err != nil {
		return err
	}
	_, anyMounted, anyBusy, err := m.inspectJournalDevices(journal)
	if err != nil {
		return err
	}
	if trustedProcess || anyMounted || anyBusy {
		return fmt.Errorf("state volume pre-pivot cleanup still has a live QSD, mount, or NBD")
	}
	if journal.Phase != "prepivot-quarantine" {
		journal.Phase = "prepivot-quarantine"
		if err := m.Journals.Save(journal); err != nil {
			return err
		}
	}

	secure := m.securePathOps()
	quarantineRoot := filepath.Join(m.StateRoot, "quarantine", stateVolumeToken("prepivot-", containerID+"\x00"+operationID))
	if err := secure.MkdirAll(quarantineRoot, 0700); err != nil {
		return err
	}
	for _, volume := range journal.Volumes {
		if !volume.ReadOnly {
			destination := filepath.Join(quarantineRoot, stateVolumeToken("volume-", volume.ID))
			if info, statErr := os.Lstat(volume.BackingDir); statErr == nil {
				if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("refuse to quarantine non-directory pre-pivot graph %q", volume.BackingDir)
				}
				if err := secure.Rename(volume.BackingDir, destination, stateVolumeSecureDirectory, false); err != nil {
					return fmt.Errorf("quarantine pre-pivot graph for volume %q: %w", volume.ID, err)
				}
			} else if os.IsNotExist(statErr) {
				info, destinationErr := os.Lstat(destination)
				if destinationErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("pre-pivot graph for volume %q is absent from both active and quarantine namespaces", volume.ID)
				}
			} else {
				return statErr
			}
		}
		if err := secure.Remove(volume.MountPath, stateVolumeSecureDirectory); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove pre-pivot mount path for volume %q: %w", volume.ID, err)
		}
	}
	runtimeDir := filepath.Join(m.RuntimeRoot, stateVolumeToken("container-", containerID))
	runtimeDestination := filepath.Join(quarantineRoot, "runtime")
	if info, statErr := os.Lstat(runtimeDir); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse to quarantine non-directory pre-pivot runtime %q", runtimeDir)
		}
		if err := secure.Rename(runtimeDir, runtimeDestination, stateVolumeSecureDirectory, false); err != nil {
			return fmt.Errorf("quarantine pre-pivot runtime: %w", err)
		}
	} else if os.IsNotExist(statErr) {
		if info, destinationErr := os.Lstat(runtimeDestination); destinationErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("pre-pivot runtime is absent from both active and quarantine namespaces")
		}
	} else {
		return statErr
	}
	if err := m.Journals.Quarantine(containerID); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.groups, containerID)
	m.mu.Unlock()
	return nil
}

func (m *StateVolumeManager) retireDeadJournal(journal StateVolumeJournal) error {
	group := stateVolumeGroupFromJournal(journal)
	if err := m.retireGroupTransientPaths(group); err != nil {
		return err
	}
	return m.Journals.Remove(journal.ContainerID)
}

func (m *StateVolumeManager) adoptJournalGroup(ctx context.Context, journal StateVolumeJournal) (retErr error) {
	qmp, err := m.QMPDialer.Dial(ctx, journal.QMPSocket)
	if err != nil {
		return fmt.Errorf("dial journal QMP: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = qmp.Close()
		}
	}()
	if err := qmp.ProbeSnapshotSupport(ctx); err != nil {
		return err
	}
	group := stateVolumeGroupFromJournal(journal)
	group.qmp = qmp
	group.process = newAdoptedStateVolumeProcess(journal.QSDPID, journal.QSDExecutable, journal.QSDStartTime, m.ProcessIdentity)
	for _, volume := range group.volumes {
		lease, err := m.NBD.Adopt(volume.lease.DevicePath)
		if err != nil {
			for _, adopted := range group.volumes {
				if adopted.connected && adopted.lease != nil {
					_ = adopted.lease.Release()
				}
			}
			return err
		}
		volume.lease = lease
		volume.connected = true
		volume.mounted = true
		if err := m.NBD.WaitConnected(ctx, volume.devicePath, volume.spec.SizeBytes); err != nil {
			_ = lease.Release()
			return fmt.Errorf("verify adopted NBD for volume %q: %w", volume.spec.ID, err)
		}
		if err := m.NBD.VerifyMounted(volume.devicePath, volume.spec.MountPath, volume.spec.ReadOnly); err != nil {
			_ = lease.Release()
			return fmt.Errorf("verify adopted mount for volume %q: %w", volume.spec.ID, err)
		}
		if verifier, ok := qmp.(stateVolumeRuntimeGraphVerifier); ok {
			err = verifier.VerifyStateVolumeRuntimeGraph(ctx, volume)
		} else {
			err = authenticateStateVolumeRuntimeGraph(ctx, qmp, volume)
		}
		if err != nil {
			_ = lease.Release()
			return fmt.Errorf("authenticate adopted export for volume %q: %w", volume.spec.ID, err)
		}
	}
	if err := m.reconcileJournalPivot(ctx, group, journal); err != nil {
		for _, volume := range group.volumes {
			if volume.lease != nil {
				_ = volume.lease.Release()
			}
		}
		return err
	}
	m.mu.Lock()
	if _, exists := m.groups[journal.ContainerID]; exists {
		m.mu.Unlock()
		return ErrStateVolumeGroupExists
	}
	m.groups[journal.ContainerID] = group
	m.mu.Unlock()
	m.monitorStateVolumeProcess(group)
	m.scheduleStateVolumeCompactions(group)
	return nil
}

func stateVolumeGroupFromJournal(journal StateVolumeJournal) *stateVolumeGroup {
	group := &stateVolumeGroup{
		containerID:                 journal.ContainerID,
		sourceStateSnapshotID:       journal.SourceStateSnapshotID,
		runtimeDir:                  filepath.Dir(journal.QMPSocket),
		qmpSocket:                   journal.QMPSocket,
		nbdSocket:                   journal.NBDSocket,
		ready:                       true,
		writersResumedIndeterminate: journal.Phase == "writers-resumed-indeterminate",
	}
	if journal.Recovery != nil {
		copyEnvelope := *journal.Recovery
		group.recovery = &copyEnvelope
	}
	if journal.Release != nil {
		copyEnvelope := *journal.Release
		copyEnvelope.Members = append([]StateVolumeReleaseMember(nil), journal.Release.Members...)
		group.release = &copyEnvelope
	}
	for _, entry := range journal.Volumes {
		group.volumes = append(group.volumes, &stateVolumeRuntime{
			spec: StateVolumeSpec{
				ID: entry.ID, Name: entry.Name, ContainerMountPath: entry.ContainerMountPath,
				Root: entry.Root, ReadOnly: entry.ReadOnly, Generation: entry.Generation,
				CurrentGenerationID: entry.CurrentGenerationID, LineageSourceGenerationID: entry.LineageSourceGenerationID, BackingDir: entry.BackingDir,
				SourceVolumeID: entry.SourceVolumeID, SourceGeneration: entry.SourceGeneration,
				SourceParentGenerationID:      entry.SourceParentGenerationID,
				SourceCloneParentGenerationID: entry.SourceCloneParentGenerationID,
				SourceDepth:                   entry.SourceDepth,
				MountPath:                     entry.MountPath, SizeBytes: entry.SizeBytes, ActiveLayerPath: entry.ActiveLayerPath,
				ActiveBackingPath: entry.ActiveBackingPath, ParentGenerationID: entry.ParentGenerationID,
				CloneParentGenerationID: entry.CloneParentGenerationID,
				// A journal is not an attachment credential. Same-process callers must
				// bind the live request's token after authenticating this lineage; a
				// replacement process uses repository escrow instead.
				FencingToken: entry.FencingToken,
				Depth:        entry.Depth, CreateLayer: entry.CreateLayer, Format: entry.Initialize,
			},
			exportName: entry.ExportName, fileNode: entry.FileNode, activeNode: entry.ActiveNode,
			rootNode: entry.RootNode, devicePath: entry.DevicePath,
			lease: &StateVolumeNBDLease{DevicePath: entry.DevicePath}, prepared: entry.Prepared,
			compactionJobID: entry.CompactionJobID, compactionPhase: entry.CompactionPhase,
			compactionNode: entry.CompactionNode, compactionLayerPath: entry.CompactionLayerPath,
			compactionBackingPath: entry.CompactionBackingPath, compactionPriorGenerationID: entry.CompactionPriorGenerationID,
		})
	}
	return group
}

func (m *StateVolumeManager) reconcileJournalPivot(ctx context.Context, group *stateVolumeGroup, journal StateVolumeJournal) error {
	switch journal.Phase {
	case "starting", "running":
		return m.saveGroupJournal(group, "running", "")
	case "compacting":
		// The adopted group is scheduled after it is installed in the manager.
		// Compaction QMP polling is never performed while startup holds group.mu.
		return nil
	case "recovery-bound":
		return nil
	case "terminal-committed":
		if journal.Recovery == nil || journal.OperationID == "" || journal.Recovery.OperationID != journal.OperationID {
			return fmt.Errorf("terminal committed journal has no exact recovery identity")
		}
		return nil
	case "pivoted", "detached-pending", "terminal-detach-intent":
		group.pending = pendingReceiptFromJournal(journal)
		if group.pending == nil {
			return fmt.Errorf("pivoted journal has no pending generations")
		}
		return nil
	case "pivot-intent", "pivot-frozen", "terminal-quiesced", "pivot-indeterminate", "pivot-rollback-intent", "terminal-rollback-intent":
		outcome, err := inspectJournalPivotGraph(ctx, group.qmp, journal)
		if err != nil {
			return err
		}
		if outcome == StateVolumePivotRolledBack {
			group.pending = pendingReceiptFromJournal(journal)
			if group.pending == nil {
				return fmt.Errorf("rolled-back pivot journal has no pending generations")
			}
			group.pendingRollbackRequired = true
			phase := "pivot-rollback-intent"
			if journal.Recovery != nil && journal.Recovery.Mode == string(StateSnapshotModeTerminal) {
				phase = "terminal-rollback-intent"
			}
			if journal.Phase != phase {
				journal.Phase = phase
				if err := m.Journals.Save(journal); err != nil {
					group.rollbackIntentPersistNeeded = true
					return fmt.Errorf("persist reconciled pivot rollback intent: %w", err)
				}
			}
			return nil
		}
		if journal.Phase == "pivot-rollback-intent" || journal.Phase == "terminal-rollback-intent" {
			return fmt.Errorf("durable state volume rollback intent resolved to a committed graph")
		}
		group.pending = pendingReceiptFromJournal(journal)
		if group.pending == nil {
			return fmt.Errorf("committed pivot journal has no pending generations")
		}
		for index, volume := range group.volumes {
			entry := journal.Volumes[index]
			if entry.ReadOnly {
				continue
			}
			volume.spec.ActiveBackingPath = entry.ActiveLayerPath
			volume.spec.ActiveLayerPath = entry.PivotLayerPath
			volume.spec.ParentGenerationID = entry.PendingGenerationID
			volume.spec.CloneParentGenerationID = ""
			volume.spec.Depth = entry.PendingDepth + 1
			volume.activeNode = entry.PivotNode
		}
		return m.saveGroupJournal(group, "pivoted", journal.OperationID)
	default:
		return fmt.Errorf("unsupported journal phase %q", journal.Phase)
	}
}

func pendingReceiptFromJournal(journal StateVolumeJournal) *StateVolumePivotReceipt {
	if journal.OperationID == "" {
		return nil
	}
	receipt := &StateVolumePivotReceipt{ContainerID: journal.ContainerID, OperationID: journal.OperationID}
	for _, volume := range journal.Volumes {
		if volume.PendingReused {
			if !volume.ReadOnly || volume.PendingGenerationID == "" || volume.PendingGenerationID != volume.CurrentGenerationID ||
				volume.PendingGeneration != volume.Generation || volume.PendingLayerPath != "" {
				return nil
			}
			receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
				VolumeID: volume.ID, GenerationID: volume.PendingGenerationID, Generation: volume.PendingGeneration,
				Name: volume.Name, MountPath: volume.ContainerMountPath, ReadOnly: true, Root: volume.Root, Reused: true,
				ParentGenerationID:      volume.PendingParentGenerationID,
				CloneParentGenerationID: volume.PendingCloneParentGenerationID,
				VirtualSizeBytes:        volume.SizeBytes, Depth: volume.PendingDepth,
			})
			continue
		}
		if volume.PendingGenerationID == "" || volume.PendingGeneration <= 0 || volume.PendingLayerPath == "" {
			return nil
		}
		receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
			VolumeID: volume.ID, GenerationID: volume.PendingGenerationID, Generation: volume.PendingGeneration,
			Name: volume.Name, MountPath: volume.ContainerMountPath, ReadOnly: volume.ReadOnly, Root: volume.Root,
			LayerPath: volume.PendingLayerPath, BackingPath: volume.PendingBackingPath,
			ParentGenerationID: volume.PendingParentGenerationID, Depth: volume.PendingDepth,
			CloneParentGenerationID:      volume.PendingCloneParentGenerationID,
			Compaction:                   volume.PendingCompaction,
			CompactionSourceGenerationID: volume.PendingCompactionSourceGenerationID,
			VirtualSizeBytes:             volume.SizeBytes,
		})
	}
	return receipt
}

func inspectJournalPivotGraph(ctx context.Context, qmp StateVolumeQMP, journal StateVolumeJournal) (StateVolumePivotOutcome, error) {
	graph, err := qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return "", err
	}
	newCount, oldCount, targetCount := 0, 0, 0
	for _, volume := range journal.Volumes {
		wrapper, ok := graph.Nodes[volume.RootNode]
		if !ok {
			return "", fmt.Errorf("pivot journal volume %q raw wrapper %q is missing", volume.ID, volume.RootNode)
		}
		if wrapper.Driver != "raw" {
			return "", fmt.Errorf("pivot journal volume %q wrapper %q has driver %q, not raw", volume.ID, volume.RootNode, wrapper.Driver)
		}
		export, ok := graph.Exports[volume.ExportName]
		if !ok || export.NodeName != volume.RootNode || export.ShuttingDown {
			return "", fmt.Errorf("pivot journal volume %q export %q is not attached to raw wrapper %q", volume.ID, volume.ExportName, volume.RootNode)
		}
		if volume.ReadOnly && volume.PivotNode == "" {
			if wrapper.ChildNode != volume.ActiveNode {
				return "", fmt.Errorf("read-only pivot journal volume %q raw wrapper points to %q, not %q", volume.ID, wrapper.ChildNode, volume.ActiveNode)
			}
			active, ok := graph.Nodes[volume.ActiveNode]
			if !ok {
				return "", fmt.Errorf("read-only pivot journal volume %q active node %q is missing", volume.ID, volume.ActiveNode)
			}
			if err := authenticateJournalQcowNode(volume.ID, "active", active, volume.ActiveLayerPath, volume.ActiveBackingPath); err != nil {
				return "", err
			}
			continue
		}
		if volume.PivotNode == "" || volume.ActiveNode == "" {
			return "", fmt.Errorf("pivot journal volume %q lacks graph nodes", volume.ID)
		}
		targetCount++
		switch wrapper.ChildNode {
		case volume.PivotNode:
			pivot, ok := graph.Nodes[volume.PivotNode]
			if !ok {
				return "", fmt.Errorf("pivot journal volume %q wrapper names missing pivot node %q", volume.ID, volume.PivotNode)
			}
			if err := authenticateJournalQcowNode(volume.ID, "pivot", pivot, volume.PivotLayerPath, volume.ActiveLayerPath); err != nil {
				return "", err
			}
			active, ok := graph.Nodes[volume.ActiveNode]
			if !ok {
				return "", fmt.Errorf("pivot journal volume %q backing node %q is missing", volume.ID, volume.ActiveNode)
			}
			if err := authenticateJournalQcowNode(volume.ID, "active", active, volume.ActiveLayerPath, volume.ActiveBackingPath); err != nil {
				return "", err
			}
			newCount++
		case volume.ActiveNode:
			active, ok := graph.Nodes[volume.ActiveNode]
			if !ok {
				return "", fmt.Errorf("pivot journal volume %q wrapper names missing active node %q", volume.ID, volume.ActiveNode)
			}
			if err := authenticateJournalQcowNode(volume.ID, "active", active, volume.ActiveLayerPath, volume.ActiveBackingPath); err != nil {
				return "", err
			}
			oldCount++
		default:
			return "", fmt.Errorf("pivot journal volume %q raw wrapper points to unexpected node %q", volume.ID, wrapper.ChildNode)
		}
	}
	if targetCount == 0 {
		return "", fmt.Errorf("pivot journal has no writable transaction targets")
	}
	if newCount == targetCount {
		return StateVolumePivotCommitted, nil
	}
	if newCount == 0 && oldCount == targetCount {
		return StateVolumePivotRolledBack, nil
	}
	return "", fmt.Errorf("atomic pivot graph is inconsistent: %d/%d new, %d/%d old", newCount, targetCount, oldCount, targetCount)
}

func authenticateJournalQcowNode(volumeID, role string, node StateVolumeQMPNode, expectedPath, expectedBackingPath string) error {
	if node.Driver != "qcow2" {
		return fmt.Errorf("pivot journal volume %q %s node %q has driver %q, not qcow2", volumeID, role, node.Name, node.Driver)
	}
	actualPath, err := canonicalStateVolumePath(node.FilePath)
	if err != nil {
		return fmt.Errorf("pivot journal volume %q %s node %q file: %w", volumeID, role, node.Name, err)
	}
	expected, err := canonicalStateVolumePath(expectedPath)
	if err != nil {
		return fmt.Errorf("pivot journal volume %q %s expected file: %w", volumeID, role, err)
	}
	if actualPath != expected {
		return fmt.Errorf("pivot journal volume %q %s node %q file is %q, not %q", volumeID, role, node.Name, actualPath, expected)
	}
	if expectedBackingPath == "" {
		if node.BackingFilePath != "" || node.BackingFileDepth != 0 {
			return fmt.Errorf("pivot journal volume %q %s node %q unexpectedly has backing %q depth %d", volumeID, role, node.Name, node.BackingFilePath, node.BackingFileDepth)
		}
		return nil
	}
	actualBacking, err := canonicalStateVolumePath(node.BackingFilePath)
	if err != nil {
		return fmt.Errorf("pivot journal volume %q %s node %q backing file: %w", volumeID, role, node.Name, err)
	}
	expectedBacking, err := canonicalStateVolumePath(expectedBackingPath)
	if err != nil {
		return fmt.Errorf("pivot journal volume %q %s expected backing file: %w", volumeID, role, err)
	}
	if actualBacking != expectedBacking || node.BackingFileDepth < 1 {
		return fmt.Errorf("pivot journal volume %q %s node %q backing is %q depth %d, not %q", volumeID, role, node.Name, actualBacking, node.BackingFileDepth, expectedBacking)
	}
	return nil
}

type adoptedStateVolumeProcess struct {
	pid        int
	executable string
	startTime  uint64
	identity   func(int) (string, uint64, error)
	done       chan struct{}
	once       sync.Once
	mu         sync.RWMutex
	exitErr    error
}

func newAdoptedStateVolumeProcess(pid int, executable string, startTime uint64, identity func(int) (string, uint64, error)) *adoptedStateVolumeProcess {
	process := &adoptedStateVolumeProcess{pid: pid, executable: executable, startTime: startTime, identity: identity, done: make(chan struct{})}
	go process.observe()
	return process
}

func (p *adoptedStateVolumeProcess) observe() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		executable, startTime, err := p.identity(p.pid)
		if err == nil && executable == p.executable && startTime == p.startTime {
			continue
		}
		p.finish(err)
		return
	}
}

func (p *adoptedStateVolumeProcess) finish(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.exitErr = err
		p.mu.Unlock()
		close(p.done)
	})
}

func (p *adoptedStateVolumeProcess) PID() int { return p.pid }
func (p *adoptedStateVolumeProcess) ExpectedStateVolumeProcessIdentity() (string, uint64) {
	return p.executable, p.startTime
}
func (p *adoptedStateVolumeProcess) Done() <-chan struct{} { return p.done }
func (p *adoptedStateVolumeProcess) ExitError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.exitErr
}
func (p *adoptedStateVolumeProcess) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.ExitError()
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *adoptedStateVolumeProcess) Kill() error {
	executable, startTime, err := p.identity(p.pid)
	if err != nil {
		return err
	}
	if executable != p.executable || startTime != p.startTime {
		return fmt.Errorf("refuse to kill reused QSD PID %d", p.pid)
	}
	if err := syscall.Kill(p.pid, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

var _ StateVolumeProcess = (*adoptedStateVolumeProcess)(nil)
var _ stateVolumeProcessObserver = (*adoptedStateVolumeProcess)(nil)

func journalContainsPath(journal StateVolumeJournal, path string) bool {
	path = filepath.Clean(path)
	for _, volume := range journal.Volumes {
		if filepath.Clean(volume.MountPath) == path || filepath.Clean(volume.DevicePath) == path {
			return true
		}
	}
	return strings.TrimSpace(path) != "" && filepath.Clean(journal.QMPSocket) == path
}
