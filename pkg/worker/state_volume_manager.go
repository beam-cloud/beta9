package worker

import (
	"context"
	"encoding/json"
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

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var (
	ErrStateVolumeGroupExists        = errors.New("state volume group already exists")
	ErrStateVolumeGroupNotFound      = errors.New("state volume group not found")
	ErrStateVolumePivotPending       = errors.New("state volume pivot is awaiting acknowledgement")
	ErrStateVolumeCompactionRequired = errors.New("state volume chain requires compaction")
	ErrStateVolumeQSDExited          = errors.New("state volume QSD exited unexpectedly")
)

type StateVolumeMountOps interface {
	Format(ctx context.Context, devicePath string) error
	Mount(ctx context.Context, devicePath, mountPath string, readOnly bool) error
	Sync(ctx context.Context, mountPath string) error
	Freeze(ctx context.Context, mountPath string) error
	Thaw(ctx context.Context, mountPath string) error
	Unmount(ctx context.Context, mountPath string) error
}

type LinuxStateVolumeMountOps struct {
	Runner StateVolumeCommandRunner
}

func (o LinuxStateVolumeMountOps) runner() StateVolumeCommandRunner {
	if o.Runner == nil {
		return OSStateVolumeCommandRunner{}
	}
	return o.Runner
}

func (o LinuxStateVolumeMountOps) Format(ctx context.Context, devicePath string) error {
	_, err := o.runner().Run(ctx, "mkfs.ext4", "-F", "-m", "0", devicePath)
	return err
}

func (o LinuxStateVolumeMountOps) Mount(ctx context.Context, devicePath, mountPath string, readOnly bool) error {
	options := "noatime"
	if readOnly {
		// noload is required for immutable multi-attach: ext4 must never replay
		// its journal and dirty a shared read-only qcow2 generation.
		options += ",ro,noload"
	}
	_, err := o.runner().Run(ctx, "mount", "-t", "ext4", "-o", options, devicePath, mountPath)
	return err
}

func (o LinuxStateVolumeMountOps) Sync(ctx context.Context, mountPath string) error {
	_, err := o.runner().Run(ctx, "sync", "-f", mountPath)
	return err
}

func (o LinuxStateVolumeMountOps) Freeze(ctx context.Context, mountPath string) error {
	_, err := o.runner().Run(ctx, "fsfreeze", "--freeze", mountPath)
	return err
}

func (o LinuxStateVolumeMountOps) Thaw(ctx context.Context, mountPath string) error {
	_, err := o.runner().Run(ctx, "fsfreeze", "--unfreeze", mountPath)
	if err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "invalid argument") || strings.Contains(message, "not frozen") {
			return nil
		}
	}
	return err
}

func (o LinuxStateVolumeMountOps) Unmount(ctx context.Context, mountPath string) error {
	_, err := o.runner().Run(ctx, "umount", mountPath)
	return err
}

type StateVolumeProcess interface {
	PID() int
	Wait(ctx context.Context) error
	Kill() error
}

type StateVolumeQSDLauncher interface {
	Start(args []string, extraFiles []*os.File, logPath string) (StateVolumeProcess, error)
}

// stateVolumeNBDSocketVerifier is a narrow test seam for fake QSD launchers.
// Production launchers always use the filesystem verifier below.
type stateVolumeNBDSocketVerifier interface {
	VerifyStateVolumeNBDSocket(path string) error
}

// stateVolumeRuntimeGraphVerifier is a unit-test seam. Production QMP clients
// always take the authenticated graph path below.
type stateVolumeRuntimeGraphVerifier interface {
	VerifyStateVolumeRuntimeGraph(ctx context.Context, volume *stateVolumeRuntime) error
}

type OSStateVolumeQSDLauncher struct{}

func (OSStateVolumeQSDLauncher) Start(args []string, extraFiles []*os.File, logPath string) (StateVolumeProcess, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open QSD log %s: %w", logPath, err)
	}
	cmd := exec.Command("qemu-storage-daemon", args...)
	cmd.ExtraFiles = extraFiles
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start qemu-storage-daemon: %w", err)
	}
	_ = logFile.Close()
	process := &osStateVolumeProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		process.mu.Lock()
		process.exitErr = cmd.Wait()
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

type osStateVolumeProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	mu      sync.RWMutex
	exitErr error
}

func (p *osStateVolumeProcess) PID() int {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *osStateVolumeProcess) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.ExitError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *osStateVolumeProcess) Done() <-chan struct{} { return p.done }

func (p *osStateVolumeProcess) ExitError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.exitErr
}

func (p *osStateVolumeProcess) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

type stateVolumeProcessObserver interface {
	Done() <-chan struct{}
	ExitError() error
}

// stateVolumeBoundProcessIdentity is implemented by processes adopted from a
// durable journal. It keeps stop/recovery from treating a reused PID as the
// QSD that owns this group.
type stateVolumeBoundProcessIdentity interface {
	ExpectedStateVolumeProcessIdentity() (string, uint64)
}

func (m *StateVolumeManager) monitorStateVolumeProcess(group *stateVolumeGroup) {
	if group == nil || group.process == nil {
		return
	}
	observer, ok := group.process.(stateVolumeProcessObserver)
	if !ok {
		return
	}
	go func() {
		<-observer.Done()
		group.mu.Lock()
		expected := group.stopping
		group.failed = !expected
		group.mu.Unlock()
		if expected || m.OnUnexpectedExit == nil {
			return
		}
		err := observer.ExitError()
		if err == nil {
			err = fmt.Errorf("qemu-storage-daemon exited unexpectedly")
		}
		m.OnUnexpectedExit(group.containerID, err)
	}()
}

type StateVolumeManager struct {
	WorkerID         string
	WorkerInstanceID string
	WorkerPodUID     string
	StorageNodeID    string
	RuntimeRoot      string
	StateRoot        string
	// StrictLayout binds production journals to the canonical state-volume
	// containers/volumes/mounts/block-cache layout. Tests with isolated roots
	// still enforce containment even when they use abbreviated directory names.
	StrictLayout bool
	Journals     StateVolumeJournalStore
	SecurePaths  stateVolumeSecurePathOps
	NBD          *StateVolumeNBDAllocator
	Connector    StateVolumeNBDConnector
	Images       StateVolumeImageTool
	Mounts       StateVolumeMountOps
	QMPDialer    StateVolumeQMPDialer
	Launcher     StateVolumeQSDLauncher
	// ProcessIdentity is injectable for recovery tests. Production reads the
	// executable and kernel start time from /proc before trusting a PID.
	ProcessIdentity  func(int) (string, uint64, error)
	OnUnexpectedExit func(containerID string, err error)

	mu     sync.Mutex
	groups map[string]*stateVolumeGroup
}

type stateVolumeRuntime struct {
	spec       StateVolumeSpec
	exportName string
	fileNode   string
	rootNode   string
	activeNode string
	// devicePath is the stable journal identity for the NBD device. It is
	// retained after a terminal pending group releases its live allocator
	// lease so recovery can prove that the old device is no longer mounted or
	// connected without manufacturing an unlocked lease.
	devicePath                  string
	lease                       *StateVolumeNBDLease
	connected                   bool
	mounted                     bool
	frozen                      bool
	prepared                    bool
	compactionJobID             string
	compactionPhase             string
	compactionNode              string
	compactionLayerPath         string
	compactionBackingPath       string
	compactionPriorGenerationID string
}

type stateVolumeGroup struct {
	mu                          sync.Mutex
	containerID                 string
	sourceStateSnapshotID       string
	runtimeDir                  string
	qmpSocket                   string
	nbdSocket                   string
	process                     StateVolumeProcess
	qmp                         StateVolumeQMP
	volumes                     []*stateVolumeRuntime
	pending                     *StateVolumePivotReceipt
	pendingRollbackRequired     bool
	rollbackIntentPersistNeeded bool
	acknowledgedOperationID     string
	indeterminate               bool
	writersResumedIndeterminate bool
	stopping                    bool
	failed                      bool
	ready                       bool
	resumeRequired              bool
	resumeCommitted             bool
	resumeHook                  func(context.Context, bool) error
	terminalCompletionRequired  bool
	terminalComplete            func(context.Context, bool) error
	recovery                    *StateVolumeRecoveryEnvelope
	release                     *StateVolumeReleaseEnvelope
	compactionWorker            bool
	compactionErr               error
	compactionErrObserved       bool
	compactionDone              chan struct{}
	compactionCancel            context.CancelFunc
	compactionEpoch             uint64
}

func (m *StateVolumeManager) defaults() error {
	if m.RuntimeRoot == "" {
		m.RuntimeRoot = "/var/lib/beta9/state-volumes/runtime"
	}
	if m.StateRoot == "" {
		m.StateRoot = filepath.Dir(m.RuntimeRoot)
	}
	if m.Journals.RootDir == "" {
		m.Journals.RootDir = "/var/lib/beta9/state-volumes/journals"
	}
	if m.SecurePaths == nil {
		m.SecurePaths = newStateVolumeSecurePathOps()
	}
	if m.Journals.SecurePaths == nil {
		m.Journals.SecurePaths = m.SecurePaths
	}
	if m.NBD == nil {
		m.NBD = &StateVolumeNBDAllocator{}
	}
	if m.Connector == nil {
		m.Connector = NBDClientConnector{}
	}
	if m.Images == nil {
		m.Images = QEMUStateVolumeImageTool{}
	}
	if m.Mounts == nil {
		m.Mounts = LinuxStateVolumeMountOps{}
	}
	if m.QMPDialer == nil {
		m.QMPDialer = UnixStateVolumeQMPDialer{}
	}
	if m.Launcher == nil {
		m.Launcher = OSStateVolumeQSDLauncher{}
	}
	if m.ProcessIdentity == nil {
		m.ProcessIdentity = stateVolumeProcessIdentity
	}
	if m.groups == nil {
		m.groups = make(map[string]*stateVolumeGroup)
	}
	return nil
}

func (m *StateVolumeManager) securePathOps() stateVolumeSecurePathOps {
	if m.SecurePaths != nil {
		return m.SecurePaths
	}
	return newStateVolumeSecurePathOps()
}

func BuildStateVolumeQSDArgs(qmpSocket, pidFile, nbdSocket string, volumes []*stateVolumeRuntime) ([]string, error) {
	if !filepath.IsAbs(qmpSocket) || !filepath.IsAbs(pidFile) || !filepath.IsAbs(nbdSocket) {
		return nil, fmt.Errorf("invalid QSD socket or pidfile path")
	}
	if strings.ContainsAny(qmpSocket, ",\n") || strings.ContainsAny(pidFile, ",\n") || strings.ContainsAny(nbdSocket, ",\n") {
		return nil, fmt.Errorf("QSD socket and pidfile paths cannot contain commas or newlines")
	}
	if filepath.Dir(nbdSocket) != filepath.Dir(qmpSocket) || filepath.Base(nbdSocket) != "nbd.sock" {
		return nil, fmt.Errorf("QSD NBD socket must use the owner-private runtime directory")
	}
	if len(volumes) == 0 {
		return nil, fmt.Errorf("QSD requires at least one state volume")
	}
	args := []string{
		"--pidfile", pidFile,
		"--chardev", "socket,path=" + qmpSocket + ",server=on,wait=off,id=qmp0",
		"--monitor", "chardev=qmp0",
		"--nbd-server", "addr.type=unix,addr.path=" + nbdSocket,
	}
	for _, volume := range volumes {
		if volume == nil || volume.spec.ActiveLayerPath == "" || volume.fileNode == "" || volume.activeNode == "" || volume.rootNode == "" || volume.exportName == "" {
			return nil, fmt.Errorf("incomplete QSD state volume")
		}
		fileConfig, _ := json.Marshal(map[string]any{
			"driver":    "file",
			"filename":  volume.spec.ActiveLayerPath,
			"node-name": volume.fileNode,
			"read-only": volume.spec.ReadOnly,
		})
		qcowConfig, _ := json.Marshal(map[string]any{
			"driver":    "qcow2",
			"file":      volume.fileNode,
			"node-name": volume.activeNode,
			"read-only": volume.spec.ReadOnly,
		})
		rawConfig, _ := json.Marshal(map[string]any{
			"driver":    "raw",
			"file":      volume.activeNode,
			"node-name": volume.rootNode,
			"read-only": volume.spec.ReadOnly,
		})
		writable := "on"
		if volume.spec.ReadOnly {
			writable = "off"
		}
		args = append(args,
			"--blockdev", string(fileConfig),
			"--blockdev", string(qcowConfig),
			"--blockdev", string(rawConfig),
			"--export", fmt.Sprintf("type=nbd,id=%s,node-name=%s,name=%s,writable=%s", volume.exportName, volume.rootNode, volume.exportName, writable),
		)
	}
	return args, nil
}

func (m *StateVolumeManager) Start(ctx context.Context, spec StateVolumeGroupSpec) (_ *StateVolumeGroupHandle, retErr error) {
	if err := m.defaults(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ContainerID) == "" || len(spec.Volumes) == 0 {
		return nil, fmt.Errorf("state volume group requires a container ID and volumes")
	}
	if err := validateStateVolumeGroupPaths(spec.Volumes); err != nil {
		return nil, err
	}
	rootCount := 0
	for _, volume := range spec.Volumes {
		if volume.Root {
			rootCount++
			if volume.Name != "root" || volume.ContainerMountPath != "/" || volume.ReadOnly {
				return nil, fmt.Errorf("state volume root must be the writable canonical root member")
			}
		}
		if volume.ParentGenerationID != "" && volume.CloneParentGenerationID != "" {
			return nil, fmt.Errorf("state volume %q cannot have both parent and clone parent", volume.ID)
		}
		if volume.Format && volume.CreateLayer {
			return nil, fmt.Errorf("state volume %q cannot initialize and restore a layer", volume.ID)
		}
		if volume.CreateLayer && strings.TrimSpace(volume.ActiveBackingPath) == "" {
			return nil, fmt.Errorf("restored state volume %q requires an authenticated backing layer", volume.ID)
		}
		if volume.ReadOnly {
			if volume.Format || volume.CreateLayer {
				return nil, fmt.Errorf("read-only state volume %q cannot create a layer", volume.ID)
			}
			if volume.AttachmentToken != "" || volume.FencingToken != 0 {
				return nil, fmt.Errorf("read-only state volume %q carries a writer fence", volume.ID)
			}
			continue
		}
		if parsed, err := uuid.Parse(volume.AttachmentToken); err != nil || parsed.String() != strings.ToLower(volume.AttachmentToken) || volume.FencingToken <= 0 {
			return nil, fmt.Errorf("writable state volume %q requires canonical attachment and positive fencing tokens", volume.ID)
		}
	}
	if rootCount > 1 {
		return nil, fmt.Errorf("state volume group contains multiple root members")
	}
	sort.Slice(spec.Volumes, func(i, j int) bool { return spec.Volumes[i].ID < spec.Volumes[j].ID })
	runtimeDir := filepath.Join(m.RuntimeRoot, stateVolumeToken("container-", spec.ContainerID))
	group := &stateVolumeGroup{
		containerID:           spec.ContainerID,
		sourceStateSnapshotID: spec.SourceStateSnapshotID,
		runtimeDir:            runtimeDir,
		qmpSocket:             filepath.Join(runtimeDir, "qmp.sock"),
		nbdSocket:             filepath.Join(runtimeDir, "nbd.sock"),
	}
	// Reserve the container identity through the complete start transaction.
	// A second caller must never launch another QSD between an unlocked
	// existence check and the final insert.
	m.mu.Lock()
	if _, exists := m.groups[spec.ContainerID]; exists {
		m.mu.Unlock()
		return nil, ErrStateVolumeGroupExists
	}
	m.groups[spec.ContainerID] = group
	m.mu.Unlock()
	// Build and validate the complete deterministic graph plan without touching
	// the filesystem or allocating a kernel device. The fsynced init-intent
	// below is therefore the first mutation in a fresh group startup.
	for _, volumeSpec := range spec.Volumes {
		if volumeSpec.SizeBytes <= 0 || volumeSpec.Depth < 0 || volumeSpec.Depth > StateVolumeMaxActiveDepth {
			m.mu.Lock()
			delete(m.groups, spec.ContainerID)
			m.mu.Unlock()
			return nil, fmt.Errorf("state volume %q has invalid size or depth", volumeSpec.ID)
		}
		if volumeSpec.ActiveLayerPath == "" {
			volumeSpec.ActiveLayerPath = filepath.Join(volumeSpec.BackingDir, "base.qcow2")
		}
		activeRoot := volumeSpec.BackingDir
		if volumeSpec.ReadOnly {
			activeRoot = volumeSpec.ReadOnlyLayerRoot
			if strings.TrimSpace(activeRoot) == "" {
				m.mu.Lock()
				delete(m.groups, spec.ContainerID)
				m.mu.Unlock()
				return nil, fmt.Errorf("read-only volume %q has no trusted layer root", volumeSpec.ID)
			}
		}
		if err := ensureStateVolumePathUnder(activeRoot, volumeSpec.ActiveLayerPath); err != nil {
			m.mu.Lock()
			delete(m.groups, spec.ContainerID)
			m.mu.Unlock()
			return nil, fmt.Errorf("volume %q active layer: %w", volumeSpec.ID, err)
		}
		if err := validateStateVolumePathPair(volumeSpec.ActiveLayerPath, volumeSpec.MountPath); err != nil {
			m.mu.Lock()
			delete(m.groups, spec.ContainerID)
			m.mu.Unlock()
			return nil, fmt.Errorf("volume %q active layer and mount: %w", volumeSpec.ID, err)
		}
		if volumeSpec.Depth == 0 {
			volumeSpec.Depth = 1
		}
		if volumeSpec.Format || volumeSpec.CreateLayer {
			if _, err := os.Lstat(volumeSpec.ActiveLayerPath); err == nil {
				m.mu.Lock()
				delete(m.groups, spec.ContainerID)
				m.mu.Unlock()
				return nil, fmt.Errorf("refuse to replace existing state volume layer %s", volumeSpec.ActiveLayerPath)
			} else if !os.IsNotExist(err) {
				m.mu.Lock()
				delete(m.groups, spec.ContainerID)
				m.mu.Unlock()
				return nil, err
			}
		}
		token := stateVolumeToken("", spec.ContainerID+"\x00"+volumeSpec.ID)
		group.volumes = append(group.volumes, &stateVolumeRuntime{
			spec: volumeSpec, exportName: "export-" + token, fileNode: "file-" + token,
			activeNode: "active-" + token, rootNode: "root-" + token,
		})
	}
	journalStarted := false
	createdLayers := make([]string, 0, len(spec.Volumes))
	defer func() {
		if retErr == nil {
			return
		}
		cleanupErr := m.stopGroup(context.Background(), group, journalStarted)
		if cleanupErr == nil {
			for _, path := range createdLayers {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove failed-start layer %q: %w", path, err))
				}
			}
			for _, volume := range group.volumes {
				_ = os.Remove(volume.spec.MountPath)
			}
			_ = os.RemoveAll(runtimeDir)
			m.mu.Lock()
			if m.groups[spec.ContainerID] == group {
				delete(m.groups, spec.ContainerID)
			}
			m.mu.Unlock()
		}
		retErr = errors.Join(retErr, cleanupErr)
	}()
	preparationIntentPhase := "init-intent"
	preparationProgressPhase := "init-preparing"
	for _, volume := range group.volumes {
		if volume.spec.CreateLayer {
			preparationIntentPhase = "restore-intent"
			preparationProgressPhase = "restore-preparing"
			break
		}
	}
	if err := m.saveGroupJournal(group, preparationIntentPhase, ""); err != nil {
		return nil, err
	}
	journalStarted = true
	if err := os.MkdirAll(runtimeDir, 0700); err != nil {
		return nil, fmt.Errorf("create QSD runtime directory: %w", err)
	}
	if err := os.Chmod(runtimeDir, 0700); err != nil {
		return nil, fmt.Errorf("secure QSD runtime directory: %w", err)
	}
	runtimeInfo, err := os.Lstat(runtimeDir)
	if err != nil || !runtimeInfo.IsDir() || runtimeInfo.Mode()&os.ModeSymlink != 0 || runtimeInfo.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("QSD runtime directory %q is not an owner-private directory", runtimeDir)
	}
	_ = os.Remove(group.qmpSocket)

	for _, volume := range group.volumes {
		volumeSpec := &volume.spec
		if volumeSpec.SizeBytes <= 0 || volumeSpec.Depth < 0 || volumeSpec.Depth > StateVolumeMaxActiveDepth {
			return nil, fmt.Errorf("state volume %q has invalid size or depth", volumeSpec.ID)
		}
		if err := os.MkdirAll(volumeSpec.BackingDir, 0700); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(volumeSpec.MountPath, 0755); err != nil {
			return nil, err
		}
		if volumeSpec.ActiveLayerPath == "" {
			volumeSpec.ActiveLayerPath = filepath.Join(volumeSpec.BackingDir, "base.qcow2")
		}
		activeRoot := volumeSpec.BackingDir
		if volumeSpec.ReadOnly {
			activeRoot = volumeSpec.ReadOnlyLayerRoot
			if strings.TrimSpace(activeRoot) == "" {
				return nil, fmt.Errorf("read-only volume %q has no trusted layer root", volumeSpec.ID)
			}
		}
		if err := ensureStateVolumePathUnder(activeRoot, volumeSpec.ActiveLayerPath); err != nil {
			return nil, fmt.Errorf("volume %q active layer: %w", volumeSpec.ID, err)
		}
		if err := validateStateVolumePathPair(volumeSpec.ActiveLayerPath, volumeSpec.MountPath); err != nil {
			return nil, fmt.Errorf("volume %q active layer and mount: %w", volumeSpec.ID, err)
		}
		if volumeSpec.Depth == 0 {
			volumeSpec.Depth = 1
		}
		if volumeSpec.Format || volumeSpec.CreateLayer {
			// The durable initialization/restore intent above must be the first
			// filesystem mutation. Only now create the exact layer parent; a
			// crash at any point is classified by the preparation journal.
			if err := os.MkdirAll(filepath.Dir(volumeSpec.ActiveLayerPath), 0700); err != nil {
				return nil, fmt.Errorf("create state volume %q layer directory: %w", volumeSpec.ID, err)
			}
			if _, err := os.Lstat(volumeSpec.ActiveLayerPath); err == nil {
				return nil, fmt.Errorf("refuse to replace existing state volume layer %s", volumeSpec.ActiveLayerPath)
			} else if !os.IsNotExist(err) {
				return nil, err
			}
			backingPath := ""
			if volumeSpec.CreateLayer {
				backingPath = volumeSpec.ActiveBackingPath
			}
			if err := m.Images.Create(ctx, volumeSpec.ActiveLayerPath, volumeSpec.SizeBytes, backingPath); err != nil {
				return nil, fmt.Errorf("create state volume %q: %w", volumeSpec.ID, err)
			}
			createdLayers = append(createdLayers, volumeSpec.ActiveLayerPath)
		} else {
			if err := m.Images.Check(ctx, volumeSpec.ActiveLayerPath); err != nil {
				return nil, fmt.Errorf("check state volume %q: %w", volumeSpec.ID, err)
			}
		}
		imageInfo, err := m.Images.Info(ctx, volumeSpec.ActiveLayerPath)
		if err != nil {
			return nil, fmt.Errorf("inspect state volume %q: %w", volumeSpec.ID, err)
		}
		if err := validateStateVolumeImageInfo(imageInfo, volumeSpec.SizeBytes, volumeSpec.ActiveBackingPath); err != nil {
			return nil, fmt.Errorf("validate state volume %q: %w", volumeSpec.ID, err)
		}
		if err := syncStateVolumeFileAndDirectory(volumeSpec.ActiveLayerPath); err != nil {
			return nil, fmt.Errorf("sync prepared state volume %q: %w", volumeSpec.ID, err)
		}
		volume.prepared = true
		if err := m.saveGroupJournal(group, preparationProgressPhase, ""); err != nil {
			return nil, err
		}
		lease, err := m.NBD.Acquire()
		if err != nil {
			return nil, fmt.Errorf("allocate NBD for state volume %q: %w", volumeSpec.ID, err)
		}
		volume.devicePath = lease.DevicePath
		volume.lease = lease
		if err := m.saveGroupJournal(group, preparationProgressPhase, ""); err != nil {
			return nil, err
		}
	}

	_ = os.Remove(group.nbdSocket)
	args, err := BuildStateVolumeQSDArgs(group.qmpSocket, filepath.Join(runtimeDir, "qsd.pid"), group.nbdSocket, group.volumes)
	if err != nil {
		return nil, err
	}
	// The NBD identities and exact owner-private Unix endpoint are durable
	// before exec. Startup recovery can therefore classify a crash at every
	// subsequent phase without guessing which devices belonged to this group.
	if err := m.saveGroupJournal(group, "start-intent", ""); err != nil {
		return nil, err
	}
	group.process, err = m.Launcher.Start(args, nil, filepath.Join(runtimeDir, "qsd.log"))
	if err != nil {
		return nil, err
	}
	if err := m.saveGroupJournal(group, "starting", ""); err != nil {
		return nil, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	group.qmp, err = waitForStateVolumeQMP(probeCtx, m.QMPDialer, group.qmpSocket)
	if err != nil {
		return nil, err
	}
	if err := group.qmp.ProbeSnapshotSupport(probeCtx); err != nil {
		return nil, fmt.Errorf("probe QSD snapshot capabilities: %w", err)
	}
	if verifier, ok := m.Launcher.(stateVolumeNBDSocketVerifier); ok {
		err = verifier.VerifyStateVolumeNBDSocket(group.nbdSocket)
	} else {
		err = verifyStateVolumeNBDSocket(group.nbdSocket)
	}
	if err != nil {
		return nil, fmt.Errorf("verify QSD NBD Unix socket: %w", err)
	}

	for _, volume := range group.volumes {
		if err := m.Connector.Connect(ctx, group.nbdSocket, volume.exportName, volume.lease.DevicePath); err != nil {
			return nil, err
		}
		kernelCtx, kernelCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := m.NBD.WaitConnected(kernelCtx, volume.lease.DevicePath, volume.spec.SizeBytes); err != nil {
			kernelCancel()
			return nil, fmt.Errorf("verify connected state volume %q: %w", volume.spec.ID, err)
		}
		kernelCancel()
		// A successful userspace connect command is not the ownership proof. Only
		// the exact kernel pid+size postcondition makes this device Ready. Failed
		// Connect/WaitConnected paths retain connected=false, while teardown still
		// checks and disconnects every leased NBD unconditionally.
		volume.connected = true
		if verifier, ok := group.qmp.(stateVolumeRuntimeGraphVerifier); ok {
			err = verifier.VerifyStateVolumeRuntimeGraph(ctx, volume)
		} else {
			err = authenticateStateVolumeRuntimeGraph(ctx, group.qmp, volume)
		}
		if err != nil {
			return nil, fmt.Errorf("authenticate connected state volume %q export: %w", volume.spec.ID, err)
		}
		if volume.spec.Format {
			if err := m.Mounts.Format(ctx, volume.lease.DevicePath); err != nil {
				return nil, fmt.Errorf("format state volume %q: %w", volume.spec.ID, err)
			}
		}
		if err := m.Mounts.Mount(ctx, volume.lease.DevicePath, volume.spec.MountPath, volume.spec.ReadOnly); err != nil {
			return nil, fmt.Errorf("mount state volume %q: %w", volume.spec.ID, err)
		}
		volume.mounted = true
		if err := m.NBD.VerifyMounted(volume.lease.DevicePath, volume.spec.MountPath, volume.spec.ReadOnly); err != nil {
			return nil, fmt.Errorf("verify mounted state volume %q: %w", volume.spec.ID, err)
		}
	}
	if err := m.saveGroupJournal(group, "running", ""); err != nil {
		return nil, err
	}
	group.ready = true
	m.monitorStateVolumeProcess(group)
	m.scheduleStateVolumeCompactions(group)
	return stateVolumeGroupHandle(group), nil
}

func authenticateStateVolumeRuntimeGraph(ctx context.Context, qmp StateVolumeQMP, volume *stateVolumeRuntime) error {
	if qmp == nil || volume == nil {
		return fmt.Errorf("state volume runtime graph context is incomplete")
	}
	graph, err := qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return err
	}
	wrapper, ok := graph.Nodes[volume.rootNode]
	if !ok || wrapper.Driver != "raw" || wrapper.ChildNode != volume.activeNode {
		return fmt.Errorf("raw wrapper %q does not point to active node %q", volume.rootNode, volume.activeNode)
	}
	export, ok := graph.Exports[volume.exportName]
	if !ok || export.NodeName != volume.rootNode || export.ShuttingDown {
		return fmt.Errorf("export %q is not active on wrapper %q", volume.exportName, volume.rootNode)
	}
	active, ok := graph.Nodes[volume.activeNode]
	if !ok {
		return fmt.Errorf("active node %q is missing", volume.activeNode)
	}
	return authenticateJournalQcowNode(volume.spec.ID, "active", active, volume.spec.ActiveLayerPath, volume.spec.ActiveBackingPath)
}

type StateVolumeGroupHandle struct {
	ContainerID           string
	MountPaths            map[string]string
	RootVolumeID          string
	SourceStateSnapshotID string
	SourceGenerations     []StateVolumeSourceGeneration
}

func stateVolumeGroupHandle(group *stateVolumeGroup) *StateVolumeGroupHandle {
	handle := &StateVolumeGroupHandle{
		ContainerID: group.containerID, SourceStateSnapshotID: group.sourceStateSnapshotID,
		MountPaths: make(map[string]string, len(group.volumes)),
	}
	for _, volume := range group.volumes {
		handle.MountPaths[volume.spec.ID] = volume.spec.MountPath
		if volume.spec.Root {
			handle.RootVolumeID = volume.spec.ID
		}
		if group.sourceStateSnapshotID != "" {
			handle.SourceGenerations = append(handle.SourceGenerations, StateVolumeSourceGeneration{
				VolumeID: volume.spec.SourceVolumeID, GenerationID: volume.spec.LineageSourceGenerationID,
				Generation: volume.spec.SourceGeneration, Name: volume.spec.Name,
				MountPath: volume.spec.ContainerMountPath, ReadOnly: volume.spec.ReadOnly, Root: volume.spec.Root,
				ParentGenerationID:      volume.spec.SourceParentGenerationID,
				CloneParentGenerationID: volume.spec.SourceCloneParentGenerationID,
				Depth:                   volume.spec.SourceDepth,
			})
		}
	}
	sort.Slice(handle.SourceGenerations, func(i, j int) bool {
		return handle.SourceGenerations[i].VolumeID < handle.SourceGenerations[j].VolumeID
	})
	return handle
}

func waitForStateVolumeQMP(ctx context.Context, dialer StateVolumeQMPDialer, socketPath string) (StateVolumeQMP, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		client, err := dialer.Dial(ctx, socketPath)
		if err == nil {
			return client, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for QSD QMP socket: %w (last error: %v)", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func ensureStateVolumePathUnder(root, path string) error {
	canonicalRoot, err := canonicalStateVolumePath(root)
	if err != nil {
		return err
	}
	canonicalPath, err := canonicalStateVolumePath(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(canonicalRoot, canonicalPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("path %q escapes backing root %q", canonicalPath, canonicalRoot)
	}
	return nil
}

func (m *StateVolumeManager) group(containerID string) (*stateVolumeGroup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	group := m.groups[containerID]
	if group == nil {
		return nil, ErrStateVolumeGroupNotFound
	}
	return group, nil
}

// ExistingGroup returns a read-only view of a startup-reconciled group so the
// re-delivered container request can bind to it instead of launching a second
// QSD. Callers must validate every member against the authoritative request.
func (m *StateVolumeManager) ExistingGroup(containerID string) (*StateVolumeGroupHandle, []StateVolumeSpec, bool) {
	group, err := m.group(containerID)
	if err != nil {
		return nil, nil, false
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if !group.ready || group.failed || group.stopping {
		return nil, nil, false
	}
	specs := make([]StateVolumeSpec, len(group.volumes))
	for i, volume := range group.volumes {
		specs[i] = volume.spec
	}
	return stateVolumeGroupHandle(group), specs, true
}

func stateVolumeJournalPhaseIsShutdownSafe(phase string) bool {
	switch phase {
	case "detached-pending", "terminal-committed", "release-intent", "release-completed":
		return true
	default:
		return false
	}
}

// ShutdownSafeContainers authenticates the durable boundary at which the
// worker may exit without keeping its cache/storage/repository clients alive.
// A live group must be completely detached from kernel/QSD resources and its
// exact replay obligation must already be fsynced. Unsafe groups are returned
// as an error; callers poll until their shutdown deadline rather than tearing
// dependencies down underneath an upload or mounted filesystem.
func (m *StateVolumeManager) ShutdownSafeContainers() (map[string]struct{}, error) {
	safe := make(map[string]struct{})
	if m == nil {
		return safe, nil
	}
	journals, err := m.Journals.List()
	if err != nil {
		return nil, err
	}
	journalByContainer := make(map[string]StateVolumeJournal, len(journals))
	for _, journal := range journals {
		// State roots may be shared by multiple worker slots. A live foreign
		// owner is neither ours to drain nor a reason to tear down this worker.
		if m.WorkerInstanceID != "" && journal.WorkerInstanceID != "" && journal.WorkerInstanceID != m.WorkerInstanceID {
			continue
		}
		journalByContainer[journal.ContainerID] = journal
	}

	m.mu.Lock()
	groups := make([]*stateVolumeGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, group)
	}
	m.mu.Unlock()
	unsafe := make([]string, 0)
	seenGroups := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		seenGroups[group.containerID] = struct{}{}
		journal, journaled := journalByContainer[group.containerID]
		group.mu.Lock()
		detached := group.process == nil && group.qmp == nil && !group.compactionWorker
		for _, volume := range group.volumes {
			if volume.mounted || volume.connected || volume.lease != nil || volume.frozen {
				detached = false
				break
			}
		}
		recoveryBound := group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal)
		releaseBound := group.release != nil && group.release.LocalCleanupVerified && journal.Release != nil &&
			stateVolumeReleaseEnvelopeMatches(group.release, journal.Release) &&
			(journal.Phase == "release-intent" || journal.Phase == "release-completed")
		group.mu.Unlock()
		if journaled && stateVolumeJournalPhaseIsShutdownSafe(journal.Phase) && detached && (recoveryBound || releaseBound) {
			safe[group.containerID] = struct{}{}
			continue
		}
		unsafe = append(unsafe, fmt.Sprintf("%s(group detached=%t recovery=%t journal_phase=%q)", group.containerID, detached, recoveryBound, journal.Phase))
	}
	for containerID, journal := range journalByContainer {
		if _, live := seenGroups[containerID]; live {
			continue
		}
		if journal.Release != nil && journal.Release.LocalCleanupVerified &&
			(journal.Phase == "release-intent" || journal.Phase == "release-completed") {
			// This proof is written only after stopGroup has verified unmount,
			// disconnect, and QSD exit. The server escrow makes it recoverable by a
			// replacement process without snapshot metadata.
			safe[containerID] = struct{}{}
			continue
		}
		// A snapshot phase string alone is not proof that another process
		// namespace released the old QSD/NBD/mounts. Startup reconciliation must
		// first authenticate kernel clearance and reconstruct a detached group.
		unsafe = append(unsafe, fmt.Sprintf("%s(journal_phase=%q)", containerID, journal.Phase))
	}
	if len(unsafe) != 0 {
		sort.Strings(unsafe)
		return safe, fmt.Errorf("state-volume shutdown boundary is unsafe: %s", strings.Join(unsafe, ", "))
	}
	return safe, nil
}

// PendingOperation reports the durable operation currently owning a group's
// immutable layers. It is used by container teardown to avoid deleting the
// only request context that can finish a terminal publication retry.
func (m *StateVolumeManager) PendingOperation(containerID string) (string, bool) {
	group, err := m.group(containerID)
	if err != nil {
		return "", false
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending == nil || strings.TrimSpace(group.pending.OperationID) == "" {
		return "", false
	}
	return group.pending.OperationID, true
}

// PendingReceipt returns an immutable copy of the exact pending group and
// whether it has already released all live QSD/NBD/mount resources.
func (m *StateVolumeManager) PendingReceipt(containerID, operationID string) (*StateVolumePivotReceipt, bool, error) {
	group, err := m.group(containerID)
	if err != nil {
		return nil, false, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending == nil {
		return nil, false, nil
	}
	if group.pendingRollbackRequired {
		return nil, false, fmt.Errorf("state volume pivot for operation %q has no committed-outcome proof", operationID)
	}
	if group.pending.OperationID != operationID {
		return nil, false, ErrStateVolumePivotPending
	}
	if group.indeterminate {
		return nil, false, ErrStateVolumePivotIndeterminate
	}
	return cloneStateVolumePivotReceipt(group.pending), group.process == nil && group.qmp == nil, nil
}

// ReconcilePendingOperation is the mandatory barrier before a retry may read
// or upload a pending layer. It resolves a lost QMP reply from the live graph,
// thaws every filesystem, and reruns the saved resume/terminal-complete hook.
// An indeterminate receipt is never exposed to UploadPending.
func (m *StateVolumeManager) ReconcilePendingOperation(ctx context.Context, containerID, operationID string) (*StateVolumePivotReceipt, error) {
	group, err := m.group(containerID)
	if err != nil {
		return nil, err
	}
	if err := m.waitForStateVolumeCompactions(ctx, group); err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.failed {
		return nil, ErrStateVolumeQSDExited
	}
	if group.pending == nil {
		return nil, nil
	}
	if group.pending.OperationID != operationID {
		return nil, ErrStateVolumePivotPending
	}
	if group.rollbackIntentPersistNeeded {
		if err := m.persistStateVolumeRollbackIntent(group); err != nil {
			return nil, err
		}
		group.rollbackIntentPersistNeeded = false
	}
	if group.indeterminate {
		if group.writersResumedIndeterminate {
			return nil, fmt.Errorf("indeterminate state volume writers resumed; original pending operation is permanently tainted")
		}
		if group.qmp == nil {
			return nil, ErrStateVolumePivotIndeterminate
		}
		journal, err := m.Journals.Load(containerID)
		if err != nil {
			return nil, err
		}
		outcome, err := inspectJournalPivotGraph(ctx, group.qmp, journal)
		if err != nil {
			return nil, fmt.Errorf("reconcile indeterminate state volume pivot: %w", err)
		}
		group.resumeCommitted = outcome == StateVolumePivotCommitted
		if outcome == StateVolumePivotRolledBack {
			group.pendingRollbackRequired = true
			rollbackPhase := "pivot-rollback-intent"
			if group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) {
				rollbackPhase = "terminal-rollback-intent"
			}
			journal.Phase = rollbackPhase
			if err := m.Journals.Save(journal); err != nil {
				group.rollbackIntentPersistNeeded = true
				return nil, fmt.Errorf("persist reconciled state volume rollback intent: %w", err)
			}
			group.rollbackIntentPersistNeeded = false
			group.indeterminate = false
		} else {
			byID := make(map[string]StateVolumeJournalVolume, len(journal.Volumes))
			for _, entry := range journal.Volumes {
				byID[entry.ID] = entry
			}
			for _, volume := range group.volumes {
				entry, ok := byID[volume.spec.ID]
				if !ok {
					return nil, fmt.Errorf("pivot journal is missing volume %q", volume.spec.ID)
				}
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
			if err := m.saveGroupJournal(group, "pivoted", operationID); err != nil {
				return nil, err
			}
			group.indeterminate = false
		}
	}
	if err := m.thawStateVolumes(group.volumes); err != nil {
		return cloneStateVolumePivotReceipt(group.pending), err
	}
	if group.resumeRequired {
		if group.resumeHook == nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("state volume writers require an unavailable resume hook")
		}
		resumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := group.resumeHook(resumeCtx, group.resumeCommitted)
		cancel()
		if err != nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("resume state volume writers: %w", err)
		}
		group.resumeRequired = false
		group.resumeHook = nil
		group.terminalCompletionRequired = false
		group.terminalComplete = nil
	}
	if group.pendingRollbackRequired {
		if err := m.rollbackUncommittedStateVolumePivot(group); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if group.terminalCompletionRequired && group.resumeCommitted {
		if group.terminalComplete == nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("terminal state volume completion hook is unavailable")
		}
		completeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := group.terminalComplete(completeCtx, true)
		cancel()
		if err != nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("complete terminal state volume writers: %w", err)
		}
		group.terminalCompletionRequired = false
		group.terminalComplete = nil
	}
	return cloneStateVolumePivotReceipt(group.pending), nil
}

// ResumeIndeterminateWriters is the fail-safe response when the QMP graph
// itself cannot yet be authenticated. It does not expose or upload the pending
// receipt and does not guess the transaction outcome; it only thaws ext4 and
// resumes the paused runtime so a lost monitor reply cannot leave a live
// service as a SIGSTOP zombie. A later retry must still reconcile the graph.
func (m *StateVolumeManager) ResumeIndeterminateWriters(ctx context.Context, containerID, operationID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if !group.indeterminate || group.pending == nil || group.pending.OperationID != operationID {
		return ErrStateVolumePivotIndeterminate
	}
	if group.writersResumedIndeterminate {
		return fmt.Errorf("indeterminate state volume writers were already resumed; pending operation is tainted")
	}
	if group.rollbackIntentPersistNeeded {
		return fmt.Errorf("state volume writers remain stopped until the resolved rollback intent is durable")
	}
	if err := m.thawStateVolumes(group.volumes); err != nil {
		return err
	}
	if group.resumeRequired {
		if group.resumeHook == nil {
			return fmt.Errorf("indeterminate state volume writers have no resume hook")
		}
		journal, err := m.Journals.Load(containerID)
		if err != nil {
			return fmt.Errorf("load indeterminate state volume recovery journal: %w", err)
		}
		journal.Phase = "writers-resumed-indeterminate"
		if err := m.Journals.Save(journal); err != nil {
			return fmt.Errorf("persist indeterminate writer-resume taint: %w", err)
		}
		group.writersResumedIndeterminate = true
		resumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = group.resumeHook(resumeCtx, false)
		cancel()
		if err != nil {
			return fmt.Errorf("resume indeterminate state volume writers: %w", err)
		}
		group.resumeRequired = false
		group.resumeHook = nil
		// Keep a terminal completion obligation after the temporary SIGCONT.
		// If graph reconciliation later proves the pivot committed, the runtime
		// must still be deleted before its receipt can be detached or uploaded.
		if !group.terminalCompletionRequired {
			group.terminalComplete = nil
		}
	}
	return nil
}

func (m *StateVolumeManager) BindSnapshotRecovery(containerID string, envelope StateVolumeRecoveryEnvelope) error {
	if strings.TrimSpace(envelope.OperationID) == "" {
		return fmt.Errorf("state volume recovery operation ID is empty")
	}
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending != nil && group.pending.OperationID != envelope.OperationID {
		return ErrStateVolumePivotPending
	}
	if group.recovery != nil && *group.recovery != envelope {
		return fmt.Errorf("state volume recovery envelope changed for operation %q", group.recovery.OperationID)
	}
	copyEnvelope := envelope
	group.recovery = &copyEnvelope
	return m.saveGroupJournal(group, "recovery-bound", envelope.OperationID)
}

func (m *StateVolumeManager) SnapshotRecovery(containerID, operationID string) (StateVolumeRecoveryEnvelope, error) {
	group, err := m.group(containerID)
	if err != nil {
		return StateVolumeRecoveryEnvelope{}, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.recovery == nil || group.recovery.OperationID != operationID {
		return StateVolumeRecoveryEnvelope{}, fmt.Errorf("state volume recovery envelope for operation %q is unavailable", operationID)
	}
	return *group.recovery, nil
}

// CancelSnapshotRecovery clears only a proven pre-pivot recovery binding. It
// refuses indeterminate, pending, frozen, or resume-required state so callers
// can never mark the DB operation failed while an immutable generation may
// still need offline publication.
func (m *StateVolumeManager) CancelSnapshotRecovery(containerID, operationID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.recovery == nil || group.recovery.OperationID != operationID {
		return fmt.Errorf("state volume recovery operation %q is not bound", operationID)
	}
	if group.pending != nil || group.indeterminate || group.resumeRequired {
		return ErrStateVolumePivotPending
	}
	group.recovery = nil
	return m.saveGroupJournal(group, "running", "")
}

// scheduleStateVolumeCompactions starts one live block-stream worker for the
// group. The worker holds only the manager's control-plane mutex; guest I/O
// continues through QSD while the backing chain is streamed into the current
// unpublished writable top. Snapshot callers wait for this worker before
// pausing the container, so compaction time is never charged to the frozen
// consistency window.
func (m *StateVolumeManager) scheduleStateVolumeCompactions(group *stateVolumeGroup) {
	if group == nil {
		return
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	m.scheduleStateVolumeCompactionsLocked(group)
}

func (m *StateVolumeManager) scheduleStateVolumeCompactionsLocked(group *stateVolumeGroup) {
	if group == nil || group.compactionWorker || group.pending != nil || group.indeterminate ||
		group.failed || group.stopping || !group.ready || group.qmp == nil || group.compactionErr != nil {
		return
	}
	needsCompaction := false
	for _, volume := range group.volumes {
		if volume != nil && !volume.spec.ReadOnly &&
			(volume.compactionPhase != "" || (volume.spec.Depth >= StateVolumeCompactDepth && volume.spec.ActiveBackingPath != "")) {
			needsCompaction = true
			break
		}
	}
	if !needsCompaction {
		return
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	group.compactionWorker = true
	group.compactionEpoch++
	epoch := group.compactionEpoch
	group.compactionCancel = cancel
	group.compactionDone = make(chan struct{})
	group.compactionErrObserved = false
	done := group.compactionDone
	go func() {
		group.mu.Lock()
		err := m.completeStateVolumeCompactionsLocked(workerCtx, group, epoch)
		if group.compactionEpoch == epoch {
			group.compactionErr = err
			group.compactionErrObserved = false
			group.compactionWorker = false
			group.compactionCancel = nil
			close(done)
		}
		group.mu.Unlock()
	}()
}

func (m *StateVolumeManager) waitForStateVolumeCompactions(ctx context.Context, group *stateVolumeGroup) error {
	for {
		group.mu.Lock()
		if !group.compactionWorker && group.compactionErr != nil {
			err := group.compactionErr
			if !group.compactionErrObserved {
				group.compactionErrObserved = true
				group.mu.Unlock()
				return fmt.Errorf("background state volume compaction failed: %w", err)
			}
			if !stateVolumeGroupHasCompactionIntentLocked(group) {
				group.mu.Unlock()
				return fmt.Errorf("background state volume compaction remains unresolved: %w", err)
			}
			// A prior caller has observed the determinate error. Preserve the
			// durable phase as authority and let this call retry it; a failed
			// retry installs a new sticky error before any caller may proceed.
			group.compactionErr = nil
			group.compactionErrObserved = false
		}
		m.scheduleStateVolumeCompactionsLocked(group)
		if !group.compactionWorker {
			group.mu.Unlock()
			return nil
		}
		done := group.compactionDone
		group.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
}

func (m *StateVolumeManager) cancelAndWaitStateVolumeCompactions(ctx context.Context, group *stateVolumeGroup) error {
	for {
		group.mu.Lock()
		if !group.compactionWorker {
			hasIntent := stateVolumeGroupHasCompactionIntentLocked(group)
			if group.compactionErr != nil && !group.compactionErrObserved {
				err := group.compactionErr
				group.compactionErrObserved = true
				group.mu.Unlock()
				return fmt.Errorf("state volume compaction did not reach a durable cancellation outcome: %w", err)
			}
			if !hasIntent {
				if group.compactionErr != nil {
					err := group.compactionErr
					group.mu.Unlock()
					return fmt.Errorf("state volume compaction remains unresolved without a durable retry phase: %w", err)
				}
				group.mu.Unlock()
				return nil
			}
			group.compactionErr = nil
			group.compactionErrObserved = false
			m.scheduleStateVolumeCompactionCancellationLocked(group)
		}
		cancel, done := group.compactionCancel, group.compactionDone
		if cancel != nil {
			cancel()
		}
		group.mu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for state volume compaction cancellation: %w", ctx.Err())
		case <-done:
			// Re-read the durable phase and sticky error. A successful retry
			// returns below; a failed one is surfaced once and remains fenced
			// for the next exact reconciliation attempt.
		}
	}
}

func stateVolumeGroupHasCompactionIntentLocked(group *stateVolumeGroup) bool {
	if group == nil {
		return false
	}
	for _, volume := range group.volumes {
		if volume != nil && !volume.spec.ReadOnly && volume.compactionPhase != "" {
			return true
		}
	}
	return false
}

func (m *StateVolumeManager) scheduleStateVolumeCompactionCancellationLocked(group *stateVolumeGroup) {
	workerCtx, cancel := context.WithCancel(context.Background())
	// Cancellation recovery is intentionally born canceled: the durable state
	// machine authenticates and resolves the existing job/graph, but can never
	// start or continue background compaction during teardown.
	cancel()
	group.compactionWorker = true
	group.compactionEpoch++
	epoch := group.compactionEpoch
	group.compactionCancel = cancel
	group.compactionDone = make(chan struct{})
	group.compactionErrObserved = false
	done := group.compactionDone
	go func() {
		group.mu.Lock()
		err := m.completeStateVolumeCompactionsLocked(workerCtx, group, epoch)
		if group.compactionEpoch == epoch {
			group.compactionErr = err
			group.compactionErrObserved = false
			group.compactionWorker = false
			group.compactionCancel = nil
			close(done)
		}
		group.mu.Unlock()
	}()
}

// completeStateVolumeCompactionsLocked completes or reconciles every
// journaled live block-stream job. group.mu must be held. A previous
// background failure is surfaced once before a caller may retry the durable
// state machine, which prevents a snapshot from silently proceeding on a
// chain whose compaction failed.
func (m *StateVolumeManager) completeStateVolumeCompactionsLocked(ctx context.Context, group *stateVolumeGroup, epoch uint64) error {
	if group == nil || group.qmp == nil {
		return fmt.Errorf("state volume compaction requires a live QMP graph")
	}
	for _, volume := range group.volumes {
		if volume == nil || volume.spec.ReadOnly {
			continue
		}
		if volume.compactionPhase == "" && (volume.spec.Depth < StateVolumeCompactDepth || volume.spec.ActiveBackingPath == "") {
			continue
		}
		if err := m.completeStateVolumeCompactionLocked(ctx, group, volume, epoch); err != nil {
			return fmt.Errorf("compact state volume %q: %w", volume.spec.ID, err)
		}
	}
	return nil
}

func (m *StateVolumeManager) completeStateVolumeCompactionLocked(ctx context.Context, group *stateVolumeGroup, volume *stateVolumeRuntime, epoch uint64) error {
	if group.pending != nil || group.indeterminate || group.failed || group.stopping {
		return fmt.Errorf("state volume group is not eligible for compaction")
	}
	if ctx.Err() != nil && volume.compactionPhase == "" {
		return nil
	}
	if volume.spec.ActiveBackingPath == "" {
		// A crash may occur after job-finalize rewrites the graph but before the
		// parentless state is journaled. Authenticate the graph below when an
		// intent remains; otherwise the in-memory state is already canonical.
		if volume.compactionPhase == "" {
			volume.spec.ParentGenerationID = ""
			volume.spec.CloneParentGenerationID = ""
			volume.spec.Depth = 1
			return nil
		}
	}
	if volume.compactionPhase == "" {
		if volume.spec.Depth < StateVolumeCompactDepth {
			return nil
		}
		if volume.spec.CurrentGenerationID == "" || volume.spec.ActiveBackingPath == "" {
			return fmt.Errorf("depth %d active graph has no current generation or backing", volume.spec.Depth)
		}
		volume.compactionJobID = stateVolumeToken("stream-", group.containerID+"\x00"+volume.spec.ID+"\x00"+volume.spec.CurrentGenerationID)
		volume.compactionPhase = "intent"
		volume.compactionNode = volume.activeNode
		volume.compactionLayerPath = volume.spec.ActiveLayerPath
		volume.compactionBackingPath = volume.spec.ActiveBackingPath
		volume.compactionPriorGenerationID = volume.spec.CurrentGenerationID
		if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
			return fmt.Errorf("persist block-stream intent: %w", err)
		}
	}
	for {
		if group.compactionEpoch != epoch {
			return fmt.Errorf("state volume compaction ownership changed")
		}
		if ctx.Err() != nil {
			return m.cancelStateVolumeCompactionLocked(group, volume, epoch)
		}
		view := *volume
		qmp := group.qmp
		jobID := volume.compactionJobID
		group.mu.Unlock()
		flat, graphErr := authenticateStateVolumeCompactionGraph(ctx, qmp, &view)
		var job *StateVolumeQMPBlockJob
		var jobErr error
		if graphErr == nil {
			job, jobErr = qmp.QueryBlockJob(ctx, jobID)
		}
		group.mu.Lock()
		if group.compactionEpoch != epoch || volume.compactionJobID != jobID || volume.activeNode != view.activeNode ||
			volume.spec.ActiveLayerPath != view.spec.ActiveLayerPath {
			return fmt.Errorf("state volume compaction graph changed while QMP was unlocked")
		}
		if ctx.Err() != nil {
			return m.cancelStateVolumeCompactionLocked(group, volume, epoch)
		}
		if graphErr != nil {
			return graphErr
		}
		if jobErr != nil {
			return fmt.Errorf("query block-stream job: %w", jobErr)
		}
		if flat {
			if job != nil {
				if job.Error != "" {
					return fmt.Errorf("block-stream job %q concluded with error: %s", job.ID, job.Error)
				}
				if job.Status != "concluded" {
					return fmt.Errorf("block-stream graph is parentless while job %q has status %q", job.ID, job.Status)
				}
				group.mu.Unlock()
				dismissErr := qmp.DismissBlockJob(ctx, jobID)
				group.mu.Lock()
				if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
					return fmt.Errorf("state volume compaction ownership changed during job dismissal")
				}
				if dismissErr != nil {
					return fmt.Errorf("dismiss completed block-stream job: %w", dismissErr)
				}
			}
			prior := *volume
			volume.spec.ActiveBackingPath = ""
			volume.spec.ParentGenerationID = ""
			volume.spec.CloneParentGenerationID = ""
			volume.spec.Depth = 1
			volume.compactionJobID = ""
			volume.compactionPhase = ""
			volume.compactionNode = ""
			volume.compactionLayerPath = ""
			volume.compactionBackingPath = ""
			// compactionPriorGenerationID remains as private journal audit state;
			// it is never emitted as physical manifest ancestry.
			if err := m.saveGroupJournal(group, "running", ""); err != nil {
				*volume = prior
				return fmt.Errorf("persist parentless compacted graph: %w", err)
			}
			return nil
		}
		if job == nil {
			volume.compactionPhase = "intent"
			if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
				return err
			}
			nodeName := volume.compactionNode
			group.mu.Unlock()
			startErr := qmp.StartBlockStream(ctx, nodeName, jobID)
			group.mu.Lock()
			if group.compactionEpoch != epoch || volume.compactionJobID != jobID || volume.compactionNode != nodeName {
				return fmt.Errorf("state volume compaction ownership changed during job start")
			}
			if startErr != nil {
				if ctx.Err() != nil {
					return m.cancelStateVolumeCompactionLocked(group, volume, epoch)
				}
				if errors.Is(startErr, ErrStateVolumeCompactionIndeterminate) {
					volume.compactionPhase = "start-indeterminate"
					_ = m.saveGroupJournal(group, "compacting", "")
					continue
				}
				return fmt.Errorf("start block-stream: %w", startErr)
			}
			volume.compactionPhase = "started"
			if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
				return err
			}
			continue
		}
		if job.Error != "" {
			return fmt.Errorf("block-stream job %q failed: %s", job.ID, job.Error)
		}
		switch job.Status {
		case "pending":
			volume.compactionPhase = "finalizing"
			if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
				return err
			}
			group.mu.Unlock()
			finalizeErr := qmp.FinalizeBlockJob(ctx, jobID)
			group.mu.Lock()
			if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
				return fmt.Errorf("state volume compaction ownership changed during job finalization")
			}
			if finalizeErr != nil {
				if ctx.Err() != nil {
					return m.cancelStateVolumeCompactionLocked(group, volume, epoch)
				}
				if errors.Is(finalizeErr, ErrStateVolumeCompactionIndeterminate) {
					volume.compactionPhase = "finalize-indeterminate"
					_ = m.saveGroupJournal(group, "compacting", "")
					continue
				}
				return fmt.Errorf("finalize block-stream: %w", finalizeErr)
			}
		case "created", "running", "paused", "ready", "standby", "waiting":
			group.mu.Unlock()
			select {
			case <-ctx.Done():
				group.mu.Lock()
				return m.cancelStateVolumeCompactionLocked(group, volume, epoch)
			case <-time.After(25 * time.Millisecond):
			}
			group.mu.Lock()
		case "concluded":
			return fmt.Errorf("block-stream job concluded before the active graph became parentless")
		default:
			return fmt.Errorf("block-stream job %q has unsupported status %q", job.ID, job.Status)
		}
	}
}

// cancelStateVolumeCompactionLocked resolves a teardown cancellation to an
// authenticated graph/job outcome before returning. group.mu is held on entry
// and return. A caller must never stop QSD, detach NBD, or retire graph paths
// while this leaves cancel-intent/cancel-indeterminate in the journal.
func (m *StateVolumeManager) cancelStateVolumeCompactionLocked(group *stateVolumeGroup, volume *stateVolumeRuntime, epoch uint64) error {
	if group == nil || group.qmp == nil || volume == nil || volume.compactionJobID == "" {
		return fmt.Errorf("state volume compaction cancellation has no durable job identity")
	}
	jobID := volume.compactionJobID
	volume.compactionPhase = "cancel-intent"
	if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
		return fmt.Errorf("persist block-stream cancellation intent: %w", err)
	}

	qmp := group.qmp
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	group.mu.Unlock()
	cancelErr := qmp.CancelBlockJob(cancelCtx, jobID)
	group.mu.Lock()
	cancel()
	if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
		return fmt.Errorf("state volume compaction ownership changed during job cancellation")
	}
	if cancelErr != nil {
		volume.compactionPhase = "cancel-indeterminate"
		if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
			return errors.Join(cancelErr, err)
		}
	}

	reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer reconcileCancel()
	for {
		view := *volume
		group.mu.Unlock()
		flat, graphErr := authenticateStateVolumeCompactionGraph(reconcileCtx, qmp, &view)
		job, jobErr := qmp.QueryBlockJob(reconcileCtx, jobID)
		group.mu.Lock()
		if group.compactionEpoch != epoch || volume.compactionJobID != jobID || volume.activeNode != view.activeNode ||
			volume.spec.ActiveLayerPath != view.spec.ActiveLayerPath {
			return fmt.Errorf("state volume compaction graph changed during cancellation reconciliation")
		}
		if graphErr != nil || jobErr != nil {
			volume.compactionPhase = "cancel-indeterminate"
			saveErr := m.saveGroupJournal(group, "compacting", "")
			return errors.Join(fmt.Errorf("authenticate canceled block-stream outcome: %w", errors.Join(graphErr, jobErr)), saveErr)
		}

		if flat {
			if job != nil {
				if job.Error != "" || job.Status != "concluded" {
					volume.compactionPhase = "cancel-indeterminate"
					_ = m.saveGroupJournal(group, "compacting", "")
					return fmt.Errorf("parentless graph has unresolved canceled job %q status=%q error=%q", job.ID, job.Status, job.Error)
				}
				group.mu.Unlock()
				dismissErr := qmp.DismissBlockJob(reconcileCtx, jobID)
				group.mu.Lock()
				if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
					return fmt.Errorf("state volume compaction ownership changed during canceled job dismissal")
				}
				if dismissErr != nil {
					volume.compactionPhase = "cancel-indeterminate"
					_ = m.saveGroupJournal(group, "compacting", "")
					return fmt.Errorf("dismiss canceled finalized block-stream job: %w", dismissErr)
				}
			}
			prior := *volume
			volume.spec.ActiveBackingPath = ""
			volume.spec.ParentGenerationID = ""
			volume.spec.CloneParentGenerationID = ""
			volume.spec.Depth = 1
			m.clearStateVolumeCompactionIntentLocked(volume, true)
			if err := m.saveGroupJournal(group, "running", ""); err != nil {
				*volume = prior
				return fmt.Errorf("persist finalized compaction during cancellation: %w", err)
			}
			return nil
		}

		if job == nil {
			prior := *volume
			m.clearStateVolumeCompactionIntentLocked(volume, false)
			if err := m.saveGroupJournal(group, "running", ""); err != nil {
				*volume = prior
				return fmt.Errorf("persist canceled block-stream outcome: %w", err)
			}
			return nil
		}
		if job.Status == "concluded" {
			group.mu.Unlock()
			dismissErr := qmp.DismissBlockJob(reconcileCtx, jobID)
			group.mu.Lock()
			if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
				return fmt.Errorf("state volume compaction ownership changed during canceled job dismissal")
			}
			if dismissErr != nil {
				volume.compactionPhase = "cancel-indeterminate"
				_ = m.saveGroupJournal(group, "compacting", "")
				return fmt.Errorf("dismiss canceled block-stream job: %w", dismissErr)
			}
			prior := *volume
			m.clearStateVolumeCompactionIntentLocked(volume, false)
			if err := m.saveGroupJournal(group, "running", ""); err != nil {
				*volume = prior
				return fmt.Errorf("persist concluded block-stream cancellation: %w", err)
			}
			return nil
		}

		volume.compactionPhase = "cancel-intent"
		if err := m.saveGroupJournal(group, "compacting", ""); err != nil {
			return err
		}
		group.mu.Unlock()
		retryErr := qmp.CancelBlockJob(reconcileCtx, jobID)
		group.mu.Lock()
		if group.compactionEpoch != epoch || volume.compactionJobID != jobID {
			return fmt.Errorf("state volume compaction ownership changed during cancellation retry")
		}
		if retryErr != nil {
			volume.compactionPhase = "cancel-indeterminate"
			_ = m.saveGroupJournal(group, "compacting", "")
		}
		select {
		case <-reconcileCtx.Done():
			return fmt.Errorf("reconcile block-stream cancellation: %w", reconcileCtx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (m *StateVolumeManager) clearStateVolumeCompactionIntentLocked(volume *stateVolumeRuntime, flattened bool) {
	volume.compactionJobID = ""
	volume.compactionPhase = ""
	volume.compactionNode = ""
	volume.compactionLayerPath = ""
	volume.compactionBackingPath = ""
	if !flattened {
		volume.compactionPriorGenerationID = ""
	}
}

func authenticateStateVolumeCompactionGraph(ctx context.Context, qmp StateVolumeQMP, volume *stateVolumeRuntime) (bool, error) {
	graph, err := qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return false, fmt.Errorf("query block-stream graph: %w", err)
	}
	wrapper, ok := graph.Nodes[volume.rootNode]
	if !ok || wrapper.Driver != "raw" || wrapper.ChildNode != volume.activeNode {
		return false, fmt.Errorf("compaction raw wrapper %q is not attached to active node %q", volume.rootNode, volume.activeNode)
	}
	export, ok := graph.Exports[volume.exportName]
	if !ok || export.NodeName != volume.rootNode || export.ShuttingDown {
		return false, fmt.Errorf("compaction export %q is not attached to raw wrapper %q", volume.exportName, volume.rootNode)
	}
	active, ok := graph.Nodes[volume.activeNode]
	if !ok {
		return false, fmt.Errorf("compaction active node %q is missing", volume.activeNode)
	}
	if err := authenticateJournalQcowNode(volume.spec.ID, "compaction active", active, volume.compactionLayerPath, ""); err == nil {
		return true, nil
	}
	if err := authenticateJournalQcowNode(volume.spec.ID, "compaction active", active, volume.compactionLayerPath, volume.compactionBackingPath); err != nil {
		return false, err
	}
	return false, nil
}

// PlanSnapshot deterministically binds the exact consistency-group
// membership and next generation IDs before any runtime pause or pivot. The
// control plane escrows this plan with the writer fencing tokens, allowing a
// detached terminal operation to commit idempotently after lease expiry.
func (m *StateVolumeManager) PlanSnapshot(ctx context.Context, containerID, operationID string) (*StateVolumePivotReceipt, error) {
	if strings.TrimSpace(operationID) == "" {
		return nil, fmt.Errorf("state volume snapshot operation ID is empty")
	}
	group, err := m.group(containerID)
	if err != nil {
		return nil, err
	}
	if err := m.waitForStateVolumeCompactions(ctx, group); err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.failed {
		return nil, ErrStateVolumeQSDExited
	}
	if group.indeterminate {
		return nil, ErrStateVolumePivotIndeterminate
	}
	if group.pending != nil {
		if group.pending.OperationID != operationID {
			return nil, ErrStateVolumePivotPending
		}
		return cloneStateVolumePivotReceipt(group.pending), nil
	}
	receipt := &StateVolumePivotReceipt{ContainerID: containerID, OperationID: operationID}
	for _, volume := range group.volumes {
		if volume.spec.ReadOnly {
			receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
				VolumeID: volume.spec.ID, GenerationID: volume.spec.CurrentGenerationID,
				Generation: volume.spec.Generation, Name: volume.spec.Name,
				MountPath: volume.spec.ContainerMountPath, ReadOnly: true,
				Root: volume.spec.Root, Reused: true,
				ParentGenerationID:      volume.spec.ParentGenerationID,
				CloneParentGenerationID: volume.spec.CloneParentGenerationID,
				VirtualSizeBytes:        volume.spec.SizeBytes, Depth: volume.spec.Depth,
			})
			continue
		}
		receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
			VolumeID:     volume.spec.ID,
			GenerationID: stateVolumeGenerationID(containerID, volume.spec.ID, operationID),
			Generation:   volume.spec.Generation + 1, Name: volume.spec.Name,
			MountPath: volume.spec.ContainerMountPath, Root: volume.spec.Root,
			LayerPath: volume.spec.ActiveLayerPath, BackingPath: volume.spec.ActiveBackingPath,
			ParentGenerationID:           volume.spec.ParentGenerationID,
			CloneParentGenerationID:      volume.spec.CloneParentGenerationID,
			Compaction:                   volume.compactionPriorGenerationID != "" && volume.spec.ActiveBackingPath == "" && volume.spec.Depth == 1,
			CompactionSourceGenerationID: volume.compactionPriorGenerationID,
			VirtualSizeBytes:             volume.spec.SizeBytes, Depth: volume.spec.Depth,
		})
	}
	return receipt, nil
}

func (m *StateVolumeManager) Pivot(ctx context.Context, containerID, operationID string) (*StateVolumePivotReceipt, error) {
	return m.PivotWithHooks(ctx, containerID, operationID, StateVolumePivotHooks{})
}

// PivotWithFrozenHook runs hook after every writable filesystem is frozen and
// before the atomic QMP transaction. It is used to bind runtime-memory bytes
// to the exact filesystem generation being published.
func (m *StateVolumeManager) PivotWithFrozenHook(ctx context.Context, containerID, operationID string, hook func(context.Context) error) (*StateVolumePivotReceipt, error) {
	return m.PivotWithHooks(ctx, containerID, operationID, StateVolumePivotHooks{Frozen: hook})
}

type StateVolumePivotHooks struct {
	Quiesce func(context.Context) error
	Frozen  func(context.Context) error
	Resume  func(context.Context) error
	// Complete runs instead of Resume and is told whether the atomic graph
	// transaction committed. Terminal snapshots use it to terminate paused
	// writers after success while still resuming them on pre-pivot failure.
	Complete func(context.Context, bool) error
}

// PivotWithHooks pauses writers before sync, freezes every writable member,
// pivots the whole graph atomically, thaws, and guarantees Resume on all exits
// after a successful Quiesce.
func (m *StateVolumeManager) PivotWithHooks(ctx context.Context, containerID, operationID string, hooks StateVolumePivotHooks) (_ *StateVolumePivotReceipt, retErr error) {
	if strings.TrimSpace(operationID) == "" {
		return nil, fmt.Errorf("state volume pivot operation ID is empty")
	}
	group, err := m.group(containerID)
	if err != nil {
		return nil, err
	}
	if err := m.waitForStateVolumeCompactions(ctx, group); err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.failed {
		return nil, ErrStateVolumeQSDExited
	}
	if group.rollbackIntentPersistNeeded {
		if err := m.persistStateVolumeRollbackIntent(group); err != nil {
			return cloneStateVolumePivotReceipt(group.pending), err
		}
		group.rollbackIntentPersistNeeded = false
	}
	if err := m.thawStateVolumes(group.volumes); err != nil {
		return cloneStateVolumePivotReceipt(group.pending), err
	}
	if group.resumeRequired {
		if group.resumeHook == nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("state volume writers require reconciliation before snapshot retry")
		}
		resumeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := group.resumeHook(resumeCtx, group.resumeCommitted)
		cancel()
		if err != nil {
			return cloneStateVolumePivotReceipt(group.pending), fmt.Errorf("resume state volume writers: %w", err)
		}
		group.resumeRequired = false
		group.resumeHook = nil
		if group.terminalCompletionRequired {
			group.terminalCompletionRequired = false
			group.terminalComplete = nil
		}
	}
	if group.pendingRollbackRequired {
		if err := m.rollbackUncommittedStateVolumePivot(group); err != nil {
			return nil, err
		}
	}
	if group.indeterminate {
		return nil, ErrStateVolumePivotIndeterminate
	}
	if group.pending != nil {
		if group.pending.OperationID == operationID {
			return cloneStateVolumePivotReceipt(group.pending), nil
		}
		return nil, ErrStateVolumePivotPending
	}
	// A determinate outcome belongs only to its exact pending operation. Never
	// let Complete(true) from an acknowledged prior snapshot bleed into a new
	// operation that later proves rolled back.
	group.resumeCommitted = false

	actions := make([]StateVolumeSnapshotAction, 0, len(group.volumes))
	pivotVolumes := make([]*stateVolumeRuntime, 0, len(group.volumes))
	receipt := &StateVolumePivotReceipt{ContainerID: containerID, OperationID: operationID}
	for _, volume := range group.volumes {
		if volume.spec.ReadOnly && volume.spec.CurrentGenerationID != "" {
			receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
				VolumeID: volume.spec.ID, GenerationID: volume.spec.CurrentGenerationID,
				Generation: volume.spec.Generation, Name: volume.spec.Name,
				MountPath: volume.spec.ContainerMountPath, ReadOnly: true, Root: volume.spec.Root, Reused: true,
				ParentGenerationID:      volume.spec.ParentGenerationID,
				CloneParentGenerationID: volume.spec.CloneParentGenerationID,
				VirtualSizeBytes:        volume.spec.SizeBytes, Depth: volume.spec.Depth,
			})
			continue
		}
		if volume.spec.Depth >= StateVolumeMaxDepth {
			return nil, ErrStateVolumeCompactionRequired
		}
		generationID := stateVolumeGenerationID(containerID, volume.spec.ID, operationID)
		newNode := stateVolumeToken("active-", containerID+"\x00"+volume.spec.ID+"\x00"+operationID)
		newPath := filepath.Join(volume.spec.BackingDir, "layers", stateVolumeToken("next-", operationID)+".qcow2")
		if err := ensureStateVolumePathUnder(volume.spec.BackingDir, newPath); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(newPath), 0700); err != nil {
			return nil, err
		}
		if _, err := os.Lstat(newPath); err == nil {
			return nil, fmt.Errorf("pivot target already exists: %s", newPath)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		actions = append(actions, StateVolumeSnapshotAction{CurrentNode: volume.activeNode, NewNode: newNode, NewPath: newPath, Mode: "existing"})
		pivotVolumes = append(pivotVolumes, volume)
		receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
			VolumeID:                     volume.spec.ID,
			GenerationID:                 generationID,
			Generation:                   volume.spec.Generation + 1,
			Name:                         volume.spec.Name,
			MountPath:                    volume.spec.ContainerMountPath,
			ReadOnly:                     volume.spec.ReadOnly,
			Root:                         volume.spec.Root,
			LayerPath:                    volume.spec.ActiveLayerPath,
			BackingPath:                  volume.spec.ActiveBackingPath,
			ParentGenerationID:           volume.spec.ParentGenerationID,
			CloneParentGenerationID:      volume.spec.CloneParentGenerationID,
			Compaction:                   volume.compactionPriorGenerationID != "" && volume.spec.ActiveBackingPath == "" && volume.spec.Depth == 1,
			CompactionSourceGenerationID: volume.compactionPriorGenerationID,
			VirtualSizeBytes:             volume.spec.SizeBytes,
			Depth:                        volume.spec.Depth,
		})
	}
	group.pending = receipt
	if err := m.savePivotIntent(group, operationID, actions); err != nil {
		group.pending = nil
		return nil, err
	}
	// The complete multi-volume target plan is fsynced before creating the
	// first child. A crash after any individual create therefore leaves a
	// deterministic journal that can remove a proven rollback or publish the
	// immutable old tops after terminal pod replacement.
	for index, volume := range pivotVolumes {
		action := actions[index]
		if err := m.Images.Create(ctx, action.NewPath, volume.spec.SizeBytes, volume.spec.ActiveLayerPath); err != nil {
			removeStateVolumePivotTargets(actions)
			group.pending = nil
			saveErr := m.saveGroupJournal(group, "running", "")
			return nil, errors.Join(fmt.Errorf("create pivot layer for state volume %q: %w", volume.spec.ID, err), saveErr)
		}
		if err := m.Images.Check(ctx, action.NewPath); err != nil {
			removeStateVolumePivotTargets(actions)
			group.pending = nil
			saveErr := m.saveGroupJournal(group, "running", "")
			return nil, errors.Join(fmt.Errorf("check pivot layer for state volume %q: %w", volume.spec.ID, err), saveErr)
		}
		imageInfo, err := m.Images.Info(ctx, action.NewPath)
		if err == nil {
			err = validateStateVolumeImageInfo(imageInfo, volume.spec.SizeBytes, volume.spec.ActiveLayerPath)
		}
		if err != nil {
			removeStateVolumePivotTargets(actions)
			group.pending = nil
			saveErr := m.saveGroupJournal(group, "running", "")
			return nil, errors.Join(fmt.Errorf("validate pivot layer for state volume %q: %w", volume.spec.ID, err), saveErr)
		}
	}
	quiesced := false
	committed := false
	if hooks.Quiesce != nil {
		if err := hooks.Quiesce(ctx); err != nil {
			group.pending = nil
			removeStateVolumePivotTargets(actions)
			_ = m.saveGroupJournal(group, "running", "")
			return nil, fmt.Errorf("quiesce state volume writers: %w", err)
		}
		quiesced = true
		group.resumeRequired = true
		group.resumeHook = func(resumeCtx context.Context, committed bool) error {
			if hooks.Complete != nil {
				return hooks.Complete(resumeCtx, committed)
			}
			if hooks.Resume != nil {
				return hooks.Resume(resumeCtx)
			}
			return nil
		}
		if hooks.Complete != nil {
			group.terminalCompletionRequired = true
			group.terminalComplete = hooks.Complete
		}
	}
	defer func() {
		if group.indeterminate || group.rollbackIntentPersistNeeded {
			return
		}
		// Record the determinate QMP outcome before any thaw attempt. A
		// persistent thaw failure must retain Complete(true) authority across
		// retries; otherwise a terminal snapshot could resume writers after its
		// graph transaction already committed.
		group.resumeCommitted = committed
		// The kernel freeze postcondition is authoritative. Never resume or
		// terminate writers until every writable filesystem has thawed. A thaw
		// failure retains resumeRequired and the pending journal for an exact retry.
		if err := m.thawStateVolumes(group.volumes); err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		if !quiesced {
			if group.pendingRollbackRequired {
				retErr = errors.Join(retErr, m.rollbackUncommittedStateVolumePivot(group))
			}
			return
		}
		resumeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := group.resumeHook(resumeCtx, committed)
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("resume state volume writers: %w", err))
		} else {
			group.resumeRequired = false
			group.resumeHook = nil
			if group.terminalCompletionRequired {
				group.terminalCompletionRequired = false
				group.terminalComplete = nil
			}
			quiesced = false
			if group.pendingRollbackRequired {
				retErr = errors.Join(retErr, m.rollbackUncommittedStateVolumePivot(group))
			}
		}
	}()

	for _, volume := range group.volumes {
		if volume.spec.ReadOnly {
			continue
		}
		if err := m.Mounts.Sync(ctx, volume.spec.MountPath); err != nil {
			group.pendingRollbackRequired = true
			return nil, fmt.Errorf("sync state volume %q: %w", volume.spec.ID, err)
		}
	}
	frozen := make([]*stateVolumeRuntime, 0, len(group.volumes))
	for _, volume := range group.volumes {
		if volume.spec.ReadOnly {
			continue
		}
		if err := m.Mounts.Freeze(ctx, volume.spec.MountPath); err != nil {
			group.pendingRollbackRequired = true
			return nil, fmt.Errorf("freeze state volume %q: %w", volume.spec.ID, err)
		}
		frozen = append(frozen, volume)
		volume.frozen = true
	}
	if hooks.Frozen != nil {
		if err := hooks.Frozen(ctx); err != nil {
			group.pendingRollbackRequired = true
			return nil, fmt.Errorf("state snapshot frozen hook: %w", err)
		}
	}
	quiescedPhase := "pivot-frozen"
	if group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) {
		quiescedPhase = "terminal-quiesced"
	}
	if err := m.savePivotJournal(group, quiescedPhase, operationID, actions); err != nil {
		group.pendingRollbackRequired = true
		return nil, err
	}
	var transactionErr error
	if len(actions) != 0 {
		transactionErr = group.qmp.TransactionSnapshot(ctx, actions)
	}
	if transactionErr != nil {
		if errors.Is(transactionErr, ErrStateVolumePivotIndeterminate) {
			group.indeterminate = true
			_ = m.savePivotJournal(group, "pivot-indeterminate", operationID, actions)
			return cloneStateVolumePivotReceipt(receipt), transactionErr
		}
		group.pendingRollbackRequired = true
		rollbackPhase := "pivot-rollback-intent"
		if group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) {
			rollbackPhase = "terminal-rollback-intent"
		}
		if persistErr := m.savePivotJournal(group, rollbackPhase, operationID, actions); persistErr != nil {
			// The last durable journal still permits publication. Writers must
			// remain frozen/stopped until an exact retry fsyncs a nonpublishable
			// rollback outcome.
			group.rollbackIntentPersistNeeded = true
			return nil, errors.Join(transactionErr, fmt.Errorf("persist determinate pivot rollback intent: %w", persistErr))
		}
		group.rollbackIntentPersistNeeded = false
		return nil, transactionErr
	}
	committed = true
	group.resumeCommitted = true
	group.pendingRollbackRequired = false
	for i, volume := range pivotVolumes {
		volume.activeNode = actions[i].NewNode
		volume.spec.ActiveBackingPath = volume.spec.ActiveLayerPath
		volume.spec.ActiveLayerPath = actions[i].NewPath
		for _, generation := range receipt.Generations {
			if generation.VolumeID == volume.spec.ID {
				volume.spec.ParentGenerationID = generation.GenerationID
				volume.spec.CloneParentGenerationID = ""
				break
			}
		}
		volume.spec.Depth++
	}
	if err := m.saveGroupJournal(group, "pivoted", operationID); err != nil {
		return cloneStateVolumePivotReceipt(receipt), err
	}
	thawErr := m.thawStateVolumes(frozen)
	if thawErr != nil {
		return cloneStateVolumePivotReceipt(receipt), thawErr
	}
	return cloneStateVolumePivotReceipt(receipt), nil
}

func removeStateVolumePivotTargets(actions []StateVolumeSnapshotAction) {
	for _, action := range actions {
		_ = os.Remove(action.NewPath)
	}
}

// rollbackUncommittedStateVolumePivot is called only after every filesystem is
// thawed and any quiesced writer has been resumed. It destroys the pre-created
// children from the durable pivot-intent journal and clears the in-memory
// receipt so a same-operation retry must execute a real QMP transaction rather
// than uploading a layer that may still be writable.
func (m *StateVolumeManager) rollbackUncommittedStateVolumePivot(group *stateVolumeGroup) error {
	if group == nil || !group.pendingRollbackRequired {
		return nil
	}
	if group.indeterminate || group.resumeRequired {
		return fmt.Errorf("state volume pivot outcome is not safe to roll back")
	}
	for _, volume := range group.volumes {
		if volume.frozen {
			return fmt.Errorf("state volume %q remains frozen", volume.spec.ID)
		}
	}
	journal, err := m.Journals.Load(group.containerID)
	if err != nil {
		return fmt.Errorf("load uncommitted state volume pivot journal: %w", err)
	}
	for _, volume := range journal.Volumes {
		if volume.PivotLayerPath != "" {
			_ = os.Remove(volume.PivotLayerPath)
		}
	}
	pending := group.pending
	group.pending = nil
	group.pendingRollbackRequired = false
	phase, operationID := "running", ""
	if group.recovery != nil {
		phase, operationID = "recovery-bound", group.recovery.OperationID
	}
	if err := m.saveGroupJournal(group, phase, operationID); err != nil {
		// The durable rollback-intent phase remains nonpublishable. Restore
		// the in-memory obligation so a later retry cannot return the receipt.
		group.pending = pending
		group.pendingRollbackRequired = true
		return err
	}
	return nil
}

func (m *StateVolumeManager) persistStateVolumeRollbackIntent(group *stateVolumeGroup) error {
	if group == nil || group.pending == nil || !group.pendingRollbackRequired {
		return fmt.Errorf("state volume rollback intent has no exact pending operation")
	}
	journal, err := m.Journals.Load(group.containerID)
	if err != nil {
		return fmt.Errorf("load state volume rollback intent: %w", err)
	}
	if journal.OperationID != group.pending.OperationID {
		return fmt.Errorf("state volume rollback journal operation %q does not match %q", journal.OperationID, group.pending.OperationID)
	}
	phase := "pivot-rollback-intent"
	if group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) {
		phase = "terminal-rollback-intent"
	}
	if journal.Phase == phase {
		return nil
	}
	switch journal.Phase {
	case "pivot-intent", "pivot-frozen", "terminal-quiesced", "pivot-indeterminate":
	default:
		return fmt.Errorf("state volume rollback cannot transition journal phase %q", journal.Phase)
	}
	journal.Phase = phase
	if err := m.Journals.Save(journal); err != nil {
		return fmt.Errorf("persist state volume rollback intent: %w", err)
	}
	return nil
}

func (m *StateVolumeManager) thawStateVolumes(volumes []*stateVolumeRuntime) error {
	var result error
	for i := len(volumes) - 1; i >= 0; i-- {
		if !volumes[i].frozen {
			continue
		}
		if err := m.Mounts.Thaw(context.Background(), volumes[i].spec.MountPath); err != nil {
			result = errors.Join(result, fmt.Errorf("thaw state volume %q: %w", volumes[i].spec.ID, err))
		} else {
			volumes[i].frozen = false
		}
	}
	return result
}

func cloneStateVolumePivotReceipt(receipt *StateVolumePivotReceipt) *StateVolumePivotReceipt {
	if receipt == nil {
		return nil
	}
	clone := *receipt
	clone.Generations = append([]StateVolumePivotGeneration(nil), receipt.Generations...)
	return &clone
}

func (m *StateVolumeManager) UploadPending(ctx context.Context, containerID, operationID string, cas BlockV1CAS) ([]StateVolumeGenerationReceipt, error) {
	group, err := m.group(containerID)
	if err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.failed {
		return nil, ErrStateVolumeQSDExited
	}
	if group.indeterminate {
		return nil, ErrStateVolumePivotIndeterminate
	}
	if group.pendingRollbackRequired {
		return nil, fmt.Errorf("pending state volume pivot for operation %q has no committed-outcome proof", operationID)
	}
	if group.pending == nil || group.pending.OperationID != operationID {
		return nil, fmt.Errorf("no pending state volume pivot for operation %q", operationID)
	}
	receipts := make([]StateVolumeGenerationReceipt, len(group.pending.Generations))
	errGroup, uploadCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(blockV1TransferConcurrency)
	for i, generation := range group.pending.Generations {
		i, generation := i, generation
		errGroup.Go(func() error {
			if generation.Reused {
				receipts[i] = StateVolumeGenerationReceipt{
					VolumeID: generation.VolumeID, GenerationID: generation.GenerationID, Generation: generation.Generation,
					Name: generation.Name, MountPath: generation.MountPath, ReadOnly: generation.ReadOnly,
					Root: generation.Root, Reused: true,
					ParentGenerationID:      generation.ParentGenerationID,
					CloneParentGenerationID: generation.CloneParentGenerationID,
					Depth:                   generation.Depth, VirtualSizeBytes: generation.VirtualSizeBytes,
				}
				return nil
			}
			// The pivot makes LayerPath immutable, but the live writes that formed
			// it may have exposed latent qcow2 metadata/refcount corruption that an
			// earlier Info call cannot detect. Authenticate the complete image
			// before the first chunk enters CAS or any generation can become
			// AVAILABLE.
			if err := m.Images.Check(uploadCtx, generation.LayerPath); err != nil {
				return fmt.Errorf("check immutable state volume %q generation %q: %w", generation.VolumeID, generation.GenerationID, err)
			}
			manifest, err := CreateBlockV1Manifest(uploadCtx, generation.LayerPath, BlockV1Metadata{
				VolumeID:                generation.VolumeID,
				GenerationID:            generation.GenerationID,
				ParentGenerationID:      generation.ParentGenerationID,
				CloneParentGenerationID: generation.CloneParentGenerationID,
				VirtualSizeBytes:        generation.VirtualSizeBytes,
				Depth:                   generation.Depth,
				Generation:              generation.Generation,
				BackingPath:             generation.BackingPath,
			}, m.Images, cas)
			if err != nil {
				return fmt.Errorf("upload state volume %q: %w", generation.VolumeID, err)
			}
			receipts[i] = StateVolumeGenerationReceipt{
				VolumeID: generation.VolumeID, GenerationID: generation.GenerationID, Generation: generation.Generation,
				Name: generation.Name, MountPath: generation.MountPath, ReadOnly: generation.ReadOnly, Root: generation.Root,
				ParentGenerationID: manifest.ParentGenerationID, CloneParentGenerationID: manifest.CloneParentGenerationID, Depth: manifest.Depth,
				VirtualSizeBytes: manifest.VirtualSizeBytes, Manifest: manifest,
			}
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].VolumeID < receipts[j].VolumeID })
	return receipts, nil
}

func (m *StateVolumeManager) AcknowledgePending(containerID, operationID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending == nil || group.pending.OperationID != operationID {
		if group.pending == nil && group.acknowledgedOperationID == operationID {
			return nil
		}
		return fmt.Errorf("no pending state volume pivot for operation %q", operationID)
	}
	byVolume := make(map[string]StateVolumePivotGeneration, len(group.pending.Generations))
	for _, generation := range group.pending.Generations {
		if !generation.Reused {
			byVolume[generation.VolumeID] = generation
		}
	}
	for _, volume := range group.volumes {
		if volume.spec.ReadOnly && volume.spec.CurrentGenerationID != "" {
			continue
		}
		generation, ok := byVolume[volume.spec.ID]
		if !ok || generation.Generation != volume.spec.Generation+1 {
			return fmt.Errorf("pending generation counter for volume %q is inconsistent", volume.spec.ID)
		}
		volume.spec.Generation = generation.Generation
		volume.spec.CurrentGenerationID = generation.GenerationID
		volume.spec.CloneParentGenerationID = ""
		volume.compactionPriorGenerationID = ""
	}
	group.pending = nil
	group.acknowledgedOperationID = operationID
	if group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) {
		// Keep the non-secret operation envelope until terminal resource and
		// lifecycle cleanup finishes. A crash after the control-plane Commit
		// but before Stop must be discoverable at the next startup; otherwise
		// the gateway can short-circuit the available operation while the old
		// journal and retained ContainerInstance remain stranded forever.
		return m.saveGroupJournal(group, "terminal-committed", operationID)
	}
	group.recovery = nil
	if err := m.saveGroupJournal(group, "running", ""); err != nil {
		return err
	}
	m.scheduleStateVolumeCompactionsLocked(group)
	return nil
}

func (m *StateVolumeManager) Stop(ctx context.Context, containerID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	if err := m.cancelAndWaitStateVolumeCompactions(ctx, group); err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	group.compactionEpoch++
	if group.pending != nil || group.indeterminate {
		return ErrStateVolumePivotPending
	}
	if err := m.stopGroup(ctx, group, true); err != nil {
		return err
	}
	retireErr := m.retireGroupTransientPaths(group)
	m.mu.Lock()
	delete(m.groups, containerID)
	m.mu.Unlock()
	return retireErr
}

func cloneStateVolumeReleaseEnvelope(in *StateVolumeReleaseEnvelope) *StateVolumeReleaseEnvelope {
	if in == nil {
		return nil
	}
	out := *in
	out.Members = append([]StateVolumeReleaseMember(nil), in.Members...)
	return &out
}

func stateVolumeReleaseEnvelopeMatches(left, right *StateVolumeReleaseEnvelope) bool {
	if left == nil || right == nil || left.JournalDigest != right.JournalDigest ||
		left.WorkspaceID != right.WorkspaceID || left.SourceWorkerID != right.SourceWorkerID ||
		left.SourceWorkerInstanceID != right.SourceWorkerInstanceID || left.StorageNodeID != right.StorageNodeID {
		return false
	}
	leftMembers, leftErr := canonicalStateVolumeReleaseMembers(left.Members)
	rightMembers, rightErr := canonicalStateVolumeReleaseMembers(right.Members)
	if leftErr != nil || rightErr != nil || len(leftMembers) != len(rightMembers) {
		return false
	}
	for index := range leftMembers {
		if leftMembers[index] != rightMembers[index] {
			return false
		}
	}
	return true
}

// PersistReleaseDetachIntent fsyncs the non-secret local obligation before the
// repository escrows attachment authority. It is idempotent and never
// downgrades an already armed or detached obligation on response replay.
func (m *StateVolumeManager) PersistReleaseDetachIntent(containerID string, release StateVolumeReleaseEnvelope) error {
	if err := m.defaults(); err != nil {
		return err
	}
	digest, err := stateVolumeReleaseJournalDigest(containerID, release)
	if err != nil {
		return err
	}
	if release.JournalDigest == "" {
		release.JournalDigest = digest
	}
	if release.JournalDigest != digest || release.ReleaseClaimID != "" || release.ReleaseClaimGeneration != 0 || release.LocalCleanupVerified {
		return fmt.Errorf("invalid initial state-volume release obligation")
	}
	release.Members, err = canonicalStateVolumeReleaseMembers(release.Members)
	if err != nil {
		return err
	}

	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group != nil {
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.pending != nil || group.indeterminate {
			return ErrStateVolumePivotPending
		}
		if group.release != nil {
			if !stateVolumeReleaseEnvelopeMatches(group.release, &release) {
				return fmt.Errorf("state-volume release obligation conflicts with an existing journal")
			}
			return nil
		}
		group.release = cloneStateVolumeReleaseEnvelope(&release)
		if err := m.saveGroupJournal(group, "release-detach-intent", ""); err != nil {
			group.release = nil
			return err
		}
		return nil
	}

	journal, loadErr := m.Journals.Load(containerID)
	if loadErr == nil {
		if journal.Release == nil || !stateVolumeReleaseEnvelopeMatches(journal.Release, &release) {
			return fmt.Errorf("container has a non-release journal without an in-memory cleanup owner")
		}
		return nil
	}
	if !errors.Is(loadErr, os.ErrNotExist) {
		return loadErr
	}
	// No graph was ever installed (for example, authoritative lease renewal
	// succeeded and preparation failed before Start). Persist a path-free
	// obligation so a crash cannot strand the repository attachment.
	return m.Journals.Save(StateVolumeJournal{
		ContainerID: containerID, WorkerID: m.WorkerID, WorkerInstanceID: m.WorkerInstanceID,
		WorkerPodUID: m.WorkerPodUID, StorageNodeID: m.StorageNodeID,
		Phase: "release-detach-intent", Release: cloneStateVolumeReleaseEnvelope(&release),
	})
}

// ArmReleaseIntent binds the local fsynced digest to the repository's source
// escrow. No filesystem, NBD, or QSD resource may be detached before this
// phase is durable.
func (m *StateVolumeManager) ArmReleaseIntent(containerID, claimID string, claimGeneration int64) error {
	parsed, err := uuid.Parse(claimID)
	if err != nil || parsed.String() != claimID || claimGeneration != 0 {
		return fmt.Errorf("invalid source state-volume release escrow identity")
	}
	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group != nil {
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.release == nil {
			return fmt.Errorf("state-volume release was not locally persisted")
		}
		if group.release.ReleaseClaimID != "" && (group.release.ReleaseClaimID != claimID || group.release.ReleaseClaimGeneration != claimGeneration) {
			return fmt.Errorf("state-volume release escrow changed on replay")
		}
		group.release.ReleaseClaimID = claimID
		group.release.ReleaseClaimGeneration = claimGeneration
		phase := "release-armed"
		if group.release.LocalCleanupVerified {
			phase = "release-intent"
		}
		return m.saveGroupJournal(group, phase, "")
	}
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return err
	}
	if journal.Release == nil {
		return fmt.Errorf("state-volume release was not locally persisted")
	}
	if journal.Release.ReleaseClaimID != "" && (journal.Release.ReleaseClaimID != claimID || journal.Release.ReleaseClaimGeneration != claimGeneration) {
		return fmt.Errorf("state-volume release escrow changed on replay")
	}
	journal.Release.ReleaseClaimID = claimID
	journal.Release.ReleaseClaimGeneration = claimGeneration
	journal.Phase = "release-armed"
	if journal.Release.LocalCleanupVerified {
		journal.Phase = "release-intent"
	}
	return m.Journals.Save(journal)
}

// RecordClaimedRelease persists replacement claim ownership before the
// claimant asks the repository to delete any escrowed attachment. It is valid
// only after local kernel cleanup has been authenticated and fsynced.
func (m *StateVolumeManager) RecordClaimedRelease(containerID, claimID string, claimGeneration int64) error {
	parsed, err := uuid.Parse(claimID)
	if err != nil || parsed.String() != claimID || claimGeneration <= 0 {
		return fmt.Errorf("invalid replacement state-volume release claim")
	}
	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group != nil {
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.release == nil || !group.release.LocalCleanupVerified {
			return fmt.Errorf("state-volume release claim preceded authenticated local cleanup")
		}
		if group.release.ReleaseClaimGeneration > 0 &&
			(group.release.ReleaseClaimID != claimID || group.release.ReleaseClaimGeneration > claimGeneration) {
			return fmt.Errorf("state-volume release claim was superseded")
		}
		group.release.ReleaseClaimID = claimID
		group.release.ReleaseClaimGeneration = claimGeneration
		return m.saveGroupJournal(group, "release-intent", "")
	}
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return err
	}
	if journal.Release == nil || !journal.Release.LocalCleanupVerified {
		return fmt.Errorf("state-volume release claim preceded authenticated local cleanup")
	}
	if journal.Release.ReleaseClaimGeneration > 0 &&
		(journal.Release.ReleaseClaimID != claimID || journal.Release.ReleaseClaimGeneration > claimGeneration) {
		return fmt.Errorf("state-volume release claim was superseded")
	}
	journal.Release.ReleaseClaimID = claimID
	journal.Release.ReleaseClaimGeneration = claimGeneration
	journal.WorkerID = m.WorkerID
	journal.WorkerInstanceID = m.WorkerInstanceID
	journal.WorkerPodUID = m.WorkerPodUID
	journal.StorageNodeID = m.StorageNodeID
	journal.Phase = "release-intent"
	return m.Journals.Save(journal)
}

// DetachReleaseIntent proves the source has no remaining mount, NBD, or QSD
// resource and fsyncs that proof. The graph and journal remain recoverable
// until the repository acknowledges the exact attachment release.
func (m *StateVolumeManager) DetachReleaseIntent(ctx context.Context, containerID string) error {
	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group == nil {
		journal, err := m.Journals.Load(containerID)
		if err != nil {
			return err
		}
		if journal.Release == nil || journal.Release.ReleaseClaimID == "" || len(journal.Volumes) != 0 {
			return fmt.Errorf("release-only journal is not armed or unexpectedly names graph resources")
		}
		journal.Release.LocalCleanupVerified = true
		journal.Phase = "release-intent"
		return m.Journals.Save(journal)
	}
	if err := m.cancelAndWaitStateVolumeCompactions(ctx, group); err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.release == nil || group.release.ReleaseClaimID == "" {
		return fmt.Errorf("state-volume release is not server-escrowed")
	}
	if group.release.LocalCleanupVerified {
		return nil
	}
	group.compactionEpoch++
	if group.pending != nil || group.indeterminate {
		return ErrStateVolumePivotPending
	}
	if err := m.stopGroup(ctx, group, false); err != nil {
		return err
	}
	group.process = nil
	group.qmp = nil
	group.stopping = false
	group.release.LocalCleanupVerified = true
	return m.saveGroupJournal(group, "release-intent", "")
}

func (m *StateVolumeManager) MarkReleaseCompleted(containerID string) error {
	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group != nil {
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.release == nil || !group.release.LocalCleanupVerified {
			return fmt.Errorf("state-volume release completion preceded local cleanup")
		}
		return m.saveGroupJournal(group, "release-completed", "")
	}
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return err
	}
	if journal.Release == nil || !journal.Release.LocalCleanupVerified {
		return fmt.Errorf("state-volume release completion preceded local cleanup")
	}
	journal.Phase = "release-completed"
	return m.Journals.Save(journal)
}

// FinalizeReleaseIntent retires only a fully detached graph after the server
// has completed the escrowed release. A crash before this call leaves the
// release-completed journal available for idempotent replacement cleanup.
func (m *StateVolumeManager) FinalizeReleaseIntent(containerID string) error {
	m.mu.Lock()
	group := m.groups[containerID]
	m.mu.Unlock()
	if group == nil {
		journal, err := m.Journals.Load(containerID)
		if err != nil {
			return err
		}
		if journal.Phase != "release-completed" || journal.Release == nil || !journal.Release.LocalCleanupVerified {
			return fmt.Errorf("state-volume release is not complete")
		}
		if len(journal.Volumes) != 0 {
			if err := m.validateJournalPaths(journal); err != nil {
				return fmt.Errorf("validate completed release graph before retirement: %w", err)
			}
			retired := stateVolumeGroupFromJournal(journal)
			retired.process = nil
			retired.qmp = nil
			for _, volume := range retired.volumes {
				volume.lease = nil
				volume.connected = false
				volume.mounted = false
				volume.frozen = false
			}
			if err := m.retireGroupTransientPaths(retired); err != nil {
				return err
			}
		}
		return m.Journals.Remove(containerID)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return err
	}
	if journal.Phase != "release-completed" || group.release == nil || !group.release.LocalCleanupVerified {
		return fmt.Errorf("state-volume release is not complete")
	}
	for _, volume := range group.volumes {
		if volume.mounted || volume.connected || volume.lease != nil || volume.frozen {
			return fmt.Errorf("state-volume release still owns kernel resources")
		}
	}
	if group.process != nil || group.qmp != nil {
		return fmt.Errorf("state-volume release still owns QSD resources")
	}
	if err := m.retireGroupTransientPaths(group); err != nil {
		return err
	}
	if err := m.Journals.Remove(containerID); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.groups, containerID)
	m.mu.Unlock()
	return nil
}

func (m *StateVolumeManager) TerminalCommitOwnsRelease(containerID string) bool {
	group, err := m.group(containerID)
	if err != nil {
		return false
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.pending == nil && group.recovery != nil && group.recovery.Mode == string(StateSnapshotModeTerminal) &&
		group.acknowledgedOperationID != ""
}

// DetachPending safely releases all live kernel/QSD resources after a
// terminal runtime has stopped while retaining the immutable pending graph and
// journal for idempotent offline upload/commit replay.
func (m *StateVolumeManager) DetachPending(ctx context.Context, containerID, operationID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending == nil || group.pending.OperationID != operationID || group.indeterminate {
		return fmt.Errorf("no determinate pending state volume pivot for operation %q", operationID)
	}
	if err := m.thawStateVolumes(group.volumes); err != nil {
		return err
	}
	if err := m.stopGroup(ctx, group, false); err != nil {
		return err
	}
	group.process = nil
	group.qmp = nil
	group.stopping = false
	if err := m.saveGroupJournal(group, "detached-pending", operationID); err != nil {
		return err
	}
	return nil
}

// SealAndDetachTerminalPending is the shutdown-safe terminal failure path. A
// terminal checkpoint may stop the runtime before an object-store or control
// plane failure is observed. In that state there are no writers and the
// current writable qcow2 layers themselves are an exact consistency-group
// boundary; a replacement live child is unnecessary. Persist the receipt
// before releasing any kernel resource, then unmount, disconnect, and stop
// QSD. UploadPending remains usable against the detached immutable files.
func (m *StateVolumeManager) SealAndDetachTerminalPending(ctx context.Context, containerID, operationID string) error {
	if strings.TrimSpace(operationID) == "" {
		return fmt.Errorf("terminal state volume operation ID is empty")
	}
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.pending != nil && group.pending.OperationID != operationID {
		return ErrStateVolumePivotPending
	}
	if group.pending == nil {
		receipt := &StateVolumePivotReceipt{ContainerID: containerID, OperationID: operationID}
		for _, volume := range group.volumes {
			if volume.spec.ReadOnly {
				if volume.spec.CurrentGenerationID == "" || volume.spec.Generation <= 0 {
					return fmt.Errorf("read-only state volume %q has no immutable generation", volume.spec.ID)
				}
				receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
					VolumeID: volume.spec.ID, GenerationID: volume.spec.CurrentGenerationID,
					Generation: volume.spec.Generation, Name: volume.spec.Name,
					MountPath: volume.spec.ContainerMountPath, ReadOnly: true,
					Root: volume.spec.Root, Reused: true,
					ParentGenerationID:      volume.spec.ParentGenerationID,
					CloneParentGenerationID: volume.spec.CloneParentGenerationID,
					VirtualSizeBytes:        volume.spec.SizeBytes, Depth: volume.spec.Depth,
				})
				continue
			}
			if volume.spec.Generation < 0 || volume.spec.Depth < 1 || volume.spec.Depth > StateVolumeMaxDepth {
				return fmt.Errorf("state volume %q cannot be sealed at generation %d depth %d", volume.spec.ID, volume.spec.Generation, volume.spec.Depth)
			}
			receipt.Generations = append(receipt.Generations, StateVolumePivotGeneration{
				VolumeID:     volume.spec.ID,
				GenerationID: stateVolumeGenerationID(containerID, volume.spec.ID, operationID),
				Generation:   volume.spec.Generation + 1,
				Name:         volume.spec.Name, MountPath: volume.spec.ContainerMountPath,
				ReadOnly: false, Root: volume.spec.Root,
				LayerPath: volume.spec.ActiveLayerPath, BackingPath: volume.spec.ActiveBackingPath,
				ParentGenerationID:           volume.spec.ParentGenerationID,
				CloneParentGenerationID:      volume.spec.CloneParentGenerationID,
				Compaction:                   volume.compactionPriorGenerationID != "" && volume.spec.ActiveBackingPath == "" && volume.spec.Depth == 1,
				CompactionSourceGenerationID: volume.compactionPriorGenerationID,
				VirtualSizeBytes:             volume.spec.SizeBytes, Depth: volume.spec.Depth,
			})
		}
		group.pending = receipt
	}
	// This intent is durable before teardown. A worker replacement can either
	// adopt the still-live group or finish the safe detach; it must never retire
	// these layers as an ordinary dead container.
	if err := m.saveGroupJournal(group, "terminal-detach-intent", operationID); err != nil {
		return err
	}
	if err := m.thawStateVolumes(group.volumes); err != nil {
		return err
	}
	if err := m.stopGroup(ctx, group, false); err != nil {
		return err
	}
	group.process = nil
	group.qmp = nil
	group.stopping = false
	group.failed = false
	group.indeterminate = false
	group.writersResumedIndeterminate = false
	group.resumeRequired = false
	group.resumeHook = nil
	if err := m.saveGroupJournal(group, "detached-pending", operationID); err != nil {
		return err
	}
	return nil
}

// retireGroupTransientPaths removes a stopped container from the active
// namespace without irreversibly deleting its private layers. The retired
// tree is outside runtime/mount/container graph roots and may be reaped by the
// host cache policy after the committed CAS state is verified.
func (m *StateVolumeManager) retireGroupTransientPaths(group *stateVolumeGroup) error {
	if group == nil {
		return nil
	}
	reapRoot := filepath.Join(filepath.Dir(m.RuntimeRoot), "retired",
		stateVolumeToken("group-", group.containerID+"\x00"+time.Now().UTC().String()))
	secure := m.securePathOps()
	if err := secure.MkdirAll(reapRoot, 0700); err != nil {
		return err
	}
	for _, volume := range group.volumes {
		if volume.spec.BackingDir != "" {
			info, err := os.Lstat(volume.spec.BackingDir)
			if err == nil {
				if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("refuse to retire non-directory backing path %q", volume.spec.BackingDir)
				}
				destination := filepath.Join(reapRoot, stateVolumeToken("volume-", volume.spec.ID))
				if err := secure.Rename(volume.spec.BackingDir, destination, stateVolumeSecureDirectory, false); err != nil {
					return fmt.Errorf("retire state volume %q backing graph: %w", volume.spec.ID, err)
				}
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		if err := secure.Remove(volume.spec.MountPath, stateVolumeSecureDirectory); err != nil {
			return fmt.Errorf("remove empty state volume mount path %q: %w", volume.spec.MountPath, err)
		}
		_ = secure.Remove(filepath.Dir(volume.spec.MountPath), stateVolumeSecureDirectory)
	}
	if group.runtimeDir != "" {
		runtimeDestination := filepath.Join(reapRoot, "runtime")
		if err := secure.Rename(group.runtimeDir, runtimeDestination, stateVolumeSecureDirectory, false); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("retire QSD runtime path: %w", err)
		}
	}
	return nil
}

// QuarantineWritable stops a group in the only safe order (unmount, detach
// NBD, stop QSD) and moves every container-private writable layer out of the
// active graph. It is used after a failed memory restore: CRIU may have
// modified the mounted upperdir before returning an error, so remounting the
// same child would not be an exact cold restore of the requested snapshot.
// Quarantine is intentionally recoverable; normal transient cleanup can reap
// these files after the replacement graph is known-good.
func (m *StateVolumeManager) QuarantineWritable(ctx context.Context, containerID string) error {
	group, err := m.group(containerID)
	if err != nil {
		return err
	}
	if err := m.cancelAndWaitStateVolumeCompactions(ctx, group); err != nil {
		return err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	// Invalidate every prior worker token before any path can be retired and a
	// same-container replacement graph can be installed.
	group.compactionEpoch++
	if group.pending != nil || group.indeterminate {
		return ErrStateVolumePivotPending
	}

	for _, volume := range group.volumes {
		if volume.spec.ReadOnly {
			continue
		}
		if err := ensureStateVolumePathUnder(volume.spec.BackingDir, volume.spec.ActiveLayerPath); err != nil {
			return fmt.Errorf("refuse to quarantine state volume %q: %w", volume.spec.ID, err)
		}
	}
	if err := m.stopGroup(ctx, group, true); err != nil {
		return err
	}
	retireErr := m.retireGroupTransientPaths(group)
	m.mu.Lock()
	delete(m.groups, containerID)
	m.mu.Unlock()
	return retireErr
}

func (m *StateVolumeManager) stopGroup(ctx context.Context, group *stateVolumeGroup, removeJournal bool) error {
	if group == nil {
		return nil
	}
	// Never detach an NBD device (or stop its QSD) below a mounted ext4
	// filesystem. A failed unmount is recoverable on the next Stop call while
	// the lease, export, and journal remain intact.
	for i := len(group.volumes) - 1; i >= 0; i-- {
		volume := group.volumes[i]
		if volume.mounted {
			if err := m.Mounts.Unmount(ctx, volume.spec.MountPath); err != nil {
				return fmt.Errorf("unmount state volume %q: %w", volume.spec.ID, err)
			}
			if err := m.NBD.WaitUnmounted(ctx, volume.lease.DevicePath, volume.spec.MountPath); err != nil {
				return fmt.Errorf("verify unmounted state volume %q: %w", volume.spec.ID, err)
			}
			volume.mounted = false
		}
	}
	var disconnectErrors []error
	for i := len(group.volumes) - 1; i >= 0; i-- {
		volume := group.volumes[i]
		if err := m.disconnectStateVolumeLease(ctx, volume); err != nil {
			disconnectErrors = append(disconnectErrors, err)
		}
	}
	if len(disconnectErrors) != 0 {
		return errors.Join(disconnectErrors...)
	}
	group.stopping = true
	var expectedExecutable string
	var expectedStartTime uint64
	var processAlreadyGone bool
	if group.process != nil {
		var err error
		expectedExecutable, expectedStartTime, processAlreadyGone, err = m.captureStateVolumeProcessIdentity(group.process)
		if err != nil {
			return err
		}
	}
	if group.qmp != nil {
		_ = group.qmp.Quit(ctx)
		_ = group.qmp.Close()
	}
	if group.process != nil && !processAlreadyGone {
		if err := m.stopStateVolumeProcess(ctx, group.process, expectedExecutable, expectedStartTime); err != nil {
			return err
		}
	}
	if removeJournal {
		if err := m.Journals.Remove(group.containerID); err != nil {
			return err
		}
	}
	return nil
}

func (m *StateVolumeManager) captureStateVolumeProcessIdentity(process StateVolumeProcess) (string, uint64, bool, error) {
	if process == nil || process.PID() <= 0 {
		return "", 0, false, fmt.Errorf("state volume QSD has no valid process identity")
	}
	pid := process.PID()
	executable, startTime, err := m.ProcessIdentity(pid)
	if err != nil {
		if stateVolumeProcessIsAbsent(err) {
			return "", 0, true, nil
		}
		return "", 0, false, fmt.Errorf("authenticate state volume QSD PID %d before shutdown: %w", pid, err)
	}
	if executable == "" || startTime == 0 {
		return "", 0, false, fmt.Errorf("state volume QSD PID %d has an incomplete process identity", pid)
	}
	if bound, ok := process.(stateVolumeBoundProcessIdentity); ok {
		expectedExecutable, expectedStartTime := bound.ExpectedStateVolumeProcessIdentity()
		if expectedExecutable == "" || expectedStartTime == 0 {
			return "", 0, false, fmt.Errorf("adopted state volume QSD PID %d has an incomplete expected identity", pid)
		}
		if executable != expectedExecutable || startTime != expectedStartTime {
			// The journaled process is gone and its PID was reused. Never signal
			// the replacement; the old exact owner already satisfies the death
			// postcondition.
			return expectedExecutable, expectedStartTime, true, nil
		}
		return expectedExecutable, expectedStartTime, false, nil
	}
	return executable, startTime, false, nil
}

func (m *StateVolumeManager) stopStateVolumeProcess(ctx context.Context, process StateVolumeProcess, expectedExecutable string, expectedStartTime uint64) error {
	pid := process.PID()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	waitErr := process.Wait(waitCtx)
	cancel()
	stillExact, identityErr := m.stateVolumeProcessStillExact(pid, expectedExecutable, expectedStartTime)
	if identityErr != nil {
		return identityErr
	}
	if !stillExact {
		return nil
	}

	// Wait may return a timeout or a userspace exit-status error. Kernel
	// identity is authoritative: if the exact PID/starttime is still alive,
	// force it down and then wait a second time so no zombie/child bookkeeping
	// can be mistaken for a completed teardown.
	killErr := process.Kill()
	secondWaitCtx, secondCancel := context.WithTimeout(context.Background(), 3*time.Second)
	secondWaitErr := process.Wait(secondWaitCtx)
	secondCancel()
	stillExact, identityErr = m.stateVolumeProcessStillExact(pid, expectedExecutable, expectedStartTime)
	shutdownErrors := func(final error) error {
		errs := make([]error, 0, 4)
		if waitErr != nil {
			errs = append(errs, fmt.Errorf("wait for state volume QSD PID %d before SIGKILL: %w", pid, waitErr))
		}
		if killErr != nil {
			errs = append(errs, fmt.Errorf("kill state volume QSD PID %d: %w", pid, killErr))
		}
		if secondWaitErr != nil {
			errs = append(errs, fmt.Errorf("wait for state volume QSD PID %d after SIGKILL: %w", pid, secondWaitErr))
		}
		errs = append(errs, final)
		return errors.Join(errs...)
	}
	if identityErr != nil {
		return shutdownErrors(identityErr)
	}
	if stillExact {
		return shutdownErrors(fmt.Errorf("state volume QSD PID %d with start time %d is still alive after SIGKILL", pid, expectedStartTime))
	}
	return nil
}

func (m *StateVolumeManager) stateVolumeProcessStillExact(pid int, expectedExecutable string, expectedStartTime uint64) (bool, error) {
	executable, startTime, err := m.ProcessIdentity(pid)
	if err != nil {
		if stateVolumeProcessIsAbsent(err) {
			return false, nil
		}
		return false, fmt.Errorf("authenticate state volume QSD PID %d after shutdown: %w", pid, err)
	}
	return executable == expectedExecutable && startTime == expectedStartTime, nil
}

func stateVolumeProcessIsAbsent(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// disconnectStateVolumeLease treats the kernel NBD and mount postconditions,
// observed while the exact node-global flock is still held, as authoritative.
// A userspace nbd-client error after the kernel has already cleared is benign;
// an apparently successful command that leaves pid/size or a mount behind is
// unsafe and retains the lease, journal, and QSD for exact retry.
func (m *StateVolumeManager) disconnectStateVolumeLease(ctx context.Context, volume *stateVolumeRuntime) error {
	if volume == nil || volume.lease == nil {
		return nil
	}
	devicePath := volume.lease.DevicePath
	if err := m.NBD.WaitUnmounted(ctx, devicePath, volume.spec.MountPath); err != nil {
		return fmt.Errorf("verify unmounted state volume %q before disconnect: %w", volume.spec.ID, err)
	}
	disconnectErr := m.Connector.Disconnect(ctx, devicePath)
	postconditionErr := m.NBD.WaitDisconnected(ctx, devicePath)
	if postconditionErr != nil {
		if disconnectErr != nil {
			disconnectErr = fmt.Errorf("disconnect state volume %q: %w", volume.spec.ID, disconnectErr)
		}
		return errors.Join(disconnectErr,
			fmt.Errorf("verify disconnected state volume %q: %w", volume.spec.ID, postconditionErr))
	}
	volume.connected = false
	if err := volume.lease.Release(); err != nil {
		return fmt.Errorf("release state volume %q lease: %w", volume.spec.ID, err)
	}
	volume.lease = nil
	return nil
}

func (m *StateVolumeManager) savePivotIntent(group *stateVolumeGroup, operationID string, actions []StateVolumeSnapshotAction) error {
	return m.savePivotJournal(group, "pivot-intent", operationID, actions)
}

func (m *StateVolumeManager) savePivotJournal(group *stateVolumeGroup, phase, operationID string, actions []StateVolumeSnapshotAction) error {
	journal := m.groupJournal(group, phase, operationID)
	for i := range journal.Volumes {
		for _, action := range actions {
			if action.CurrentNode == journal.Volumes[i].ActiveNode || action.NewNode == journal.Volumes[i].ActiveNode {
				journal.Volumes[i].PivotNode = action.NewNode
				journal.Volumes[i].PivotLayerPath = action.NewPath
				break
			}
		}
	}
	return m.Journals.Save(journal)
}

type StateVolumePivotOutcome string

const (
	StateVolumePivotCommitted  StateVolumePivotOutcome = "committed"
	StateVolumePivotRolledBack StateVolumePivotOutcome = "rolled_back"
)

// InspectJournalPivot is the only safe recovery decision after a transaction
// response is lost. Callers must never retry the QMP transaction based only on
// the presence or absence of the target qcow2 files.
func (m *StateVolumeManager) InspectJournalPivot(ctx context.Context, containerID string) (StateVolumePivotOutcome, error) {
	if err := m.defaults(); err != nil {
		return "", err
	}
	journal, err := m.Journals.Load(containerID)
	if err != nil {
		return "", err
	}
	if journal.Phase != "pivot-intent" && journal.Phase != "pivot-indeterminate" {
		return "", fmt.Errorf("state volume journal is not awaiting pivot reconciliation (phase %q)", journal.Phase)
	}
	qmp, err := m.QMPDialer.Dial(ctx, journal.QMPSocket)
	if err != nil {
		return "", fmt.Errorf("reconnect QMP for pivot reconciliation: %w", err)
	}
	defer qmp.Close()
	outcome, err := inspectJournalPivotGraph(ctx, qmp, journal)
	if err != nil {
		return "", fmt.Errorf("query QSD graph for pivot reconciliation: %w", err)
	}
	return outcome, nil
}

func (m *StateVolumeManager) saveGroupJournal(group *stateVolumeGroup, phase, operationID string) error {
	return m.Journals.Save(m.groupJournal(group, phase, operationID))
}

func (m *StateVolumeManager) groupJournal(group *stateVolumeGroup, phase, operationID string) StateVolumeJournal {
	journal := StateVolumeJournal{
		Version: stateVolumeJournalVersion, ContainerID: group.containerID,
		WorkerID: m.WorkerID, WorkerInstanceID: m.WorkerInstanceID, WorkerPodUID: m.WorkerPodUID, StorageNodeID: m.StorageNodeID,
		SourceStateSnapshotID: group.sourceStateSnapshotID, QMPSocket: group.qmpSocket,
		NBDSocket: group.nbdSocket, OperationID: operationID, Phase: phase,
	}
	if group.recovery != nil {
		copyEnvelope := *group.recovery
		journal.Recovery = &copyEnvelope
	}
	if group.release != nil {
		copyEnvelope := *group.release
		copyEnvelope.Members = append([]StateVolumeReleaseMember(nil), group.release.Members...)
		journal.Release = &copyEnvelope
	}
	if group.process != nil {
		journal.QSDPID = group.process.PID()
		journal.QSDExecutable, journal.QSDStartTime, _ = m.ProcessIdentity(group.process.PID())
	}
	pending := make(map[string]StateVolumePivotGeneration)
	if group.pending != nil {
		for _, generation := range group.pending.Generations {
			pending[generation.VolumeID] = generation
		}
	}
	for _, volume := range group.volumes {
		journal.Volumes = append(journal.Volumes, StateVolumeJournalVolume{
			ID: volume.spec.ID, Name: volume.spec.Name, ContainerMountPath: volume.spec.ContainerMountPath,
			Root: volume.spec.Root, ReadOnly: volume.spec.ReadOnly, Initialize: volume.spec.Format,
			CreateLayer: volume.spec.CreateLayer, Prepared: volume.prepared, Generation: volume.spec.Generation,
			ExportName: volume.exportName, DevicePath: volume.devicePath,
			BackingDir: volume.spec.BackingDir, MountPath: volume.spec.MountPath, SizeBytes: volume.spec.SizeBytes,
			RootNode: volume.rootNode, FileNode: volume.fileNode, ActiveNode: volume.activeNode,
			ActiveLayerPath: volume.spec.ActiveLayerPath, ActiveBackingPath: volume.spec.ActiveBackingPath,
			CurrentGenerationID:           volume.spec.CurrentGenerationID,
			LineageSourceGenerationID:     volume.spec.LineageSourceGenerationID,
			SourceVolumeID:                volume.spec.SourceVolumeID,
			SourceGeneration:              volume.spec.SourceGeneration,
			SourceParentGenerationID:      volume.spec.SourceParentGenerationID,
			SourceCloneParentGenerationID: volume.spec.SourceCloneParentGenerationID,
			SourceDepth:                   volume.spec.SourceDepth,
			ParentGenerationID:            volume.spec.ParentGenerationID,
			CloneParentGenerationID:       volume.spec.CloneParentGenerationID,
			// Attachment credentials are deliberately never persisted. The fencing
			// token is a non-secret lineage identity; release authority is escrowed
			// by the repository before local detach.
			FencingToken:    volume.spec.FencingToken,
			Depth:           volume.spec.Depth,
			CompactionJobID: volume.compactionJobID, CompactionPhase: volume.compactionPhase,
			CompactionNode: volume.compactionNode, CompactionLayerPath: volume.compactionLayerPath,
			CompactionBackingPath:       volume.compactionBackingPath,
			CompactionPriorGenerationID: volume.compactionPriorGenerationID,
		})
		if generation, ok := pending[volume.spec.ID]; ok {
			last := &journal.Volumes[len(journal.Volumes)-1]
			last.PendingGenerationID = generation.GenerationID
			last.PendingGeneration = generation.Generation
			last.PendingReused = generation.Reused
			last.PendingLayerPath = generation.LayerPath
			last.PendingBackingPath = generation.BackingPath
			last.PendingParentGenerationID = generation.ParentGenerationID
			last.PendingCloneParentGenerationID = generation.CloneParentGenerationID
			last.PendingCompaction = generation.Compaction
			last.PendingCompactionSourceGenerationID = generation.CompactionSourceGenerationID
			last.PendingDepth = generation.Depth
		}
	}
	return journal
}

func stateVolumeProcessIdentity(pid int) (string, uint64, error) {
	if pid <= 0 {
		return "", 0, fmt.Errorf("invalid process id %d", pid)
	}
	executable, err := os.Readlink(filepath.Join("/proc", fmt.Sprint(pid), "exe"))
	if err != nil {
		return "", 0, err
	}
	stat, err := os.ReadFile(filepath.Join("/proc", fmt.Sprint(pid), "stat"))
	if err != nil {
		return "", 0, err
	}
	closing := strings.LastIndexByte(string(stat), ')')
	if closing < 0 {
		return "", 0, fmt.Errorf("invalid /proc stat for pid %d", pid)
	}
	fields := strings.Fields(string(stat[closing+1:]))
	// Field 22 (starttime) is index 19 after removing pid and comm.
	if len(fields) <= 19 {
		return "", 0, fmt.Errorf("short /proc stat for pid %d", pid)
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return "", 0, err
	}
	return executable, startTime, nil
}
