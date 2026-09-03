// Package disk implements qcow2-backed durable disks for workers.
//
// Each volume is a chain of immutable qcow2 layers plus a writable head,
// served by one qemu-storage-daemon over NBD and mounted as ext4. A snapshot
// is a metadata-only pivot: freeze the filesystem, redirect writes to a new
// empty overlay, thaw. The sealed layer is then shipped to object storage as
// sparse content-addressed chunks, and restores rebuild the chain from those
// chunks (via the node-local content cache when available).
package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog/log"
)

const (
	// DefaultRoot is where volumes, layers, and runtime state live on a worker.
	DefaultRoot = "/var/lib/beta9/qcow-disks"

	// DefaultMaxChainDepth is a backstop on the local backing chain. Compact
	// keeps chains shallow; the cap only trips if compaction keeps failing.
	DefaultMaxChainDepth = 64

	// DefaultFlattenDepth is the published chain length at which the adapter
	// publishes a flattened parentless generation instead of a delta.
	DefaultFlattenDepth = 16
)

// Binaries are the external tools the engine drives. Empty fields default to
// $PATH lookups.
type Binaries struct {
	QemuImg   string
	QSD       string
	NBDClient string
	Modprobe  string
	Mknod     string
	Stat      string
	MkfsExt4  string
	Fsfreeze  string
	Mount     string
	Umount    string
}

func (b *Binaries) applyDefaults() {
	setDefault(&b.QemuImg, "qemu-img")
	setDefault(&b.QSD, "qemu-storage-daemon")
	setDefault(&b.NBDClient, "nbd-client")
	setDefault(&b.Modprobe, "modprobe")
	setDefault(&b.Mknod, "mknod")
	setDefault(&b.Stat, "stat")
	setDefault(&b.MkfsExt4, "mkfs.ext4")
	setDefault(&b.Fsfreeze, "fsfreeze")
	setDefault(&b.Mount, "mount")
	setDefault(&b.Umount, "umount")
}

func setDefault(field *string, value string) {
	if *field == "" {
		*field = value
	}
}

// qsdComm is the process comm name used for identity checks before signaling.
// The kernel truncates comm to 15 bytes.
func (b Binaries) qsdComm() string {
	comm := filepath.Base(b.QSD)
	if len(comm) > 15 {
		comm = comm[:15]
	}
	return comm
}

type Config struct {
	Root          string
	Binaries      Binaries
	MaxChainDepth int
	// Debug adds per-phase timings to attach logs.
	Debug bool

	// Test hooks.
	SysBlockPath string
	DevPath      string
	Runner       func(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Manager owns every qcow2 volume on a worker node.
type Manager struct {
	root          string
	binaries      Binaries
	sysBlockPath  string
	devPath       string
	run           runner
	maxChainDepth int
	debug         bool

	mu      sync.Mutex
	volumes map[string]*Volume

	preflightOnce sync.Once
	preflightErr  error
}

func NewManager(config Config) *Manager {
	if config.Root == "" {
		config.Root = DefaultRoot
	}
	if config.SysBlockPath == "" {
		config.SysBlockPath = "/sys/block"
	}
	if config.DevPath == "" {
		config.DevPath = "/dev"
	}
	if config.MaxChainDepth <= 0 {
		config.MaxChainDepth = DefaultMaxChainDepth
	}
	config.Binaries.applyDefaults()
	run := runner(execRunner)
	if config.Runner != nil {
		run = config.Runner
	}
	return &Manager{
		root:          config.Root,
		binaries:      config.Binaries,
		sysBlockPath:  config.SysBlockPath,
		devPath:       config.DevPath,
		run:           run,
		maxChainDepth: config.MaxChainDepth,
		debug:         config.Debug,
		volumes:       make(map[string]*Volume),
	}
}

func (m *Manager) volumeDir(key string) string {
	return filepath.Join(m.root, "volumes", key)
}

// runtimeDir is where a volume's QMP/NBD sockets and pidfile live. It hashes
// the key instead of embedding it: volume keys carry workspace UUIDs, and the
// full path must stay well under the 108-byte unix socket path limit.
func (m *Manager) runtimeDir(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(m.root, "run", hex.EncodeToString(sum[:6]))
}

func (m *Manager) lockDir() string {
	return filepath.Join(m.root, "nbd-locks")
}

// preflight verifies the required binaries exist. It runs once, on first use,
// so workers that never attach a qcow disk don't need qemu installed.
func (m *Manager) preflight() error {
	m.preflightOnce.Do(func() {
		for _, binary := range []string{
			m.binaries.QemuImg, m.binaries.QSD, m.binaries.NBDClient,
			m.binaries.Modprobe, m.binaries.Mknod, m.binaries.Stat,
			m.binaries.MkfsExt4, m.binaries.Fsfreeze, m.binaries.Mount, m.binaries.Umount,
		} {
			if _, err := exec.LookPath(binary); err != nil {
				m.preflightErr = fmt.Errorf("qcow disk support requires %s: %w", binary, err)
				return
			}
		}
		m.preflightErr = os.MkdirAll(m.root, 0o700)
	})
	return m.preflightErr
}

// Attach brings a volume online and registers it under its key.
func (m *Manager) Attach(ctx context.Context, spec AttachSpec, source ChunkSource) (*Volume, error) {
	if err := m.preflight(); err != nil {
		return nil, err
	}
	if spec.Key == "" || spec.Key != filepath.Base(spec.Key) {
		return nil, fmt.Errorf("invalid volume key %q", spec.Key)
	}
	if spec.VirtualSizeBytes <= 0 {
		return nil, fmt.Errorf("volume %s requires a positive size", spec.Key)
	}
	if spec.Mountpoint == "" {
		return nil, fmt.Errorf("volume %s requires a mountpoint", spec.Key)
	}

	m.mu.Lock()
	if _, exists := m.volumes[spec.Key]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("volume %s is already attached", spec.Key)
	}
	// Reserve the key while attaching so concurrent attaches cannot race.
	m.volumes[spec.Key] = nil
	m.mu.Unlock()

	volume, err := m.attach(ctx, spec, source)

	m.mu.Lock()
	if err != nil {
		delete(m.volumes, spec.Key)
	} else {
		m.volumes[spec.Key] = volume
	}
	m.mu.Unlock()
	return volume, err
}

// Volume returns the attached volume registered under key.
func (m *Manager) Volume(key string) (*Volume, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	volume, ok := m.volumes[key]
	return volume, ok && volume != nil
}

// Detach takes a volume offline. Unknown keys are a no-op so cleanup paths
// can call this unconditionally.
func (m *Manager) Detach(ctx context.Context, key string) error {
	m.mu.Lock()
	volume, ok := m.volumes[key]
	m.mu.Unlock()
	if !ok || volume == nil {
		return nil
	}
	if err := volume.detach(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.volumes, key)
	m.mu.Unlock()
	return nil
}

// DetachAll tears down every volume still owned by this worker. It is safe to
// call after the last container exits and during worker shutdown.
func (m *Manager) DetachAll(ctx context.Context) error {
	m.mu.Lock()
	keys := make([]string, 0, len(m.volumes))
	for key, volume := range m.volumes {
		if volume != nil {
			keys = append(keys, key)
		}
	}
	m.mu.Unlock()

	var errs error
	for _, key := range keys {
		if err := m.Detach(ctx, key); err != nil {
			errs = errors.Join(errs, fmt.Errorf("detach volume %s: %w", key, err))
		}
	}
	return errs
}

// Recover sweeps volume state left behind by a previous worker process. Live
// volumes (daemon running and filesystem mounted) are adopted; everything
// else is torn down. Layer files always stay behind for reuse.
func (m *Manager) Recover(ctx context.Context) error {
	volumesDir := filepath.Join(m.root, "volumes")
	entries, err := os.ReadDir(volumesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(entries) > 0 {
		if err := m.ensureNBDDevices(ctx); err != nil {
			return err
		}
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(volumesDir, entry.Name())
		state, err := loadVolumeState(dir)
		if err != nil || state == nil || !state.Attached {
			continue
		}
		if m.adoptVolume(dir, state) {
			log.Info().Str("volume", state.Key).Int("qsd_pid", state.QSDPid).Msg("adopted live qcow volume")
			continue
		}
		log.Warn().Str("volume", state.Key).Msg("cleaning up crashed qcow volume")
		m.cleanupCrashedVolume(ctx, dir, state)
	}
	return nil
}

// adoptVolume re-registers a volume whose daemon and mount survived a worker
// restart. The NBD lock is re-acquired to fence out other processes.
func (m *Manager) adoptVolume(dir string, state *volumeState) bool {
	if !processAlive(state.QSDPid, m.binaries.qsdComm()) || !isMountpoint(state.Mountpoint) {
		return false
	}
	deviceName := filepath.Base(state.NBDDevice)
	device, ok := m.lockNBDDevice(deviceName)
	if !ok {
		return false
	}

	volume := &Volume{
		manager: m,
		dir:     dir,
		state:   state,
		fmtNode: fmtNodeName(state.PivotCount),
		qsd: &qsdProcess{
			pid:        state.QSDPid,
			qmpSocket:  state.QMPSocket,
			nbdSocket:  state.NBDSocket,
			runtimeDir: m.runtimeDir(state.Key),
		},
		nbd: device,
	}
	if err := volume.reconcileHeadNode(context.Background()); err != nil {
		log.Warn().Str("volume", state.Key).Err(err).Msg("failed to reconcile adopted volume head")
		device.release()
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.volumes[state.Key]; exists {
		device.release()
		return false
	}
	m.volumes[state.Key] = volume
	return true
}

func (m *Manager) cleanupCrashedVolume(ctx context.Context, dir string, state *volumeState) {
	if state.Mountpoint != "" {
		if err := m.unmount(ctx, state.Mountpoint); err != nil {
			log.Warn().Str("volume", state.Key).Err(err).Msg("failed to unmount crashed volume")
		}
	}
	if state.NBDDevice != "" {
		if device, ok := m.lockNBDDevice(filepath.Base(state.NBDDevice)); ok {
			if m.nbdDeviceBusy(device.name) {
				if err := m.disconnectNBDDevice(ctx, device); err != nil {
					log.Warn().Str("volume", state.Key).Err(err).Msg("failed to disconnect crashed volume device")
				}
			} else {
				device.release()
			}
		}
	}
	killProcess(state.QSDPid, m.binaries.qsdComm())

	state.Attached = false
	state.QSDPid = 0
	state.QMPSocket = ""
	state.NBDSocket = ""
	state.NBDDevice = ""
	if err := saveVolumeState(dir, state); err != nil {
		log.Warn().Str("volume", state.Key).Err(err).Msg("failed to persist recovered volume state")
	}
	_ = os.RemoveAll(m.runtimeDir(state.Key))
}
