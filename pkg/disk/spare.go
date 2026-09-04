package disk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
)

// Fresh disks are the common case for new sandboxes, and most of their attach
// time is fixed per-device kernel work: the NBD connect alone spends tens of
// milliseconds freezing block queues, and mkfs runs on top of that. A spare is
// a fresh formatted volume brought online ahead of time under a private key;
// adopting one costs a rename of the key index and a mount.
//
// A spare's directory never moves. The daemon holds the head open by that
// path and block-commit addresses layers by filename, so volumes/<key> becomes
// a symlink to the spare's directory instead.
//
// Spares are best effort: they stop building when few NBD devices remain and
// are released when a real attach finds none.
const (
	sparePrefix = ".spare-"
	// spareTarget is how many spares are kept per virtual size once a size has
	// been requested. Two covers the usual pair of containers a worker starts
	// together.
	spareTarget = 2
	// spareDeviceReserve keeps spares from taking the last few NBD devices;
	// those are for real attaches, and other workers on the same node share
	// the pool.
	spareDeviceReserve = 4
)

func isSpareKey(key string) bool { return strings.HasPrefix(key, sparePrefix) }

// resolveVolumeDir returns the directory holding key's state, following the
// index symlink of an adopted spare. A dangling link is removed.
func (m *Manager) resolveVolumeDir(key string) (string, error) {
	dir := m.volumeDir(key)
	info, err := os.Lstat(dir)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return dir, nil
	}
	target, err := filepath.EvalSymlinks(dir)
	if err == nil {
		return target, nil
	}
	if os.IsNotExist(err) {
		return dir, os.Remove(dir)
	}
	return "", err
}

// adoptSpare hands a spare of the requested size to spec's key. On failure the
// spare is destroyed and nil is returned so the caller builds the volume the
// slow way.
func (m *Manager) adoptSpare(ctx context.Context, spec AttachSpec) *Volume {
	spare := m.takeSpare(spec.VirtualSizeBytes)
	if spare == nil {
		return nil
	}
	if err := spare.adopt(ctx, spec); err != nil {
		log.Warn().Err(err).Str("volume", spec.Key).Msg("failed to adopt spare qcow volume")
		spare.destroy()
		if info, err := os.Lstat(m.volumeDir(spec.Key)); err == nil && info.Mode()&os.ModeSymlink != 0 {
			_ = os.Remove(m.volumeDir(spec.Key))
		}
		return nil
	}
	return spare
}

// adopt rekeys a spare: the key is indexed at the spare's directory, the
// runtime directory moves under the key (the daemon's open sockets follow),
// the intent is persisted, and the filesystem is mounted last. A crash before
// the state save leaves a spare, which recovery removes; one after it leaves
// an ordinary crashed volume with nothing written yet.
func (v *Volume) adopt(ctx context.Context, spec AttachSpec) error {
	m := v.manager
	keyDir := m.volumeDir(spec.Key)
	if err := os.RemoveAll(keyDir); err != nil {
		return err
	}
	if err := os.Symlink(v.dir, keyDir); err != nil {
		return err
	}
	runtimeDir := m.runtimeDir(spec.Key)
	if err := os.Rename(v.qsd.runtimeDir, runtimeDir); err != nil {
		return err
	}
	v.qsd.runtimeDir = runtimeDir
	v.qsd.qmpSocket = filepath.Join(runtimeDir, filepath.Base(v.qsd.qmpSocket))
	v.qsd.nbdSocket = filepath.Join(runtimeDir, filepath.Base(v.qsd.nbdSocket))
	v.state.Key = spec.Key
	v.state.QMPSocket = v.qsd.qmpSocket
	v.state.NBDSocket = v.qsd.nbdSocket
	v.state.Mountpoint = spec.Mountpoint
	if err := saveVolumeState(v.dir, v.state); err != nil {
		return err
	}
	return m.mountExt4(ctx, v.nbd.Path, spec.Mountpoint, false)
}

// destroy tears a spare down and removes its directory; spares hold no data.
func (v *Volume) destroy() {
	ctx := context.Background()
	if v.nbd != nil {
		_ = v.manager.disconnectNBDDevice(ctx, v.nbd)
	}
	_ = v.manager.stopQSD(ctx, v.qsd)
	_ = os.RemoveAll(v.manager.runtimeDir(v.state.Key))
	_ = os.RemoveAll(v.dir)
}

func (m *Manager) takeSpare(size int64) *Volume {
	m.mu.Lock()
	defer m.mu.Unlock()
	pool := m.spares[size]
	if len(pool) == 0 {
		return nil
	}
	spare := pool[len(pool)-1]
	m.spares[size] = pool[:len(pool)-1]
	return spare
}

// replenishSpares tops the pool for size up to spareTarget in the background.
// One builder runs per size at a time.
func (m *Manager) replenishSpares(size int64) {
	m.mu.Lock()
	if m.closed || m.spareBuilds[size] || len(m.spares[size]) >= spareTarget {
		m.mu.Unlock()
		return
	}
	m.spareBuilds[size] = true
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.spareBuilds, size)
			m.mu.Unlock()
		}()
		for {
			m.mu.Lock()
			done := m.closed || len(m.spares[size]) >= spareTarget
			m.mu.Unlock()
			if done {
				return
			}
			if m.freeNBDDevices() <= spareDeviceReserve {
				return
			}
			spare, err := m.buildSpare(context.Background(), size)
			if err != nil {
				log.Warn().Err(err).Int64("size", size).Msg("failed to build spare qcow volume")
				return
			}
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				spare.destroy()
				return
			}
			m.spares[size] = append(m.spares[size], spare)
			m.mu.Unlock()
		}
	}()
}

// buildSpare creates, formats, and brings online a fresh volume under a spare
// key. It is not mounted.
func (m *Manager) buildSpare(ctx context.Context, size int64) (*Volume, error) {
	var id [6]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	key := sparePrefix + hex.EncodeToString(id[:])
	dir := m.volumeDir(key)
	layersDir := filepath.Join(dir, layersSubdir)
	if err := os.MkdirAll(layersDir, 0o700); err != nil {
		return nil, err
	}
	headPath := headLayerPath(layersDir, 0)
	if err := m.createQcowBase(ctx, headPath, size); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	spare := &Volume{
		manager:   m,
		dir:       dir,
		state:     &volumeState{Key: key, VirtualSizeBytes: size, HeadPath: headPath},
		freshHead: true,
	}
	if err := spare.start(ctx); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return spare, nil
}

// reclaimSpare destroys one spare of any size to free its NBD device for a
// real attach. It reports whether a spare was released.
func (m *Manager) reclaimSpare() bool {
	m.mu.Lock()
	var victim *Volume
	for size, pool := range m.spares {
		if len(pool) == 0 {
			continue
		}
		victim = pool[len(pool)-1]
		m.spares[size] = pool[:len(pool)-1]
		break
	}
	m.mu.Unlock()
	if victim == nil {
		return false
	}
	log.Info().Str("volume", victim.state.Key).Msg("releasing spare qcow volume: nbd devices exhausted")
	victim.destroy()
	return true
}

// destroySpares empties the pool and stops replenishment.
func (m *Manager) destroySpares() {
	m.mu.Lock()
	m.closed = true
	spares := m.spares
	m.spares = make(map[int64][]*Volume)
	m.mu.Unlock()
	for _, pool := range spares {
		for _, spare := range pool {
			spare.destroy()
		}
	}
}
