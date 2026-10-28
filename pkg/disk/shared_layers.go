package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

// Published layers are immutable and a generation's parent never changes, so
// every volume on a node whose chain holds a generation can back onto one copy
// of it: VMs created from the same template download its layers once. A layer
// is fetched into the node's layer store, rebased onto its parent's copy there
// and only then installed under its final name. Volumes reference installed
// layers in place and keep only their own layers and head in their directory.
// Nothing writes to or deletes an installed layer while a volume references
// it: merges commit only into a volume's own layers (see merge).
const sharedLayersSubdir = "shared-layers"

const partialLayerSuffix = ".partial"

func (m *Manager) sharedLayersDir() string {
	return filepath.Join(m.root, sharedLayersSubdir)
}

// sharedLayerPath names a generation's copy in the layer store. The parent is
// part of the name, so a copy is only reused on top of the parent it was
// rebased onto.
func (m *Manager) sharedLayerPath(snapshotID, parentID string) (string, error) {
	if snapshotID == "" || snapshotID != filepath.Base(snapshotID) || strings.HasPrefix(snapshotID, ".") {
		return "", fmt.Errorf("invalid snapshot id %q", snapshotID)
	}
	name := snapshotID
	if parentID != "" {
		sum := sha256.Sum256([]byte(parentID))
		name += "." + hex.EncodeToString(sum[:6])
	}
	return filepath.Join(m.sharedLayersDir(), name+".qcow2"), nil
}

func (m *Manager) isSharedLayer(path string) bool {
	return filepath.Dir(path) == m.sharedLayersDir()
}

// ensureSharedLayer installs a published generation at path unless a copy is
// already there. Concurrent attaches of the same generation share one fetch.
func (m *Manager) ensureSharedLayer(ctx context.Context, layer ChainLayer, path, parentPath string, source ChunkSource, gate chunkGate) error {
	unlock, err := m.layerLocks.lock(ctx, path)
	if err != nil {
		return err
	}
	defer unlock()
	if fileExists(path) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	partial := path + partialLayerSuffix
	defer os.Remove(partial)
	if err := fetchLayer(ctx, source, layer.Layer, partial, gate); err != nil {
		return err
	}
	if parentPath != "" {
		// Downloaded layers carry the origin host's backing path. An unsafe
		// rebase only rewrites the header, so the parent need not exist yet.
		if err := m.rebaseQcow(ctx, partial, parentPath); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(partial, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return replaceFileAtomic(file, path)
}

// pruneSharedLayers removes layer store files no volume references, including
// fetches a crash interrupted. Recovery runs it before anything can attach.
func (m *Manager) pruneSharedLayers() {
	entries, err := os.ReadDir(m.sharedLayersDir())
	if err != nil {
		return
	}
	referenced, ok := m.referencedLayerPaths()
	if !ok {
		return
	}
	removed := 0
	for _, entry := range entries {
		path := filepath.Join(m.sharedLayersDir(), entry.Name())
		if entry.IsDir() || referenced[path] {
			continue
		}
		if err := os.Remove(path); err == nil {
			removed++
		}
	}
	if removed > 0 {
		log.Info().Int("layers", removed).Msg("pruned unreferenced shared qcow layers")
	}
}

// referencedLayerPaths collects every layer a volume's state names. It
// reports false when a state cannot be read, since that volume might
// reference any layer.
func (m *Manager) referencedLayerPaths() (map[string]bool, bool) {
	referenced := make(map[string]bool)
	volumesDir := filepath.Join(m.root, "volumes")
	entries, err := os.ReadDir(volumesDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, false
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		state, err := loadVolumeState(filepath.Join(volumesDir, entry.Name()))
		if err != nil {
			return nil, false
		}
		if state == nil {
			continue
		}
		for _, layer := range state.Chain {
			referenced[layer.Path] = true
		}
		for _, layer := range state.Pending {
			referenced[layer.Path] = true
		}
	}
	return referenced, true
}

// keyedLock serializes work per key; a waiter gives up when its context ends.
type keyedLock struct {
	mu   sync.Mutex
	held map[string]chan struct{}
}

func (l *keyedLock) lock(ctx context.Context, key string) (func(), error) {
	for {
		l.mu.Lock()
		if l.held == nil {
			l.held = make(map[string]chan struct{})
		}
		done, busy := l.held[key]
		if !busy {
			done = make(chan struct{})
			l.held[key] = done
			l.mu.Unlock()
			return func() {
				l.mu.Lock()
				delete(l.held, key)
				l.mu.Unlock()
				close(done)
			}, nil
		}
		l.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
