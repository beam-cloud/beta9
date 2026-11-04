package disk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// PruneDetached reclaims old local volume copies whose entire contents are
// already in their published backing chain. Pending seals, journals, active
// attachments, and unpublished head writes are never discarded. State mtime
// is renewed by attach/detach, so the cutoff follows actual volume activity.
func (m *Manager) PruneDetached(ctx context.Context, cutoff time.Time) (int, int64, error) {
	return m.PruneDetachedWithBudget(ctx, cutoff, 0)
}

// Root is the filesystem containing this manager's local volume copies.
func (m *Manager) Root() string { return m.root }

// PruneDetachedWithBudget also evicts recent, fully published copies under disk
// pressure, oldest first. TTL still applies after the pressure target is met.
// Active volumes and unpublished writes retain the same protection as above.
func (m *Manager) PruneDetachedWithBudget(ctx context.Context, cutoff time.Time, bytesToFree int64) (int, int64, error) {
	if cutoff.IsZero() && bytesToFree <= 0 {
		return 0, 0, nil
	}
	m.cachePruneMu.Lock()
	defer m.cachePruneMu.Unlock()
	// Interrupted deletions contain only copies already proven recoverable.
	leftovers, err := os.ReadDir(m.root)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, 0, err
	}
	for _, entry := range leftovers {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".evict-volume-") {
			if err := os.RemoveAll(filepath.Join(m.root, entry.Name())); err != nil {
				return 0, 0, err
			}
		}
	}
	root := filepath.Join(m.root, "volumes")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var freed int64
	pruned := 0
	type candidate struct {
		dir    string
		before os.FileInfo
	}
	candidates := make([]candidate, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		before, err := os.Stat(filepath.Join(dir, stateFileName))
		if err == nil && before.Mode().IsRegular() {
			candidates = append(candidates, candidate{dir: dir, before: before})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].before.ModTime().Before(candidates[j].before.ModTime()) })
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return pruned, freed, err
		}
		dir, before := candidate.dir, candidate.before
		statePath := filepath.Join(dir, stateFileName)
		if (cutoff.IsZero() || !before.ModTime().Before(cutoff)) && freed >= bytesToFree {
			continue
		}
		state, err := loadVolumeState(dir)
		if err != nil || !detachedCacheCandidate(state) {
			continue
		}
		// QEMU takes its normal file locks here; never force-share with a live
		// daemon. Probe without the manager mutex so unrelated starts proceed.
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		recoverable := m.detachedHeadPublished(probeCtx, state)
		cancel()
		if !recoverable {
			continue
		}

		m.mu.Lock()
		_, attaching := m.volumes[state.Key]
		after, statErr := os.Stat(statePath)
		unchanged := statErr == nil && os.SameFile(before, after) && before.ModTime().Equal(after.ModTime()) && before.Size() == after.Size()
		if attaching || !unchanged {
			m.mu.Unlock()
			continue
		}
		keyDir := m.volumeDir(state.Key)
		if keyDir != dir {
			// Adopted spares keep their physical directory and index it by key.
			target, err := filepath.EvalSymlinks(keyDir)
			if err != nil || filepath.Clean(target) != filepath.Clean(dir) {
				m.mu.Unlock()
				continue
			}
		}
		// Rename while admission is excluded, then remove files without holding
		// the mutex. An attach after this point rebuilds from origin/cache.
		tombstone, err := os.MkdirTemp(m.root, ".evict-volume-")
		if err == nil {
			err = os.Rename(dir, filepath.Join(tombstone, "volume"))
		}
		if err == nil && keyDir != dir {
			err = os.Remove(keyDir)
			if err != nil {
				_ = os.Rename(filepath.Join(tombstone, "volume"), dir)
			}
		}
		m.mu.Unlock()
		if err != nil {
			if tombstone != "" {
				_ = os.Remove(tombstone)
			}
			return pruned, freed, err
		}
		size := allocatedVolumeBytes(tombstone)
		if err := os.RemoveAll(tombstone); err != nil {
			return pruned, freed, err
		}
		pruned++
		freed += size
	}
	return pruned, freed, nil
}

func detachedCacheCandidate(state *volumeState) bool {
	if state == nil || state.Key == "" || state.Key != filepath.Base(state.Key) ||
		isSpareKey(state.Key) || state.Attached || state.Journal || len(state.Pending) != 0 || len(state.Chain) == 0 {
		return false
	}
	for _, layer := range state.Chain {
		if layer.SnapshotID == "" || layer.Path == "" {
			return false
		}
	}
	return true
}

func (m *Manager) detachedHeadPublished(ctx context.Context, state *volumeState) bool {
	if state.ReadOnly {
		return state.HeadPath == ""
	}
	if state.HeadPath == "" {
		return false
	}
	output, err := m.run(ctx, m.binaries.QemuImg, "info", "--output=json", "-f", "qcow2", state.HeadPath)
	var info qcowInfo
	if err != nil || json.Unmarshal(output, &info) != nil || info.VirtualSize != state.VirtualSizeBytes {
		return false
	}
	parent := state.Chain[len(state.Chain)-1].Path
	backing := info.BackingFile
	if !filepath.IsAbs(backing) {
		backing = filepath.Join(filepath.Dir(state.HeadPath), backing)
	}
	if filepath.Clean(backing) != filepath.Clean(parent) {
		return false
	}
	output, err = m.run(ctx, m.binaries.QemuImg, "map", "--output=json", "-f", "qcow2", state.HeadPath)
	if err != nil {
		return false
	}
	return emptyQcowHead(output, state.VirtualSizeBytes) == nil
}

// qemu-img map reports guest sectors, including optimized zero clusters. A
// depth-zero present extent is a head write, even when it contains only zeros.
// Missing fields or gaps are not proof that a local head is disposable.
func emptyQcowHead(output []byte, size int64) error {
	var extents []struct {
		Start   int64 `json:"start"`
		Length  int64 `json:"length"`
		Depth   *int  `json:"depth"`
		Present *bool `json:"present"`
	}
	if err := json.Unmarshal(output, &extents); err != nil {
		return err
	}
	var end int64
	for _, extent := range extents {
		if extent.Start != end || extent.Length <= 0 || extent.Length > size-end ||
			extent.Depth == nil || *extent.Depth < 0 || extent.Present == nil {
			return fmt.Errorf("incomplete qcow allocation map")
		}
		if *extent.Depth == 0 && *extent.Present {
			return fmt.Errorf("qcow head contains unpublished sectors")
		}
		end += extent.Length
	}
	if end != size || size <= 0 {
		return fmt.Errorf("incomplete qcow allocation map")
	}
	return nil
}

func allocatedVolumeBytes(root string) int64 {
	var size int64
	_ = filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if info, err := entry.Info(); err == nil {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && (info.IsDir() || stat.Nlink == 1) {
				size += stat.Blocks * 512
			}
		}
		return nil
	})
	return size
}
