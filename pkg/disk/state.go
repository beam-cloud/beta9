package disk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Volume directory layout: layer files under layersSubdir, the durability
// record beside them.
const (
	stateFileName = "state.json"
	layersSubdir  = "layers"
)

// headLayerPath names the writable head created by pivot number n. The
// counter keeps names unique so a sealed layer is never overwritten.
func headLayerPath(layersDir string, pivot int) string {
	return filepath.Join(layersDir, fmt.Sprintf("head-%06d.qcow2", pivot))
}

// volumeState is the single per-volume durability record. It is written with
// an atomic rename before and after every mutating step, and is all that
// recovery needs to clean up or adopt a volume after a crash.
type volumeState struct {
	Key              string `json:"key"`
	VirtualSizeBytes int64  `json:"virtual_size_bytes"`
	ReadOnly         bool   `json:"read_only"`
	Mountpoint       string `json:"mountpoint,omitempty"`
	Attached         bool   `json:"attached"`
	// Formatted records that mkfs ran on the base image; it prevents a reused
	// but never-snapshotted disk from being wiped by a second mkfs.
	Formatted bool `json:"formatted"`

	// Chain is the published backing chain, base first. Every layer file is
	// immutable once listed here.
	Chain []stateLayer `json:"chain,omitempty"`
	// Pending are sealed layers that have not been published yet. They sit
	// between Chain and Head in backing order and must be published (oldest
	// first) before any newer layer, or the remote chain would lose clusters.
	Pending []stateLayer `json:"pending,omitempty"`
	// HeadPath is the writable tip. Empty for read-only volumes.
	HeadPath   string `json:"head_path,omitempty"`
	PivotCount int    `json:"pivot_count"`

	QSDPid    int    `json:"qsd_pid,omitempty"`
	QMPSocket string `json:"qmp_socket,omitempty"`
	NBDSocket string `json:"nbd_socket,omitempty"`
	NBDDevice string `json:"nbd_device,omitempty"`
}

type stateLayer struct {
	// SnapshotID is the published DiskSnapshot external ID, empty while the
	// layer is still pending.
	SnapshotID string `json:"snapshot_id,omitempty"`
	Path       string `json:"path"`
}

func (s *volumeState) publishedIDs() map[string]bool {
	ids := make(map[string]bool, len(s.Chain))
	for _, layer := range s.Chain {
		if layer.SnapshotID != "" {
			ids[layer.SnapshotID] = true
		}
	}
	return ids
}

// lastLayerPath returns the backing parent for a new head: the newest pending
// layer, else the newest chain layer, else nothing.
func (s *volumeState) lastLayerPath() string {
	if n := len(s.Pending); n > 0 {
		return s.Pending[n-1].Path
	}
	if n := len(s.Chain); n > 0 {
		return s.Chain[n-1].Path
	}
	return ""
}

func (s *volumeState) depth() int {
	depth := len(s.Chain) + len(s.Pending)
	if s.HeadPath != "" {
		depth++
	}
	return depth
}

func loadVolumeState(dir string) (*volumeState, error) {
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	state := &volumeState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("parse volume state in %s: %w", dir, err)
	}
	return state, nil
}

func saveVolumeState(dir string, state *volumeState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, stateFileName), data)
}

// writeFileAtomic replaces path with data through a synced temporary file and
// a rename, so a crash mid-write never leaves a torn file behind.
func writeFileAtomic(path string, data []byte) error {
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}
