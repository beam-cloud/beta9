package disk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const detachedMarkerName = "detached.json"

type detachedVolumeMarker struct {
	Version     int    `json:"version"`
	StateSHA256 string `json:"state_sha256"`
}

func clearVhostAttachment(state *volumeState) {
	state.Attached = false
	state.QSDPid = 0
	state.QMPSocket = ""
	state.ExportSocket = ""
}

// This hint is written only after QSD has confirmed shutdown. The authoritative
// manifest stays byte-for-byte intact and synced: no head, pending write, chain
// or ownership information moves into the hint. If it disappears or tears on a
// host crash, recovery sees the old attached state and cleans up the dead daemon
// normally. Omitting a redundant fsync here never publishes a disk generation.
func saveDetachedVolumeMarker(dir string, detached *volumeState) error {
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if err != nil {
		return err
	}
	var stored volumeState
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}
	if !stored.Attached || stored.exportMode() != ExportVhostUser || detached.Attached {
		return fmt.Errorf("not a stopped vhost attachment")
	}
	clearVhostAttachment(&stored)
	// Compare persisted representations: omitempty deliberately represents
	// both nil and drained empty pending slices as the same durable state.
	storedJSON, err := json.Marshal(&stored)
	if err != nil {
		return err
	}
	detachedJSON, err := json.Marshal(detached)
	if err != nil {
		return err
	}
	if !bytes.Equal(storedJSON, detachedJSON) {
		return fmt.Errorf("payload state changed since its last durable save")
	}
	// Local-cache retention uses manifest mtime. Refresh recency without
	// rewriting its authoritative bytes; losing this timestamp can only make
	// an origin-backed cache copy eligible for eviction earlier after a crash.
	now := time.Now()
	if err := os.Chtimes(filepath.Join(dir, stateFileName), now, now); err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	marker, err := json.Marshal(detachedVolumeMarker{Version: 1, StateSHA256: hex.EncodeToString(digest[:])})
	if err != nil {
		return err
	}
	path := filepath.Join(dir, detachedMarkerName)
	tmp := path + ".tmp"
	defer os.Remove(tmp)
	if err := os.WriteFile(tmp, marker, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func applyDetachedVolumeMarker(dir string, manifest []byte, state *volumeState) {
	if !state.Attached || state.exportMode() != ExportVhostUser {
		return
	}
	data, err := os.ReadFile(filepath.Join(dir, detachedMarkerName))
	if err != nil {
		return
	}
	var marker detachedVolumeMarker
	if json.Unmarshal(data, &marker) != nil || marker.Version != 1 {
		return
	}
	digest := sha256.Sum256(manifest)
	if marker.StateSHA256 != hex.EncodeToString(digest[:]) {
		return
	}
	clearVhostAttachment(state)
}
