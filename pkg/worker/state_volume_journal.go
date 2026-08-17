package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Version 4 is the clean-cut block-state journal. Older directory/block
// experiments are intentionally not adopted; they lacked the immutable
// lineage and worker-epoch anchors required to prove a repeated-snapshot
// recovery without adopting a live process from another worker namespace.
const stateVolumeJournalVersion = 4

const (
	stateVolumeJournalMaxBytes   int64 = 1 << 20
	stateVolumeJournalMaxVolumes       = 64
)

type StateVolumeJournal struct {
	Version               int                          `json:"version"`
	ContainerID           string                       `json:"container_id"`
	WorkerID              string                       `json:"worker_id,omitempty"`
	WorkerInstanceID      string                       `json:"worker_instance_id,omitempty"`
	WorkerPodUID          string                       `json:"worker_pod_uid,omitempty"`
	StorageNodeID         string                       `json:"storage_node_id,omitempty"`
	SourceStateSnapshotID string                       `json:"source_state_snapshot_id,omitempty"`
	QSDPID                int                          `json:"qsd_pid"`
	QSDExecutable         string                       `json:"qsd_executable,omitempty"`
	QSDStartTime          uint64                       `json:"qsd_start_time,omitempty"`
	QMPSocket             string                       `json:"qmp_socket"`
	NBDSocket             string                       `json:"nbd_socket"`
	OperationID           string                       `json:"operation_id,omitempty"`
	Phase                 string                       `json:"phase"`
	Recovery              *StateVolumeRecoveryEnvelope `json:"recovery,omitempty"`
	Release               *StateVolumeReleaseEnvelope  `json:"release,omitempty"`
	Volumes               []StateVolumeJournalVolume   `json:"volumes"`
}

// StateVolumeRecoveryEnvelope is persisted only in the owner-private 0600
// recovery journal. RecoveryProofToken is the sole secret: the remaining
// fields are non-secret scope used to re-vend short-lived storage credentials
// and resume an already escrowed terminal publication after worker replacement.
type StateVolumeRecoveryEnvelope struct {
	StateSnapshotID            string `json:"state_snapshot_id"`
	RecoveryProofToken         string `json:"recovery_proof_token"`
	RecoveryClaimGeneration    int64  `json:"recovery_claim_generation,omitempty"`
	OperationID                string `json:"operation_id"`
	WorkspaceID                string `json:"workspace_id"`
	WorkspaceName              string `json:"workspace_name"`
	StubID                     string `json:"stub_id"`
	StubName                   string `json:"stub_name,omitempty"`
	StubType                   string `json:"stub_type,omitempty"`
	ImageID                    string `json:"image_id"`
	ImageDigest                string `json:"image_digest"`
	RuntimeProfile             string `json:"runtime_profile"`
	Mode                       string `json:"mode"`
	IncludeMemory              bool   `json:"include_memory"`
	Visible                    bool   `json:"visible"`
	WorkspaceStorageID         uint   `json:"workspace_storage_id"`
	WorkspaceStorageExternalID string `json:"workspace_storage_external_id,omitempty"`
	WorkspaceStorageBucket     string `json:"workspace_storage_bucket"`
	WorkspaceStorageEndpoint   string `json:"workspace_storage_endpoint,omitempty"`
	WorkspaceStorageRegion     string `json:"workspace_storage_region"`
}

// StateVolumeReleaseEnvelope is deliberately non-secret. It binds a local,
// fsynced cleanup obligation to attachment lineage and a server-side escrow;
// attachment tokens never cross this boundary. A replacement worker can claim
// the escrow only after the control plane proves the source process epoch dead.
type StateVolumeReleaseEnvelope struct {
	WorkspaceID            string                     `json:"workspace_id"`
	SourceWorkerID         string                     `json:"source_worker_id"`
	SourceWorkerInstanceID string                     `json:"source_worker_instance_id"`
	StorageNodeID          string                     `json:"storage_node_id"`
	JournalDigest          string                     `json:"journal_digest"`
	ReleaseClaimID         string                     `json:"release_claim_id,omitempty"`
	ReleaseClaimGeneration int64                      `json:"release_claim_generation,omitempty"`
	LocalCleanupVerified   bool                       `json:"local_cleanup_verified,omitempty"`
	Members                []StateVolumeReleaseMember `json:"members"`
}

type StateVolumeReleaseMember struct {
	VolumeID     string `json:"volume_id"`
	FencingToken int64  `json:"fencing_token"`
}

type stateVolumeReleaseDigestPayload struct {
	ContainerID            string                     `json:"container_id"`
	WorkspaceID            string                     `json:"workspace_id"`
	SourceWorkerID         string                     `json:"source_worker_id"`
	SourceWorkerInstanceID string                     `json:"source_worker_instance_id"`
	StorageNodeID          string                     `json:"storage_node_id"`
	Members                []StateVolumeReleaseMember `json:"members"`
}

func canonicalStateVolumeReleaseMembers(members []StateVolumeReleaseMember) ([]StateVolumeReleaseMember, error) {
	canonical := append([]StateVolumeReleaseMember(nil), members...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].VolumeID < canonical[j].VolumeID })
	for index, member := range canonical {
		parsed, err := uuid.Parse(member.VolumeID)
		if err != nil || parsed.String() != strings.ToLower(member.VolumeID) || member.FencingToken <= 0 {
			return nil, fmt.Errorf("release member %q has an invalid volume/fence identity", member.VolumeID)
		}
		if index > 0 && canonical[index-1].VolumeID == member.VolumeID {
			return nil, fmt.Errorf("duplicate release member %q", member.VolumeID)
		}
	}
	if len(canonical) == 0 {
		return nil, fmt.Errorf("state-volume release has no writable members")
	}
	return canonical, nil
}

func stateVolumeReleaseJournalDigest(containerID string, release StateVolumeReleaseEnvelope) (string, error) {
	members, err := canonicalStateVolumeReleaseMembers(release.Members)
	if err != nil {
		return "", err
	}
	payload := stateVolumeReleaseDigestPayload{
		ContainerID: containerID, WorkspaceID: release.WorkspaceID,
		SourceWorkerID: release.SourceWorkerID, SourceWorkerInstanceID: release.SourceWorkerInstanceID,
		StorageNodeID: release.StorageNodeID, Members: members,
	}
	if strings.TrimSpace(payload.ContainerID) == "" || strings.TrimSpace(payload.WorkspaceID) == "" ||
		strings.TrimSpace(payload.SourceWorkerID) == "" || strings.TrimSpace(payload.SourceWorkerInstanceID) == "" ||
		strings.TrimSpace(payload.StorageNodeID) == "" {
		return "", fmt.Errorf("state-volume release scope is incomplete")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("sha256:%x", digest[:]), nil
}

func validateStateVolumeReleaseEnvelope(containerID, phase string, release *StateVolumeReleaseEnvelope) error {
	if release == nil {
		return fmt.Errorf("state-volume release phase %q has no release envelope", phase)
	}
	members, err := canonicalStateVolumeReleaseMembers(release.Members)
	if err != nil {
		return err
	}
	release.Members = members
	digest, err := stateVolumeReleaseJournalDigest(containerID, *release)
	if err != nil {
		return err
	}
	if release.JournalDigest != digest {
		return fmt.Errorf("state-volume release journal digest mismatch")
	}
	if release.ReleaseClaimGeneration < 0 {
		return fmt.Errorf("state-volume release claim generation is negative")
	}
	switch phase {
	case "release-detach-intent":
		if release.ReleaseClaimID != "" || release.ReleaseClaimGeneration != 0 || release.LocalCleanupVerified {
			return fmt.Errorf("unarmed state-volume release intent contains completion authority")
		}
	case "release-armed":
		if parsed, err := uuid.Parse(release.ReleaseClaimID); err != nil || parsed.String() != release.ReleaseClaimID ||
			release.ReleaseClaimGeneration != 0 || release.LocalCleanupVerified {
			return fmt.Errorf("armed state-volume release has invalid source escrow identity")
		}
	case "release-intent", "release-completed":
		if !release.LocalCleanupVerified {
			return fmt.Errorf("detached state-volume release has no authenticated cleanup/escrow proof")
		}
		if release.ReleaseClaimID == "" {
			if phase == "release-completed" || release.ReleaseClaimGeneration != 0 {
				return fmt.Errorf("detached state-volume release has invalid empty claim identity")
			}
		} else if parsed, err := uuid.Parse(release.ReleaseClaimID); err != nil || parsed.String() != release.ReleaseClaimID {
			return fmt.Errorf("detached state-volume release has invalid claim identity")
		}
	default:
		return fmt.Errorf("state-volume release envelope is invalid in journal phase %q", phase)
	}
	return nil
}

type StateVolumeJournalVolume struct {
	ID                                  string `json:"id"`
	Name                                string `json:"name,omitempty"`
	ContainerMountPath                  string `json:"container_mount_path,omitempty"`
	Root                                bool   `json:"root,omitempty"`
	ReadOnly                            bool   `json:"read_only,omitempty"`
	Initialize                          bool   `json:"initialize,omitempty"`
	CreateLayer                         bool   `json:"create_layer,omitempty"`
	Prepared                            bool   `json:"prepared,omitempty"`
	Generation                          int64  `json:"generation"`
	ExportName                          string `json:"export_name"`
	DevicePath                          string `json:"device_path"`
	BackingDir                          string `json:"backing_dir"`
	MountPath                           string `json:"mount_path"`
	SizeBytes                           int64  `json:"size_bytes"`
	RootNode                            string `json:"root_node"`
	FileNode                            string `json:"file_node"`
	ActiveNode                          string `json:"active_node"`
	ActiveLayerPath                     string `json:"active_layer_path"`
	ActiveBackingPath                   string `json:"active_backing_path,omitempty"`
	CurrentGenerationID                 string `json:"current_generation_id,omitempty"`
	LineageSourceGenerationID           string `json:"lineage_source_generation_id,omitempty"`
	SourceVolumeID                      string `json:"source_volume_id,omitempty"`
	SourceGeneration                    int64  `json:"source_generation,omitempty"`
	SourceParentGenerationID            string `json:"source_parent_generation_id,omitempty"`
	SourceCloneParentGenerationID       string `json:"source_clone_parent_generation_id,omitempty"`
	SourceDepth                         int    `json:"source_depth,omitempty"`
	ParentGenerationID                  string `json:"parent_generation_id,omitempty"`
	CloneParentGenerationID             string `json:"clone_parent_generation_id,omitempty"`
	FencingToken                        int64  `json:"fencing_token,omitempty"`
	Depth                               int    `json:"depth"`
	PivotNode                           string `json:"pivot_node,omitempty"`
	PivotLayerPath                      string `json:"pivot_layer_path,omitempty"`
	PendingGenerationID                 string `json:"pending_generation_id,omitempty"`
	PendingGeneration                   int64  `json:"pending_generation,omitempty"`
	PendingReused                       bool   `json:"pending_reused,omitempty"`
	PendingLayerPath                    string `json:"pending_layer_path,omitempty"`
	PendingBackingPath                  string `json:"pending_backing_path,omitempty"`
	PendingParentGenerationID           string `json:"pending_parent_generation_id,omitempty"`
	PendingCloneParentGenerationID      string `json:"pending_clone_parent_generation_id,omitempty"`
	PendingCompaction                   bool   `json:"pending_compaction,omitempty"`
	PendingCompactionSourceGenerationID string `json:"pending_compaction_source_generation_id,omitempty"`
	PendingDepth                        int    `json:"pending_depth,omitempty"`
	CompactionJobID                     string `json:"compaction_job_id,omitempty"`
	CompactionPhase                     string `json:"compaction_phase,omitempty"`
	CompactionNode                      string `json:"compaction_node,omitempty"`
	CompactionLayerPath                 string `json:"compaction_layer_path,omitempty"`
	CompactionBackingPath               string `json:"compaction_backing_path,omitempty"`
	CompactionPriorGenerationID         string `json:"compaction_prior_generation_id,omitempty"`
}

type StateVolumeJournalStore struct {
	RootDir     string
	SecurePaths stateVolumeSecurePathOps
}

func (s StateVolumeJournalStore) securePaths() stateVolumeSecurePathOps {
	if s.SecurePaths != nil {
		return s.SecurePaths
	}
	return newStateVolumeSecurePathOps()
}

func (s StateVolumeJournalStore) journalPath(containerID string) (string, error) {
	if strings.TrimSpace(s.RootDir) == "" {
		return "", fmt.Errorf("state volume journal root is empty")
	}
	if strings.TrimSpace(containerID) == "" {
		return "", fmt.Errorf("state volume journal container ID is empty")
	}
	return filepath.Join(s.RootDir, stateVolumeToken("container-", containerID)+".json"), nil
}

func validateStateVolumeJournal(journal StateVolumeJournal) error {
	if journal.Version != stateVolumeJournalVersion {
		return fmt.Errorf("unsupported state volume journal version %d", journal.Version)
	}
	if len(journal.Volumes) > stateVolumeJournalMaxVolumes {
		return fmt.Errorf("state volume journal member count %d exceeds supported maximum %d", len(journal.Volumes), stateVolumeJournalMaxVolumes)
	}
	if journal.ContainerID == "" {
		return fmt.Errorf("state volume journal has no container ID")
	}
	releasePhase := journal.Phase == "release-detach-intent" || journal.Phase == "release-armed" ||
		journal.Phase == "release-intent" || journal.Phase == "release-completed"
	if releasePhase {
		if err := validateStateVolumeReleaseEnvelope(journal.ContainerID, journal.Phase, journal.Release); err != nil {
			return err
		}
	} else if journal.Release != nil {
		return fmt.Errorf("state-volume release envelope is invalid in journal phase %q", journal.Phase)
	}
	// A worker can obtain an attachment and fail before constructing a graph.
	// Its release escrow is still crash-replayable, but there are no journal-
	// selected paths or kernel resources to authenticate.
	if releasePhase && len(journal.Volumes) == 0 {
		if journal.QSDPID != 0 || journal.QMPSocket != "" || journal.NBDSocket != "" {
			return fmt.Errorf("release-only journal unexpectedly names QSD resources")
		}
		return nil
	}
	preparing := stateVolumeJournalPreparationPhase(journal.Phase)
	if journal.QMPSocket == "" || !filepath.IsAbs(journal.QMPSocket) {
		return fmt.Errorf("state volume journal has invalid QMP socket %q", journal.QMPSocket)
	}
	if journal.NBDSocket == "" || !filepath.IsAbs(journal.NBDSocket) || filepath.Clean(journal.NBDSocket) != journal.NBDSocket ||
		filepath.Base(journal.NBDSocket) != "nbd.sock" || filepath.Dir(journal.NBDSocket) != filepath.Dir(journal.QMPSocket) {
		return fmt.Errorf("state volume journal has invalid NBD Unix socket %q", journal.NBDSocket)
	}
	if recovery := journal.Recovery; recovery != nil {
		if recovery.StateSnapshotID == "" || recovery.OperationID == "" || recovery.WorkspaceID == "" || recovery.WorkspaceName == "" || recovery.StubID == "" ||
			recovery.ImageID == "" || recovery.ImageDigest == "" || recovery.RuntimeProfile == "" ||
			(recovery.Mode != string(StateSnapshotModeLive) && recovery.Mode != string(StateSnapshotModeTerminal)) ||
			recovery.WorkspaceStorageID == 0 || recovery.WorkspaceStorageBucket == "" || recovery.WorkspaceStorageRegion == "" {
			return fmt.Errorf("state volume journal has an incomplete recovery envelope")
		}
		if parsed, err := uuid.Parse(recovery.RecoveryProofToken); err != nil || parsed.String() != strings.ToLower(recovery.RecoveryProofToken) {
			return fmt.Errorf("state volume journal has an invalid recovery proof token")
		}
		if journal.OperationID != "" && journal.OperationID != recovery.OperationID {
			return fmt.Errorf("state volume journal operation does not match recovery envelope")
		}
	}
	ids := make(map[string]struct{}, len(journal.Volumes))
	devices := make(map[string]struct{}, len(journal.Volumes))
	sourceVolumes := make(map[string]struct{}, len(journal.Volumes))
	sourceGenerations := make(map[string]struct{}, len(journal.Volumes))
	for _, volume := range journal.Volumes {
		if volume.ID == "" || volume.ExportName == "" || (!preparing && volume.DevicePath == "") || volume.FileNode == "" || volume.ActiveNode == "" || volume.RootNode == "" || volume.ActiveLayerPath == "" || volume.SizeBytes <= 0 {
			return fmt.Errorf("state volume journal contains an incomplete volume")
		}
		if volume.Initialize && volume.CreateLayer {
			return fmt.Errorf("state volume journal volume %q cannot initialize and restore a layer", volume.ID)
		}
		if volume.ReadOnly && (volume.Initialize || volume.CreateLayer) {
			return fmt.Errorf("read-only state volume journal volume %q cannot create a layer", volume.ID)
		}
		if volume.CreateLayer && volume.ActiveBackingPath == "" {
			return fmt.Errorf("restored state volume journal volume %q has no authenticated backing", volume.ID)
		}
		if _, exists := ids[volume.ID]; exists {
			return fmt.Errorf("state volume journal contains duplicate volume %q", volume.ID)
		}
		ids[volume.ID] = struct{}{}
		if volume.DevicePath != "" {
			if _, exists := devices[volume.DevicePath]; exists {
				return fmt.Errorf("state volume journal contains duplicate NBD device %q", volume.DevicePath)
			}
			devices[volume.DevicePath] = struct{}{}
		}
		if err := validateStateVolumePathPair(volume.BackingDir, volume.MountPath); err != nil {
			return fmt.Errorf("state volume journal volume %q: %w", volume.ID, err)
		}
		if volume.Depth < 0 || volume.Depth > StateVolumeMaxActiveDepth {
			return fmt.Errorf("state volume journal volume %q has invalid depth %d", volume.ID, volume.Depth)
		}
		if volume.Generation < 0 || volume.PendingGeneration < 0 {
			return fmt.Errorf("state volume journal volume %q has invalid generation counter", volume.ID)
		}
		if journal.SourceStateSnapshotID != "" {
			if volume.SourceVolumeID == "" || volume.LineageSourceGenerationID == "" || volume.SourceGeneration <= 0 ||
				volume.SourceDepth <= 0 || volume.SourceDepth > StateVolumeMaxDepth {
				return fmt.Errorf("restored state volume journal volume %q has incomplete authenticated source membership", volume.ID)
			}
			if _, exists := sourceVolumes[volume.SourceVolumeID]; exists {
				return fmt.Errorf("restored state volume journal has duplicate source volume %q", volume.SourceVolumeID)
			}
			if _, exists := sourceGenerations[volume.LineageSourceGenerationID]; exists {
				return fmt.Errorf("restored state volume journal has duplicate source generation %q", volume.LineageSourceGenerationID)
			}
			sourceVolumes[volume.SourceVolumeID] = struct{}{}
			sourceGenerations[volume.LineageSourceGenerationID] = struct{}{}
		}
		if volume.PendingReused && (!volume.ReadOnly || volume.PendingGenerationID == "" ||
			volume.PendingGenerationID != volume.CurrentGenerationID || volume.PendingGeneration != volume.Generation ||
			volume.PendingLayerPath != "" || volume.PendingBackingPath != "") {
			return fmt.Errorf("state volume journal volume %q has an invalid reused generation", volume.ID)
		}
		if volume.PendingCompaction {
			if volume.ReadOnly || volume.PendingReused || volume.PendingGenerationID == "" ||
				volume.PendingCompactionSourceGenerationID == "" || volume.PendingParentGenerationID != "" ||
				volume.PendingCloneParentGenerationID != "" || volume.PendingDepth != 1 {
				return fmt.Errorf("state volume journal volume %q has an invalid pending compaction", volume.ID)
			}
		} else if volume.PendingCompactionSourceGenerationID != "" {
			return fmt.Errorf("state volume journal volume %q has a compaction source without authorization", volume.ID)
		}
		if volume.CompactionPhase != "" {
			if volume.ReadOnly || volume.CompactionJobID == "" || volume.CompactionNode == "" ||
				volume.CompactionLayerPath != volume.ActiveLayerPath || volume.CompactionBackingPath == "" ||
				volume.CompactionBackingPath != volume.ActiveBackingPath || volume.CompactionPriorGenerationID == "" {
				return fmt.Errorf("state volume journal volume %q has an incomplete compaction intent", volume.ID)
			}
			switch volume.CompactionPhase {
			case "intent", "started", "start-indeterminate", "finalizing", "finalize-indeterminate", "cancel-intent", "cancel-indeterminate":
			default:
				return fmt.Errorf("state volume journal volume %q has invalid compaction phase %q", volume.ID, volume.CompactionPhase)
			}
		}
	}
	return nil
}

func stateVolumeJournalPreparationPhase(phase string) bool {
	switch phase {
	case "init-intent", "init-preparing", "restore-intent", "restore-preparing":
		return true
	default:
		return false
	}
}

func (s StateVolumeJournalStore) Save(journal StateVolumeJournal) error {
	journal.Version = stateVolumeJournalVersion
	sort.Slice(journal.Volumes, func(i, j int) bool { return journal.Volumes[i].ID < journal.Volumes[j].ID })
	if err := validateStateVolumeJournal(journal); err != nil {
		return err
	}
	path, err := s.journalPath(journal.ContainerID)
	if err != nil {
		return err
	}
	secure := s.securePaths()
	if err := secure.MkdirAll(s.RootDir, 0700); err != nil {
		return fmt.Errorf("create state volume journal root %s: %w", s.RootDir, err)
	}
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(journal); err != nil {
		return fmt.Errorf("encode state volume journal: %w", err)
	}
	if err := secure.AtomicReplaceRegular(path, data.Bytes(), 0600); err != nil {
		return fmt.Errorf("publish state volume journal %s: %w", path, err)
	}
	return nil
}

func (s StateVolumeJournalStore) Load(containerID string) (StateVolumeJournal, error) {
	path, err := s.journalPath(containerID)
	if err != nil {
		return StateVolumeJournal{}, err
	}
	file, err := s.securePaths().OpenRegular(path)
	if err != nil {
		return StateVolumeJournal{}, err
	}
	defer file.Close()
	journal, err := decodeStateVolumeJournalFile(file, path)
	if err != nil {
		return StateVolumeJournal{}, err
	}
	if journal.ContainerID != containerID {
		return StateVolumeJournal{}, fmt.Errorf("state volume journal container ID mismatch: got %q, want %q", journal.ContainerID, containerID)
	}
	if err := validateStateVolumeJournal(journal); err != nil {
		return StateVolumeJournal{}, err
	}
	return journal, nil
}

func (s StateVolumeJournalStore) List() ([]StateVolumeJournal, error) {
	if strings.TrimSpace(s.RootDir) == "" {
		return nil, fmt.Errorf("state volume journal root is empty")
	}
	entries, err := s.securePaths().ReadDir(s.RootDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list state volume journals: %w", err)
	}
	journals := make([]StateVolumeJournal, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.RootDir, entry.Name())
		file, err := s.securePaths().OpenRegular(path)
		if err != nil {
			if quarantineErr := s.quarantineJournalFilename(entry.Name()); quarantineErr != nil {
				return nil, errors.Join(fmt.Errorf("open state volume journal %s: %w", path, err), quarantineErr)
			}
			continue
		}
		journal, decodeErr := decodeStateVolumeJournalFile(file, path)
		closeErr := file.Close()
		if decodeErr != nil {
			if quarantineErr := s.quarantineJournalFilename(entry.Name()); quarantineErr != nil {
				return nil, errors.Join(fmt.Errorf("decode state volume journal %s: %w", path, decodeErr), quarantineErr)
			}
			continue
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err := validateStateVolumeJournal(journal); err != nil {
			return nil, fmt.Errorf("validate state volume journal %s: %w", path, err)
		}
		expectedPath, err := s.journalPath(journal.ContainerID)
		if err != nil || filepath.Base(expectedPath) != entry.Name() {
			if quarantineErr := s.quarantineJournalFilename(entry.Name()); quarantineErr != nil {
				return nil, errors.Join(fmt.Errorf("state volume journal filename %q does not match container %q", entry.Name(), journal.ContainerID), quarantineErr)
			}
			continue
		}
		journals = append(journals, journal)
	}
	sort.Slice(journals, func(i, j int) bool { return journals[i].ContainerID < journals[j].ContainerID })
	return journals, nil
}

func (s StateVolumeJournalStore) quarantineJournalFilename(name string) error {
	if filepath.Base(name) != name || name == "." || name == ".." || !strings.HasSuffix(name, ".json") {
		return fmt.Errorf("invalid state volume journal filename %q", name)
	}
	secure := s.securePaths()
	quarantineRoot := filepath.Join(s.RootDir, "quarantine")
	if err := secure.MkdirAll(quarantineRoot, 0700); err != nil {
		return err
	}
	source := filepath.Join(s.RootDir, name)
	destination := filepath.Join(quarantineRoot, name+"."+stateVolumeToken("unsafe-", time.Now().UTC().String()))
	return secure.Rename(source, destination, stateVolumeSecureRegular, false)
}

func decodeStateVolumeJournalFile(file *os.File, path string) (StateVolumeJournal, error) {
	if file == nil {
		return StateVolumeJournal{}, fmt.Errorf("state volume journal %s is not open", path)
	}
	info, err := file.Stat()
	if err != nil {
		return StateVolumeJournal{}, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > stateVolumeJournalMaxBytes {
		return StateVolumeJournal{}, fmt.Errorf("state volume journal %s has unsupported size %d", path, info.Size())
	}
	var journal StateVolumeJournal
	decoder := json.NewDecoder(io.LimitReader(file, stateVolumeJournalMaxBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&journal); err != nil {
		return StateVolumeJournal{}, fmt.Errorf("decode state volume journal %s: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return StateVolumeJournal{}, fmt.Errorf("state volume journal %s has trailing data", path)
	}
	if err := validateStateVolumeJournal(journal); err != nil {
		return StateVolumeJournal{}, fmt.Errorf("validate state volume journal %s: %w", path, err)
	}
	return journal, nil
}

// Quarantine moves only the journal payload selected from its validated
// container token. It never follows or mutates any path stored inside that
// untrusted payload.
func (s StateVolumeJournalStore) Quarantine(containerID string) error {
	path, err := s.journalPath(containerID)
	if err != nil {
		return err
	}
	return s.quarantineJournalFilename(filepath.Base(path))
}

func (s StateVolumeJournalStore) Remove(containerID string) error {
	path, err := s.journalPath(containerID)
	if err != nil {
		return err
	}
	if err := s.securePaths().Remove(path, stateVolumeSecureRegular); err != nil {
		return fmt.Errorf("remove state volume journal %s: %w", path, err)
	}
	return nil
}

func syncStateVolumeDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func syncStateVolumeFileAndDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state volume layer %q is not a regular file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	openedInfo, statErr := file.Stat()
	if statErr == nil && !os.SameFile(info, openedInfo) {
		statErr = fmt.Errorf("state volume layer %q changed while syncing", path)
	}
	if statErr == nil {
		statErr = file.Sync()
	}
	closeErr := file.Close()
	if statErr != nil {
		return statErr
	}
	if closeErr != nil {
		return closeErr
	}
	return syncStateVolumeDirectory(filepath.Dir(path))
}
