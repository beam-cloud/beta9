package types

import (
	"fmt"
	"strings"
	"time"
)

const (
	StorageModeJuiceFS     = "juicefs"
	StorageModeMountPoint  = "mountpoint"
	StorageModeGeese       = "geese"
	StorageModeAlluxio     = "alluxio"
	StorageModeLocal       = "local"
	StorageModeDurableDisk = "durable_disk"
)

func SafeDurableDiskName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\\", "-")
	name = strings.ReplaceAll(name, "/", "-")
	if name == "" || name == "." || name == ".." {
		return "disk"
	}
	return name
}

// Disk is a first-class ext4 state volume referenced by name when attached to
// a stub. Its immutable contents are addressed by volume generation IDs.
type Disk struct {
	Id          uint   `db:"id" json:"id"`
	ExternalId  string `db:"external_id" json:"external_id"`
	WorkspaceId uint   `db:"workspace_id" json:"workspace_id"`
	Name        string `db:"name" json:"name"`
	Size        string `db:"size" json:"size"`
	MountPath   string `db:"mount_path" json:"mount_path"`
	// CurrentGenerationId is the repository-owned mutable head for a named
	// disk. It is never accepted from the public disk API.
	CurrentGenerationId string   `db:"current_generation_id" json:"-"`
	CreatedAt           Time     `db:"created_at" json:"created_at"`
	UpdatedAt           Time     `db:"updated_at" json:"updated_at"`
	DeletedAt           NullTime `db:"deleted_at" json:"deleted_at,omitempty"`
}

type DiskWithRelated struct {
	Disk
	Workspace Workspace `db:"workspace" json:"workspace"`
}

type StateVolumeAttachment struct {
	VolumeId           string
	Name               string
	Size               string
	MountPath          string
	ContainerId        string
	SourceGenerationId string
	ReadOnly           bool
	Initialize         bool
	CloneSource        bool
	AttachmentToken    string
	FencingToken       int64
	ExpiresAt          time.Time
	Replayed           bool
}

type StateVolumeLease struct {
	VolumeId        string
	AttachmentToken string
	FencingToken    int64
}

// StateVolumeReleaseMember is the non-secret identity carried by a replacement
// worker's durable journal. Attachment tokens never leave PostgreSQL during a
// release handoff; the server escrows them after proving this exact
// volume/fence tuple still belongs to the dead source worker epoch.
type StateVolumeReleaseMember struct {
	VolumeId     string `db:"volume_id" json:"volume_id"`
	FencingToken int64  `db:"fencing_token" json:"fencing_token"`
}

type StateVolumeReleaseClaim struct {
	ExternalId               string                     `db:"external_id" json:"external_id"`
	WorkspaceId              uint                       `db:"workspace_id" json:"workspace_id"`
	ContainerId              string                     `db:"container_id" json:"container_id"`
	SourceWorkerId           string                     `db:"source_worker_id" json:"source_worker_id"`
	SourceWorkerInstanceId   string                     `db:"source_worker_instance_id" json:"source_worker_instance_id"`
	StorageNodeId            string                     `db:"storage_node_id" json:"storage_node_id"`
	RecoveryWorkerId         string                     `db:"recovery_worker_id" json:"recovery_worker_id"`
	RecoveryWorkerInstanceId string                     `db:"recovery_worker_instance_id" json:"recovery_worker_instance_id"`
	JournalDigest            string                     `db:"journal_digest" json:"journal_digest"`
	ClaimGeneration          int64                      `db:"claim_generation" json:"claim_generation"`
	Phase                    string                     `db:"phase" json:"phase"`
	Completed                bool                       `db:"completed" json:"completed"`
	Members                  []StateVolumeReleaseMember `db:"-" json:"members"`
}

type StateSnapshotReference struct {
	ExternalId         string    `db:"external_id" json:"external_id"`
	WorkspaceId        uint      `db:"workspace_id" json:"workspace_id"`
	StateSnapshotId    uint      `db:"state_snapshot_id" json:"state_snapshot_id"`
	SnapshotExternalId string    `db:"snapshot_external_id" json:"state_snapshot_external_id"`
	Kind               string    `db:"kind" json:"kind"`
	ReferenceId        string    `db:"reference_id" json:"reference_id"`
	Released           bool      `db:"released" json:"released"`
	CreatedAt          time.Time `db:"created_at" json:"created_at"`
	UpdatedAt          time.Time `db:"updated_at" json:"updated_at"`
}

// PublicStateTemplateMember is the complete workspace-neutral projection of
// one member in an immutable public state template. It deliberately contains
// no workspace, volume, generation, bucket, object-key, or credential fields.
// @go2proto
type PublicStateTemplateMember struct {
	Name              string `json:"name"`
	MountPath         string `json:"mount_path"`
	ReadOnly          bool   `json:"read_only"`
	Root              bool   `json:"root"`
	LogicalSizeBytes  int64  `json:"logical_size_bytes"`
	StoredSizeBytes   int64  `json:"stored_size_bytes"`
	ManifestDigest    string `json:"manifest_digest"`
	ManifestSizeBytes int64  `json:"manifest_size_bytes"`
	ChunkCount        int64  `json:"chunk_count"`
}

// PublicStateTemplateArtifact is the only projection exposed to an owner or a
// foreign workspace. Image is a workspace-neutral image identity whose digest
// is immutable with the exact ordered member vector and aggregate digest.
// @go2proto
type PublicStateTemplateArtifact struct {
	StateTemplateId         string                      `json:"state_template_id"`
	Version                 int64                       `json:"version"`
	Status                  string                      `json:"status"`
	AggregateManifestDigest string                      `json:"aggregate_manifest_digest"`
	Members                 []PublicStateTemplateMember `json:"members"`
	Image                   string                      `json:"image"`
	ImageDigest             string                      `json:"image_digest"`
	RuntimeProfile          string                      `json:"runtime_profile"`
}

// StateTemplateInstantiation is an immutable replay receipt. The operation
// keeps the complete public recipe even after the public template begins
// retirement, and terminalWinner is either "confirmed" or "canceled" once a
// Confirm/Cancel race has a durable winner.
// @go2proto
type StateTemplateInstantiation struct {
	OperationId                string                      `json:"operation_id"`
	Status                     string                      `json:"status"`
	TerminalWinner             string                      `json:"terminal_winner"`
	Template                   PublicStateTemplateArtifact `json:"template"`
	DestinationStubId          string                      `json:"destination_stub_id"`
	DestinationStateSnapshotId string                      `json:"destination_state_snapshot_id"`
	DestinationImageId         string                      `json:"destination_image_id"`
	DestinationImageDigest     string                      `json:"destination_image_digest"`
	DestinationRuntimeProfile  string                      `json:"destination_runtime_profile"`
	Generations                []StateGeneration           `json:"generations"`
	Replayed                   bool                        `json:"replayed"`
}

type StateCacheRetirement struct {
	Id                  uint   `db:"id"`
	WorkspaceId         uint   `db:"workspace_id"`
	WorkspaceExternalId string `db:"workspace_external_id"`
	StateSnapshotId     uint   `db:"state_snapshot_id"`
	StubExternalId      string `db:"stub_external_id"`
	VolumeId            string `db:"volume_id"`
	RevisionGeneration  int64  `db:"revision_generation"`
	RevisionId          string `db:"revision_id"`
}

// StateVolumeAttachmentPlan is the durable hand-off between PostgreSQL
// writer-lease acquisition and the Redis container-state admission. A pending
// plan is safe to abort only after the scheduler proves that the container was
// never admitted; a completed plan is deleted once that admission exists.
type StateVolumeAttachmentPlan struct {
	PlanId                  string    `db:"plan_id"`
	WorkspaceId             uint      `db:"workspace_id"`
	ContainerId             string    `db:"container_id"`
	RequestHash             string    `db:"request_hash"`
	ExpectedWritableMembers int       `db:"expected_writable_members"`
	CreatedAt               time.Time `db:"created_at"`
	Admitted                bool      `db:"admitted"`
	Enqueued                bool      `db:"enqueued"`
	Aborted                 bool      `db:"aborted"`
	AbortReason             string    `db:"abort_reason"`
	Owned                   bool      `db:"-"`
}

type StateSnapshotStatus string

const (
	StateSnapshotStatusPending   StateSnapshotStatus = "pending"
	StateSnapshotStatusAvailable StateSnapshotStatus = "available"
	StateSnapshotStatusFailed    StateSnapshotStatus = "failed"
	StateSnapshotFormatBlockV1                       = "block.v1"
)

// @go2proto
type PersistentRoot struct {
	Size string `json:"size"`
}

// RootStateMountConfig is scheduler-authored state carried only on the
// internal ContainerRequest. Public stub configuration exposes PersistentRoot
// size, never lineage IDs or fencing credentials.
// @go2proto
type RootStateMountConfig struct {
	VolumeId           string `json:"volume_id"`
	Size               string `json:"size"`
	SourceGenerationId string `json:"source_generation_id,omitempty"`
	CloneSource        bool   `json:"clone_source,omitempty"`
	Initialize         bool   `json:"initialize"`
	AttachmentToken    string `json:"attachment_token"`
	FencingToken       int64  `json:"fencing_token"`
	LeaseExpiresAtUnix int64  `json:"lease_expires_at_unix"`
}

// @go2proto
type StateGeneration struct {
	VolumeId                string `db:"volume_id" json:"volume_id"`
	GenerationId            string `db:"generation_id" json:"generation_id"`
	ParentGenerationId      string `db:"parent_generation_id" json:"parent_generation_id,omitempty"`
	CloneParentGenerationId string `db:"clone_parent_generation_id" json:"clone_parent_generation_id,omitempty"`
	Name                    string `db:"name" json:"name"`
	MountPath               string `db:"mount_path" json:"mount_path"`
	ReadOnly                bool   `db:"read_only" json:"read_only"`
	Root                    bool   `db:"root" json:"root"`
	Generation              int64  `db:"generation" json:"generation"`
}

// StateGenerationCompaction is pending-operation authorization, not immutable
// generation ancestry. It is deleted atomically when the operation becomes
// terminal, leaving the published block.v1 generation genuinely parentless.
type StateGenerationCompaction struct {
	VolumeId           string `db:"volume_id" json:"volume_id"`
	GenerationId       string `db:"generation_id" json:"generation_id"`
	SourceGenerationId string `db:"compaction_source_generation_id" json:"source_generation_id"`
}

// @go2proto
type StateRestoreReceipt struct {
	StateSnapshotId string            `json:"state_snapshot_id"`
	RestoreMode     string            `json:"restore_mode"`
	FallbackReason  string            `json:"fallback_reason"`
	Generations     []StateGeneration `json:"generations"`
}

type VolumeGeneration struct {
	Id                      uint                `db:"id" json:"id,omitempty"`
	ExternalId              string              `db:"external_id" json:"external_id"`
	WorkspaceId             uint                `db:"workspace_id" json:"workspace_id"`
	StubId                  uint                `db:"stub_id" json:"stub_id,omitempty"`
	VolumeId                string              `db:"volume_id" json:"volume_id"`
	Name                    string              `db:"name" json:"name"`
	ParentGenerationId      string              `db:"parent_generation_id" json:"parent_generation_id,omitempty"`
	CloneParentGenerationId string              `db:"clone_parent_generation_id" json:"clone_parent_generation_id,omitempty"`
	Generation              int64               `db:"generation" json:"generation"`
	Status                  StateSnapshotStatus `db:"status" json:"status"`
	Reason                  string              `db:"reason" json:"reason,omitempty"`
	ManifestKey             string              `db:"manifest_key" json:"manifest_key"`
	ManifestDigest          string              `db:"manifest_digest" json:"manifest_digest"`
	ManifestSizeBytes       int64               `db:"manifest_size_bytes" json:"manifest_size_bytes"`
	ChunkCount              int64               `db:"chunk_count" json:"chunk_count"`
	LogicalSizeBytes        int64               `db:"logical_size_bytes" json:"logical_size_bytes"`
	StoredSizeBytes         int64               `db:"stored_size_bytes" json:"stored_size_bytes"`
	BucketName              string              `db:"bucket_name" json:"bucket_name"`
	ObjectPrefix            string              `db:"object_prefix" json:"object_prefix"`
	Public                  bool                `db:"public" json:"public"`
	CreatedAt               Time                `db:"created_at" json:"created_at"`
	UpdatedAt               Time                `db:"updated_at" json:"updated_at"`
	CompletedAt             NullTime            `db:"completed_at" json:"completed_at,omitempty"`
}

type StateSnapshot struct {
	Id                       uint                `db:"id" json:"id,omitempty"`
	ExternalId               string              `db:"external_id" json:"external_id"`
	OperationId              string              `db:"operation_id" json:"operation_id"`
	WorkspaceId              uint                `db:"workspace_id" json:"workspace_id"`
	StubId                   uint                `db:"stub_id" json:"stub_id"`
	SourceContainerId        string              `db:"source_container_id" json:"source_container_id"`
	SourceWorkerId           string              `db:"source_worker_id" json:"source_worker_id,omitempty"`
	SourceWorkerInstanceId   string              `db:"source_worker_instance_id" json:"source_worker_instance_id,omitempty"`
	RecoveryWorkerId         string              `db:"recovery_worker_id" json:"recovery_worker_id,omitempty"`
	RecoveryWorkerInstanceId string              `db:"recovery_worker_instance_id" json:"recovery_worker_instance_id,omitempty"`
	RecoveryClaimGeneration  int64               `db:"recovery_claim_generation" json:"recovery_claim_generation,omitempty"`
	RecoveryProofToken       string              `db:"recovery_proof_token" json:"-"`
	StorageNodeId            string              `db:"storage_node_id" json:"storage_node_id,omitempty"`
	Armed                    bool                `db:"armed" json:"armed"`
	Mode                     string              `db:"mode" json:"mode"`
	IncludeMemory            bool                `db:"include_memory" json:"include_memory"`
	Visible                  bool                `db:"visible" json:"visible"`
	Status                   StateSnapshotStatus `db:"status" json:"status"`
	Reason                   string              `db:"reason" json:"reason,omitempty"`
	ImageId                  string              `db:"image_id" json:"image_id"`
	ImageDigest              string              `db:"image_digest" json:"image_digest"`
	RuntimeProfile           string              `db:"runtime_profile" json:"runtime_profile"`
	CheckpointId             string              `db:"checkpoint_id" json:"checkpoint_id,omitempty"`
	CheckpointDigest         string              `db:"checkpoint_digest" json:"checkpoint_digest,omitempty"`
	CheckpointCacheHash      string              `db:"checkpoint_cache_hash" json:"checkpoint_cache_hash,omitempty"`
	CheckpointSizeBytes      int64               `db:"checkpoint_size_bytes" json:"checkpoint_size_bytes,omitempty"`
	CheckpointOriginKey      string              `db:"checkpoint_origin_key" json:"checkpoint_origin_key,omitempty"`
	CheckpointAccelerator    string              `db:"checkpoint_accelerator" json:"checkpoint_accelerator,omitempty"`
	CheckpointLocality       string              `db:"checkpoint_locality" json:"checkpoint_locality,omitempty"`
	RestoreMode              string              `db:"restore_mode" json:"restore_mode"`
	FallbackReason           string              `db:"fallback_reason" json:"fallback_reason,omitempty"`
	SourceStubExternalId     string              `db:"source_stub_external_id" json:"source_stub_external_id,omitempty"`
	SourceStubName           string              `db:"source_stub_name" json:"source_stub_name,omitempty"`
	SourceStubType           string              `db:"source_stub_type" json:"source_stub_type,omitempty"`
	Public                   bool                `db:"public" json:"public"`
	CreatedAt                Time                `db:"created_at" json:"created_at"`
	UpdatedAt                Time                `db:"updated_at" json:"updated_at"`
	CompletedAt              NullTime            `db:"completed_at" json:"completed_at,omitempty"`
	Generations              []StateGeneration   `json:"generations" db:"-"`
}

type ErrStateSnapshotNotFound struct{ StateSnapshotId string }

func (e *ErrStateSnapshotNotFound) Error() string {
	return fmt.Sprintf("state snapshot not found: %s", e.StateSnapshotId)
}

type ErrVolumeGenerationNotFound struct{ GenerationId string }

func (e *ErrVolumeGenerationNotFound) Error() string {
	return fmt.Sprintf("volume generation not found: %s", e.GenerationId)
}
