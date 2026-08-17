package worker

import (
	"context"
	"io"
)

const (
	BlockV1Format                 = "block.v1"
	BlockV1ChunkSize        int64 = 4 << 20
	StateVolumeClusterSize        = 64 << 10
	StateVolumeCompactDepth       = 16
	StateVolumeMaxDepth           = 32
	// A restored immutable depth-32 head needs one unpublished writable child
	// before QSD can live-stream it parentless. No block.v1 manifest may use
	// this transient depth; PlanSnapshot waits for compaction first.
	StateVolumeMaxActiveDepth = StateVolumeMaxDepth + 1
)

// StateSnapshotMode is intentionally independent of the public RPC enum. The
// container server adapts the generated request to this worker-owned boundary.
type StateSnapshotMode string

const (
	StateSnapshotModeLive     StateSnapshotMode = "live"
	StateSnapshotModeTerminal StateSnapshotMode = "terminal"
)

type SnapshotContainerStateInput struct {
	ContainerID string
	OperationID string
	Mode        StateSnapshotMode
}

type StateVolumeGenerationReceipt struct {
	VolumeID                string
	GenerationID            string
	Generation              int64
	Name                    string
	MountPath               string
	ReadOnly                bool
	Root                    bool
	Reused                  bool
	ParentGenerationID      string
	CloneParentGenerationID string
	Depth                   int
	VirtualSizeBytes        int64
	Manifest                BlockV1Manifest
}

type SnapshotContainerStateReceipt struct {
	StateSnapshotID string
	Status          string
	Generations     []StateVolumeGenerationReceipt
	CheckpointID    string
	RestoreMode     string
	FallbackReason  string
}

type RestoreContainerStateInput struct {
	ContainerID     string
	OperationID     string
	StateSnapshotID string
	Generations     map[string]string
}

type RestoreContainerStateReceipt struct {
	StateSnapshotID string
	Status          string
	Generations     []StateVolumeGenerationReceipt
	CheckpointID    string
	RestoreMode     string
	FallbackReason  string
}

// BlockV1CAS stores immutable objects addressed by their lowercase SHA-256
// digest. Put must be idempotent for an object that already exists.
type BlockV1CAS interface {
	Put(ctx context.Context, digest string, size int64, body io.Reader) error
	Get(ctx context.Context, digest string, expectedSize int64) (io.ReadCloser, error)
}

type BlockV1ManifestResolver interface {
	ResolveBlockV1Manifest(ctx context.Context, generationID string) (BlockV1Manifest, error)
}

type StateVolumeSpec struct {
	ID                            string
	Name                          string
	ContainerMountPath            string
	Root                          bool
	ReadOnly                      bool
	Generation                    int64
	CurrentGenerationID           string
	LineageSourceGenerationID     string
	SourceVolumeID                string
	SourceGeneration              int64
	SourceParentGenerationID      string
	SourceCloneParentGenerationID string
	SourceDepth                   int
	BackingDir                    string
	MountPath                     string
	SizeBytes                     int64
	ActiveLayerPath               string
	ActiveBackingPath             string
	ReadOnlyLayerRoot             string
	ParentGenerationID            string
	CloneParentGenerationID       string
	AttachmentToken               string
	FencingToken                  int64
	Depth                         int
	// CreateLayer asks the manager to create ActiveLayerPath as a writable
	// qcow2 child of ActiveBackingPath. Unlike Format, it must never initialize
	// the guest filesystem. The complete group is journaled before creation.
	CreateLayer bool
	Format      bool
}

// StateVolumeSourceGeneration is the worker-authenticated source membership
// actually mounted for a restore. It is derived from verified manifests and
// the mounted group, never copied from repository metadata at receipt time.
type StateVolumeSourceGeneration struct {
	VolumeID                string
	GenerationID            string
	Generation              int64
	Name                    string
	MountPath               string
	ReadOnly                bool
	Root                    bool
	ParentGenerationID      string
	CloneParentGenerationID string
	Depth                   int
}

type StateVolumeGroupSpec struct {
	ContainerID           string
	SourceStateSnapshotID string
	Volumes               []StateVolumeSpec
}

type StateVolumePivotGeneration struct {
	VolumeID                     string
	GenerationID                 string
	Generation                   int64
	Name                         string
	MountPath                    string
	ReadOnly                     bool
	Root                         bool
	Reused                       bool
	LayerPath                    string
	BackingPath                  string
	ParentGenerationID           string
	CloneParentGenerationID      string
	Compaction                   bool
	CompactionSourceGenerationID string
	VirtualSizeBytes             int64
	Depth                        int
}

type StateVolumePivotReceipt struct {
	ContainerID string
	OperationID string
	Generations []StateVolumePivotGeneration
}
