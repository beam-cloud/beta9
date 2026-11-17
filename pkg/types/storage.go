package types

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"
)

// WorkspaceMountPath is the canonical source path sent to a worker. The worker
// resolves it against its configured workspace mount root.
func WorkspaceMountPath(workspaceName string, parts ...string) string {
	return path.Join(append([]string{AgentWorkspacePath, workspaceName}, parts...)...)
}

const (
	StorageModeMountPoint  = "mountpoint"
	StorageModeGeese       = "geese"
	StorageModeLocal       = "local"
	StorageModeDurableDisk = "durable_disk"
)

const (
	DurableDiskDriverSnapshot        = "snapshot"
	DurableDiskDriverQcow            = "qcow"
	CheckpointModelCacheVolumePrefix = "checkpoint-model-cache"
)

func NormalizeDurableDiskDriver(driver string) string {
	return strings.ToLower(strings.TrimSpace(driver))
}

func SafeDurableDiskName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\\", "-")
	name = strings.ReplaceAll(name, "/", "-")
	if name == "" || name == "." || name == ".." {
		return "disk"
	}
	return name
}

// RootLayerDiskPrefix begins the names of the disks that are root layers. No
// request may name a disk that begins with it: a root layer is mounted by the
// host, so it must never have been a guest's block device.
const RootLayerDiskPrefix = "root-layer-"

// RootLayer is a generation of a workspace's durable disk that goes under a
// VM's image, read-only: its files are in the VM's root wherever the image
// has none. A worker attaches a generation once and shares it between its
// VMs, and a VM starts without one its worker does not have yet.
//
// Only workers write the disk. It is set by the gateway's own callers and is
// not an option of any API.
type RootLayer struct {
	// DiskName names the disk in the workspace. It begins with
	// RootLayerDiskPrefix.
	DiskName string `json:"disk_name"`
	// Size is the disk's virtual size, as a durable disk's.
	Size string `json:"size"`
	// SnapshotID is the generation that goes under the root. It is empty
	// until the disk has one.
	SnapshotID string `json:"snapshot_id,omitempty"`
	// Publish has the worker add what the VM's workload leaves in Paths to
	// the disk when the VM exits, as a generation whose parent is SnapshotID.
	// Nothing uses that generation until the gateway chooses it.
	Publish bool `json:"publish,omitempty"`
	// Paths are the directories of the VM's root that the disk holds.
	Paths []string `json:"paths,omitempty"`
}

type rootLayerContextKey struct{}

// WithRootLayer has the stub made with ctx put layer under its containers'
// roots. A context value, so that only code in the gateway can ask for it.
func WithRootLayer(ctx context.Context, layer *RootLayer) context.Context {
	if layer == nil || !strings.HasPrefix(layer.DiskName, RootLayerDiskPrefix) {
		return ctx
	}
	return context.WithValue(ctx, rootLayerContextKey{}, layer)
}

func RootLayerFromContext(ctx context.Context) *RootLayer {
	layer, _ := ctx.Value(rootLayerContextKey{}).(*RootLayer)
	return layer
}

func CheckpointModelCacheVolumeName(stubName string) string {
	name := SafeDurableDiskName(stubName)
	if name == "disk" {
		return CheckpointModelCacheVolumePrefix
	}
	return fmt.Sprintf("%s-%s", CheckpointModelCacheVolumePrefix, name)
}

func IsDiskSnapshotFilesystemFormat(format string) bool {
	switch format {
	case DiskSnapshotFormatDirV1, DiskSnapshotFormatPostgresWalV1, DiskSnapshotFormatRedisAOFV1:
		return true
	default:
		return false
	}
}

// Disk is a first-class durable disk resource. Durable disks provide
// node-local durable state that is snapshotted to object storage. A disk is
// referenced by name when attached to a stub/service.
type Disk struct {
	Id          uint     `db:"id" json:"id"`
	ExternalId  string   `db:"external_id" json:"external_id"`
	WorkspaceId uint     `db:"workspace_id" json:"workspace_id"`
	Name        string   `db:"name" json:"name"`
	Size        string   `db:"size" json:"size"`
	Filesystem  string   `db:"filesystem" json:"filesystem"`
	Driver      string   `db:"driver" json:"driver"`
	MountPath   string   `db:"mount_path" json:"mount_path"`
	CreatedAt   Time     `db:"created_at" json:"created_at"`
	UpdatedAt   Time     `db:"updated_at" json:"updated_at"`
	DeletedAt   NullTime `db:"deleted_at" json:"deleted_at,omitempty"`
}

// DiskCreateInput is the shared input for single and batched disk creation.
type DiskCreateInput struct {
	WorkspaceID uint   `json:"workspace_id"`
	Name        string `json:"name"`
	Size        string `json:"size"`
	Filesystem  string `json:"filesystem"`
	Driver      string `json:"driver"`
	MountPath   string `json:"mount_path"`
}

type DiskWithRelated struct {
	Disk
	Workspace Workspace `db:"workspace" json:"workspace"`
}

type DiskSnapshotStatus string

const (
	DiskSnapshotStatusPending   DiskSnapshotStatus = "pending"
	DiskSnapshotStatusAvailable DiskSnapshotStatus = "available"
	DiskSnapshotStatusFailed    DiskSnapshotStatus = "failed"

	DiskSnapshotFormatDirV1         = "dir.v1"
	DiskSnapshotFormatPostgresWalV1 = "postgres.wal.v1"
	DiskSnapshotFormatRedisAOFV1    = "redis.aof.v1"
	// DiskSnapshotFormatQcowV1 is a block snapshot: one immutable qcow2 layer
	// stored as sparse content-addressed chunks, chained via ParentSnapshotId.
	DiskSnapshotFormatQcowV1 = "qcow.v1"
)

type DiskSnapshot struct {
	Id                  uint               `db:"id" json:"id,omitempty"`
	ExternalId          string             `db:"external_id" json:"external_id"`
	WorkspaceId         uint               `db:"workspace_id" json:"workspace_id"`
	StubId              uint               `db:"stub_id" json:"stub_id,omitempty"`
	DiskName            string             `db:"disk_name" json:"disk_name"`
	Format              string             `db:"format" json:"format"`
	Status              DiskSnapshotStatus `db:"status" json:"status"`
	Reason              string             `db:"reason" json:"reason,omitempty"`
	ParentSnapshotId    string             `db:"parent_snapshot_id" json:"parent_snapshot_id,omitempty"`
	Generation          int64              `db:"generation" json:"generation"`
	SizeBytes           int64              `db:"size_bytes" json:"size_bytes"`
	Filesystem          string             `db:"filesystem" json:"filesystem"`
	Driver              string             `db:"driver" json:"driver"`
	ManifestKey         string             `db:"manifest_key" json:"manifest_key"`
	ManifestDigest      string             `db:"manifest_digest" json:"manifest_digest"`
	ManifestSizeBytes   int64              `db:"manifest_size_bytes" json:"manifest_size_bytes"`
	ChunkCount          int64              `db:"chunk_count" json:"chunk_count"`
	LogicalSizeBytes    int64              `db:"logical_size_bytes" json:"logical_size_bytes"`
	StoredSizeBytes     int64              `db:"stored_size_bytes" json:"stored_size_bytes"`
	BucketName          string             `db:"bucket_name" json:"bucket_name"`
	ObjectPrefix        string             `db:"object_prefix" json:"object_prefix"`
	SourcePool          string             `db:"source_pool" json:"source_pool,omitempty"`
	SourceWorkerId      string             `db:"source_worker_id" json:"source_worker_id,omitempty"`
	SourceStorageNodeId string             `db:"source_storage_node_id" json:"source_storage_node_id,omitempty"`
	CreatedAt           Time               `db:"created_at" json:"created_at"`
	UpdatedAt           Time               `db:"updated_at" json:"updated_at"`
	CompletedAt         NullTime           `db:"completed_at" json:"completed_at,omitempty"`
	DeletedAt           NullTime           `db:"deleted_at" json:"deleted_at,omitempty"`
	Public              bool               `db:"public" json:"public"`
}

type DiskSnapshotFilter struct {
	WorkspaceId         uint
	WorkspaceExternalId string
	DiskName            string
	Statuses            []DiskSnapshotStatus
	Limit               uint64
}

type DiskSnapshotManifest struct {
	Version          int                `json:"version"`
	Format           string             `json:"format"`
	DiskName         string             `json:"disk_name"`
	Filesystem       string             `json:"filesystem"`
	Generation       int64              `json:"generation"`
	ParentSnapshotId string             `json:"parent_snapshot_id,omitempty"`
	LogicalSizeBytes int64              `json:"logical_size_bytes"`
	StoredSizeBytes  int64              `json:"stored_size_bytes"`
	Files            []DiskSnapshotFile `json:"files,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
}

type DiskSnapshotFile struct {
	Path            string              `json:"path"`
	Type            string              `json:"type"`
	Mode            int64               `json:"mode"`
	Uid             int                 `json:"uid,omitempty"`
	Gid             int                 `json:"gid,omitempty"`
	SizeBytes       int64               `json:"size_bytes,omitempty"`
	ModTimeUnixNano int64               `json:"mtime_unix_nano,omitempty"`
	ChangeUnixNano  int64               `json:"ctime_unix_nano,omitempty"`
	DeviceId        uint64              `json:"device_id,omitempty"`
	Inode           uint64              `json:"inode,omitempty"`
	LinkName        string              `json:"link_name,omitempty"`
	Chunks          []DiskSnapshotChunk `json:"chunks,omitempty"`
}

type DiskSnapshotChunk struct {
	Index       int64  `json:"index"`
	OffsetBytes int64  `json:"offset_bytes"`
	SizeBytes   int64  `json:"size_bytes"`
	ObjectKey   string `json:"object_key"`
	Digest      string `json:"digest"`
}

type ErrDiskSnapshotNotFound struct {
	SnapshotId string
}

func (e *ErrDiskSnapshotNotFound) Error() string {
	return fmt.Sprintf("disk snapshot not found: %s", e.SnapshotId)
}

type ErrDiskSnapshotNotAvailable struct {
	SnapshotId string
}

func (e *ErrDiskSnapshotNotAvailable) Error() string {
	return fmt.Sprintf("disk snapshot is not available: %s", e.SnapshotId)
}
