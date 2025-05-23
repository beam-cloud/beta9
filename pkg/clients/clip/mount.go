package clip

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	clipv1 "github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	clipv2 "github.com/beam-cloud/clip/pkg/v2"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	StorageModeLocal = clipCommon.StorageModeLocal
	StorageModeS3    = clipCommon.StorageModeS3
)

type V1MountArchiveOptions struct {
	CachePath   string
	ArchivePath string
}

type V2MountArchiveOptions struct {
	StorageType               clipCommon.StorageMode
	WarmChunks                bool
	PriorityChunks            []string
	SetPriorityChunksCallback func(chunks []string) error
	PriorityChunkSampleTime   time.Duration
}

type MountArchiveOptions struct {
	ClipVersion uint32

	// Shared options for both v1/v2
	ImageID               string
	ImageMountPath        string
	S3Config              types.S3ImageRegistry
	ContentCache          *blobcache.BlobCacheClient
	ContentCacheAvailable bool

	// Config that is specific to v1/v2
	V1 V1MountArchiveOptions
	V2 V2MountArchiveOptions
}

func MountArchive(ctx context.Context, options MountArchiveOptions) (func() error, <-chan error, *fuse.Server, error) {
	switch options.ClipVersion {
	case ClipFileFormatVersion1:
		return clipv1.MountArchive(clipv1.MountOptions{
			ArchivePath:           options.V1.ArchivePath,
			MountPoint:            fmt.Sprintf("%s/%s", options.ImageMountPath, options.ImageID),
			CachePath:             options.V1.CachePath,
			ContentCache:          options.ContentCache,
			ContentCacheAvailable: options.ContentCacheAvailable,
			Credentials: storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: options.S3Config.AccessKey,
					SecretKey: options.S3Config.SecretKey,
				},
			},
			StorageInfo: &clipCommon.S3StorageInfo{
				Bucket:         options.S3Config.BucketName,
				Region:         options.S3Config.Region,
				Endpoint:       options.S3Config.Endpoint,
				Key:            fmt.Sprintf("%s.%s", options.ImageID, registry.LocalImageFileExtension),
				ForcePathStyle: options.S3Config.ForcePathStyle,
			},
		})
	case ClipFileFormatVersion2:
		return clipv2.MountArchive(context.Background(), clipv2.MountOptions{
			ExtractOptions: clipv2.ExtractOptions{
				ImageID:     options.ImageID,
				StorageType: clipCommon.StorageModeS3,
				S3Config: clipCommon.S3StorageInfo{
					Bucket:         options.S3Config.BucketName,
					Region:         options.S3Config.Region,
					Endpoint:       options.S3Config.Endpoint,
					ForcePathStyle: options.S3Config.ForcePathStyle,
					AccessKey:      options.S3Config.AccessKey,
					SecretKey:      options.S3Config.SecretKey,
				},
			},
			MountPoint:                fmt.Sprintf("%s/%s", options.ImageMountPath, options.ImageID),
			ContentCache:              options.ContentCache,
			ContentCacheAvailable:     options.ContentCacheAvailable,
			WarmChunks:                false,
			PriorityChunks:            options.V2.PriorityChunks,
			SetPriorityChunksCallback: options.V2.SetPriorityChunksCallback,
			PriorityChunkSampleTime:   options.V2.PriorityChunkSampleTime,
		})
	}

	return nil, nil, nil, fmt.Errorf("invalid clip version: %d", options.ClipVersion)
}
