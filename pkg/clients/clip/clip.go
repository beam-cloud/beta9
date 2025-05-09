package clip

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	clipv1 "github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	clipv2 "github.com/beam-cloud/clip/pkg/v2"
	"github.com/rs/zerolog/log"
)

const (
	ClipFileFormatVersion1 uint32 = uint32(clipCommon.ClipFileFormatVersion)
	ClipFileFormatVersion2 uint32 = uint32(clipv2.ClipV2FileFormatVersion)
)

type V1CreateArchiveOptions struct {
	PrimaryRegistry *registry.ImageRegistry
}

type V2CreateArchiveOptions struct {
	MaxChunkSize int64
}

type CreateArchiveOptions struct {
	// Clip setup options
	RegistryStoreType string
	ClipVersion       uint32

	// Mounting context options
	ImageID      string
	BundlePath   string
	BundleSize   float64
	ArchivePath  string
	Verbose      bool
	ProgressChan chan int
	S3Config     types.S3ImageRegistryConfig

	// Config that is specific to v1/v2
	V1 V1CreateArchiveOptions
	V2 V2CreateArchiveOptions
}

func CreateArchive(ctx context.Context, options CreateArchiveOptions) error {
	var err error

	switch options.ClipVersion {
	case ClipFileFormatVersion1:
		// Shared options for all registry stores
		createOpts := clipv1.CreateOptions{
			InputPath:  options.BundlePath,
			OutputPath: options.ArchivePath,
		}

		switch options.RegistryStoreType {
		case registry.S3ImageRegistryStore:
			createOpts.Credentials = storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: options.S3Config.Primary.AccessKey,
					SecretKey: options.S3Config.Primary.SecretKey,
				},
			}
			createOpts.ProgressChan = options.ProgressChan

			err = clipv1.CreateAndUploadArchive(ctx, createOpts, clipCommon.S3StorageInfo{
				Bucket:         options.S3Config.Primary.BucketName,
				Region:         options.S3Config.Primary.Region,
				Endpoint:       options.S3Config.Primary.Endpoint,
				Key:            fmt.Sprintf("%s.clip", options.ImageID),
				ForcePathStyle: options.S3Config.Primary.ForcePathStyle,
			})
		case registry.LocalImageRegistryStore:
			err = clipv1.CreateArchive(createOpts)
		}
		// Push the archive to a registry
		startTime := time.Now()
		err := options.V1.PrimaryRegistry.Push(ctx, options.ArchivePath, options.ImageID)
		if err != nil {
			log.Error().Str("image_id", options.ImageID).Err(err).Msg("failed to push image")
			return err
		}

		elapsed := time.Since(startTime)
		log.Info().Str("image_id", options.ImageID).Dur("seconds", time.Duration(elapsed.Seconds())).Float64("size", options.BundleSize).Msg("image push took")
		metrics.RecordImagePushSpeed(options.BundleSize, elapsed)

	case ClipFileFormatVersion2:
		// Shared options for all registry stores
		createOpts := clipv2.CreateOptions{
			ImageID:      options.ImageID,
			SourcePath:   options.BundlePath,
			LocalPath:    options.ArchivePath,
			MaxChunkSize: options.V2.MaxChunkSize,
			Verbose:      options.Verbose,
			ProgressChan: options.ProgressChan,
		}

		switch options.RegistryStoreType {
		case registry.S3ImageRegistryStore:
			createOpts.S3Config = clipCommon.S3StorageInfo{
				AccessKey: options.S3Config.Primary.AccessKey,
				SecretKey: options.S3Config.Primary.SecretKey,
				Region:    options.S3Config.Primary.Region,
				Bucket:    options.S3Config.Primary.BucketName,
				Endpoint:  options.S3Config.Primary.Endpoint,
			}
			createOpts.StorageType = clipCommon.StorageModeS3
			err = clipv2.CreateArchive(createOpts)
		case registry.LocalImageRegistryStore:
			createOpts.StorageType = clipCommon.StorageModeLocal
			err = clipv2.CreateArchive(createOpts)
		}
	}
	return err
}
