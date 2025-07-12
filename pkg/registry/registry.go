package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	S3ImageRegistryStore     = "s3"
	LocalImageRegistryStore  = "local"
	RemoteImageFileExtension = "rclip"
	LocalImageFileExtension  = "clip"
)

type ImageRegistry struct {
	store              ObjectStore
	config             types.ImageServiceConfig
	ImageFileExtension string
	registry           types.S3ImageRegistry
}

func NewImageRegistry(config types.AppConfig, registry types.S3ImageRegistry) (*ImageRegistry, error) {
	var (
		err                error
		store              ObjectStore
		imageFileExtension string = LocalImageFileExtension
	)

	switch config.ImageService.RegistryStore {
	case S3ImageRegistryStore:
		imageFileExtension = RemoteImageFileExtension
		store, err = NewS3Store(registry)
		if err != nil {
			return nil, err
		}
	default:
		imageFileExtension = LocalImageFileExtension
		store, err = NewLocalObjectStore()
		if err != nil {
			return nil, err
		}
	}

	return &ImageRegistry{
		store:              store,
		config:             config.ImageService,
		ImageFileExtension: imageFileExtension,
		registry:           registry,
	}, nil
}

func (r *ImageRegistry) Registry() types.S3ImageRegistry {
	return r.registry
}

func (r *ImageRegistry) Exists(ctx context.Context, imageId string) (bool, error) {
	return r.store.Exists(ctx, fmt.Sprintf("%s.%s", imageId, r.ImageFileExtension))
}

func (r *ImageRegistry) Push(ctx context.Context, localPath string, imageId string) error {
	return r.store.Put(ctx, localPath, fmt.Sprintf("%s.%s", imageId, r.ImageFileExtension))
}

func (r *ImageRegistry) Pull(ctx context.Context, localPath string, imageId string) error {
	return r.store.Get(ctx, fmt.Sprintf("%s.%s", imageId, r.ImageFileExtension), localPath)
}

func (r *ImageRegistry) Size(ctx context.Context, imageId string) (int64, error) {
	return r.store.Size(ctx, fmt.Sprintf("%s.%s", imageId, r.ImageFileExtension))
}

func (r *ImageRegistry) CopyImageFromRegistry(ctx context.Context, imageId string, sourceRegistry *ImageRegistry) error {
	objects := []string{fmt.Sprintf("%s.%s", imageId, LocalImageFileExtension), fmt.Sprintf("%s.%s", imageId, RemoteImageFileExtension)}
	return copyObjects(ctx, objects, sourceRegistry.store, r.store)
}

type ObjectStore interface {
	Put(ctx context.Context, localPath string, key string) error
	Get(ctx context.Context, key string, localPath string) error
	Exists(ctx context.Context, key string) (bool, error)
	Size(ctx context.Context, key string) (int64, error)
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
	PutReader(ctx context.Context, reader io.Reader, key string) error
}

type S3Store struct {
	client *s3.Client
	config types.S3ImageRegistry
}

func NewS3Store(config types.S3ImageRegistry) (*S3Store, error) {
	cfg, err := common.GetAWSConfig(config.AccessKey, config.SecretKey, config.Region, config.Endpoint)
	if err != nil {
		return nil, err
	}

	return &S3Store{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			if config.ForcePathStyle {
				o.UsePathStyle = true
			}
		}),
		config: config,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, localPath string, key string) error {
	start := time.Now()
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	uploader := manager.NewUploader(s.client)
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		log.Error().Str("key", key).Err(err).Msg("error uploading image to registry")
		return err
	}

	info, err := os.Stat(localPath)
	if err != nil {
		log.Error().Str("key", key).Err(err).Msg("error getting file size")
		return err
	}
	sizeMB := float64(info.Size()) / 1024 / 1024

	metrics.RecordS3PutSpeed(sizeMB, time.Since(start))
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string, localPath string) error {
	start := time.Now()
	tmpLocalPath := fmt.Sprintf("%s.%s", localPath, uuid.New().String()[:6])

	f, err := os.Create(tmpLocalPath)
	if err != nil {
		log.Error().Str("key", key).Err(err).Msg("error creating temp file for image download")
		return err
	}

	downloader := manager.NewDownloader(s.client)
	_, err = downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	}, func(d *manager.Downloader) {
		d.PartSize = 100 * 1024 * 1024 // 100MiB per part
		d.Concurrency = 10
	})

	if err != nil {
		f.Close()
		os.Remove(tmpLocalPath)
		return err
	}

	f.Close()

	err = os.Rename(tmpLocalPath, localPath)
	if err != nil {
		return err
	}

	info, err := os.Stat(localPath)
	if err != nil {
		log.Error().Str("key", key).Err(err).Msg("error getting file size")
		return nil
	}
	sizeMB := float64(info.Size()) / 1024 / 1024
	metrics.RecordS3GetSpeed(sizeMB, time.Since(start))

	return nil
}

// Exists returns true if the object exists
func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := s.objectExists(ctx, key)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// Size returns the size of the object in bytes
func (s *S3Store) Size(ctx context.Context, key string) (int64, error) {
	res, err := s.headObject(ctx, key)
	if err != nil {
		return 0, err
	}
	return *res.ContentLength, nil
}

func (s *S3Store) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3Store) PutReader(ctx context.Context, reader io.Reader, key string) error {
	uploader := manager.NewUploader(s.client)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
		Body:   reader,
	})
	return err
}

// headObject returns the metadata of an object
func (s *S3Store) headObject(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-0"),
	})
	if err != nil {
		return nil, err
	}

	return s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
}

// objectExists quickly checks if an object exists with the extra head request
// as we don't need the metadata here
func (s *S3Store) objectExists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-0"),
	})

	if err != nil {
		if errors.As(err, new(*s3types.NoSuchKey)) || errors.As(err, new(*s3types.NotFound)) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func NewLocalObjectStore() (*LocalObjectStore, error) {
	return &LocalObjectStore{
		Path: "/images",
	}, nil
}

type LocalObjectStore struct {
	Path string
}

func (s *LocalObjectStore) Put(ctx context.Context, localPath string, key string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	srcFile, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destPath := filepath.Join(s.Path, key)
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	return nil
}

func (s *LocalObjectStore) Get(ctx context.Context, key string, localPath string) error {
	srcPath := filepath.Join(s.Path, key)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	return nil
}

func (s *LocalObjectStore) Exists(ctx context.Context, key string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.Path, key))
	return err == nil, nil
}

func (s *LocalObjectStore) Size(ctx context.Context, key string) (int64, error) {
	fileInfo, err := os.Stat(filepath.Join(s.Path, key))
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (s *LocalObjectStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.Path, key))
}

func (s *LocalObjectStore) PutReader(ctx context.Context, reader io.Reader, key string) error {
	destPath := filepath.Join(s.Path, key)
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, reader)
	return err
}

// copyObjects copies a list of objects from one registry to another
func copyObjects(ctx context.Context, keys []string, sourceObjectStore, destinationObjectStore ObjectStore) error {
	log.Info().Msgf("registry miss for objects <%v>, pulling from source registry", keys)

	for _, key := range keys {
		log.Info().Str("key", key).Msg("copying object")

		reader, err := sourceObjectStore.GetReader(ctx, key)
		if err != nil {
			log.Error().Err(err).Str("key", key).Msg("failed to get object from source object store")
			return err
		}
		defer reader.Close()

		err = destinationObjectStore.PutReader(ctx, reader, key)
		if err != nil {
			log.Error().Err(err).Str("key", key).Msg("failed to put object in destination object store")
			return err
		}

		log.Info().Str("key", key).Msg("successfully copied object")
	}
	return nil
}
