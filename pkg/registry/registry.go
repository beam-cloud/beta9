package registry

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	S3ImageRegistryStore     = "s3"
	LocalImageRegistryStore  = "local"
	remoteImageFileExtension = "rclip"
	localImageFileExtension  = "clip"
)

type ImageRegistry struct {
	store              ObjectStore
	config             types.ImageServiceConfig
	ImageFileExtension string
}

func NewImageRegistry(config types.AppConfig) (*ImageRegistry, error) {
	var err error
	var store ObjectStore

	var imageFileExtension string = localImageFileExtension
	switch config.ImageService.RegistryStore {
	case S3ImageRegistryStore:
		imageFileExtension = remoteImageFileExtension
		store, err = NewS3Store(config)
		if err != nil {
			return nil, err
		}
	default:
		imageFileExtension = localImageFileExtension
		store, err = NewLocalObjectStore()
		if err != nil {
			return nil, err
		}
	}

	return &ImageRegistry{
		store:              store,
		config:             config.ImageService,
		ImageFileExtension: imageFileExtension,
	}, nil
}

func (r *ImageRegistry) Exists(ctx context.Context, imageId string) bool {
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

type ObjectStore interface {
	Put(ctx context.Context, localPath string, key string) error
	Get(ctx context.Context, key string, localPath string) error
	Exists(ctx context.Context, key string) bool
	Size(ctx context.Context, key string) (int64, error)
}

type S3Store struct {
	client *s3.Client
	config types.S3ImageRegistryConfig
}

func NewS3Store(config types.AppConfig) (*S3Store, error) {
	cfg, err := common.GetAWSConfig(config.ImageService.Registries.S3.AccessKey, config.ImageService.Registries.S3.SecretKey, config.ImageService.Registries.S3.Region, config.ImageService.Registries.S3.Endpoint)
	if err != nil {
		return nil, err
	}

	return &S3Store{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			if config.ImageService.Registries.S3.ForcePathStyle {
				o.UsePathStyle = true
			}
		}),
		config: config.ImageService.Registries.S3,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, localPath string, key string) error {
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

	return nil
}

func (s *S3Store) Get(ctx context.Context, key string, localPath string) error {
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

	return nil
}

// Exists returns true if the object exists
func (s *S3Store) Exists(ctx context.Context, key string) bool {
	_, err := s.headObject(ctx, key)
	return err == nil
}

// Size returns the size of the object in bytes
func (s *S3Store) Size(ctx context.Context, key string) (int64, error) {
	res, err := s.headObject(ctx, key)
	if err != nil {
		return 0, err
	}
	return *res.ContentLength, nil
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

func (s *LocalObjectStore) Exists(ctx context.Context, key string) bool {
	_, err := os.Stat(filepath.Join(s.Path, key))
	return err == nil
}

func (s *LocalObjectStore) Size(ctx context.Context, key string) (int64, error) {
	fileInfo, err := os.Stat(filepath.Join(s.Path, key))
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
