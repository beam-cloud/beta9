package common

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/internal/types"
)

const (
	DefaultAWSRegion string = "us-east-1"

	s3ImageRegistryStoreName = "s3"
	remoteImageFileExtension = "rclip"
	localImageFileExtension  = "clip"
)

type ImageRegistry struct {
	store              ObjectStore
	config             types.ImageServiceConfig
	ImageFileExtension string
}

func NewImageRegistry(config types.ImageServiceConfig) (*ImageRegistry, error) {
	var err error
	var store ObjectStore

	var imageFileExtension string = "clip"
	switch config.RegistryStore {
	case s3ImageRegistryStoreName:
		imageFileExtension = remoteImageFileExtension
		store, err = NewS3Store(config.Registries.S3)
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
		config:             config,
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

func NewS3Store(config types.S3ImageRegistryConfig) (*S3Store, error) {
	cfg, err := getAWSConfig(config.AccessKeyID, config.SecretAccessKey, config.Region)
	if err != nil {
		return nil, err
	}

	return &S3Store{
		client: s3.NewFromConfig(cfg),
		config: config,
	}, nil
}

func getAWSConfig(accessKey string, secretKey string, region string) (aws.Config, error) {
	var cfg aws.Config
	var err error

	if accessKey == "" || secretKey == "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	} else {
		credentials := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithCredentialsProvider(credentials))
	}

	return cfg, err
}

type S3Store struct {
	client *s3.Client
	config types.S3ImageRegistryConfig
}

func (s *S3Store) Put(ctx context.Context, localPath string, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		log.Printf("error opening file<%s>: %v", localPath, err)
		return err
	}
	defer f.Close()

	uploader := manager.NewUploader(s.client)
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		log.Printf("error uploading image to registry: %v", err)
		return err
	}

	return nil
}

func (s *S3Store) Get(ctx context.Context, key string, localPath string) error {
	f, err := os.Create(localPath)
	if err != nil {
		log.Println(err)
		return err
	}

	downloader := manager.NewDownloader(s.client)
	_, err = downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	}, func(d *manager.Downloader) {
		d.PartSize = 100 * 1024 * 1024 // 100MiB per part
		d.Concurrency = 10
	})

	if err != nil {
		f.Close()
		os.Remove(localPath)
		return err
	}

	f.Close()
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
	return s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
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
	srcFile, err := os.Open(localPath)
	if err != nil {
		log.Printf("error opening file<%s>: %v", localPath, err)
		return err
	}
	defer srcFile.Close()

	destPath := filepath.Join(s.Path, key)
	destFile, err := os.Create(destPath)
	if err != nil {
		log.Printf("error creating file<%s>: %v", destPath, err)
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		log.Printf("error copying file<%s>: %v", destPath, err)
		return err
	}

	return nil
}

func (s *LocalObjectStore) Get(ctx context.Context, key string, localPath string) error {
	srcPath := filepath.Join(s.Path, key)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		log.Printf("error opening file<%s>: %v\n", localPath, err)
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(localPath)
	if err != nil {
		log.Printf("error creating file<%s>: %v\n", localPath, err)
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		log.Printf("error copying file<%s>: %v\n", localPath, err)
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
