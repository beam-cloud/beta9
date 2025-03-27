package clients

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/beta9/pkg/types"
)

type StorageClient struct {
	WorkspaceName    string
	WorkspaceStorage *types.WorkspaceStorage
	s3Client         *s3.Client
	presignClient    *s3.PresignClient
}

func NewStorageClient(ctx context.Context, workspaceName string, workspaceStorage *types.WorkspaceStorage) (*StorageClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*workspaceStorage.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			*workspaceStorage.AccessKey,
			*workspaceStorage.SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// If a custom endpoint is provided, use it
	if workspaceStorage.EndpointUrl != nil {
		cfg.BaseEndpoint = aws.String(*workspaceStorage.EndpointUrl)
	}

	s3Client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(s3Client)

	return &StorageClient{
		WorkspaceName:    workspaceName,
		WorkspaceStorage: workspaceStorage,
		s3Client:         s3Client,
		presignClient:    presignClient,
	}, nil
}

func (c *StorageClient) Upload(ctx context.Context, key string, data []byte) error {
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (c *StorageClient) Head(ctx context.Context, key string) (bool, *s3.HeadObjectOutput, error) {
	output, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return false, nil, err
	}

	return true, output, nil
}

func (c *StorageClient) Download(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (c *StorageClient) Delete(ctx context.Context, key string) error {
	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
	})
	return err
}

func (c *StorageClient) ListObjects(ctx context.Context, prefix string) ([]s3types.Object, error) {
	resp, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}
	return resp.Contents, nil
}

func (c *StorageClient) GeneratePresignedPutURL(ctx context.Context, key string, expiresInSeconds int64) (string, error) {
	result, err := c.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Duration(expiresInSeconds)*time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return result.URL, nil
}

func (c *StorageClient) GeneratePresignedGetURL(ctx context.Context, key string, expiresInSeconds int64) (string, error) {
	result, err := c.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Duration(expiresInSeconds)*time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return result.URL, nil
}

func (c *StorageClient) GeneratePresignedPutURLs(ctx context.Context, keys []string, expiresInSeconds int64) (map[string]string, error) {
	urls := make(map[string]string)
	for _, key := range keys {
		url, err := c.GeneratePresignedPutURL(ctx, key, expiresInSeconds)
		if err != nil {
			return nil, fmt.Errorf("failed to generate presigned URL for key %s: %w", key, err)
		}
		urls[key] = url
	}

	return urls, nil
}

func (c *StorageClient) ListWithPrefix(ctx context.Context, prefix string) ([]s3types.Object, error) {
	resp, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	return resp.Contents, nil
}

func (c *StorageClient) DeleteWithPrefix(ctx context.Context, prefix string) error {
	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(prefix),
	})
	return err
}
