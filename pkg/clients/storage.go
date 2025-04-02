package clients

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
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

func (c *StorageClient) S3Client() *s3.Client {
	return c.s3Client
}

func (c *StorageClient) PresignClient() *s3.PresignClient {
	return c.presignClient
}

func (c *StorageClient) BucketName() string {
	return *c.WorkspaceStorage.BucketName
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

func (c *StorageClient) ListDirectory(ctx context.Context, dir string) ([]s3types.Object, error) {
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}

	resp, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(*c.WorkspaceStorage.BucketName),
		Prefix:    aws.String(dir),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	var allObjects []s3types.Object

	// Add regular files
	for _, obj := range resp.Contents {
		if *obj.Key != dir {
			allObjects = append(allObjects, obj)
		}
	}

	// Add directories
	for _, cp := range resp.CommonPrefixes {
		dirObject := s3types.Object{
			Key:          cp.Prefix,
			Size:         aws.Int64(0),
			LastModified: aws.Time(time.Now()),
		}

		allObjects = append(allObjects, dirObject)
	}

	return allObjects, nil
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

func (c *StorageClient) DeleteWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	var continuationToken *string
	var deletedObjects []string

	for {
		resp, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(*c.WorkspaceStorage.BucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}

		// Prepare the objects to delete
		var objectsToDelete []s3types.ObjectIdentifier
		for _, obj := range resp.Contents {
			objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{Key: obj.Key})
			deletedObjects = append(deletedObjects, strings.TrimPrefix(*obj.Key, prefix))
		}

		// Delete the objects in batches
		if len(objectsToDelete) > 0 {
			_, err = c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(*c.WorkspaceStorage.BucketName),
				Delete: &s3types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				return nil, err
			}
		}

		// Check if there are more objects to process
		if resp.IsTruncated != nil && !*resp.IsTruncated {
			break
		}

		continuationToken = resp.NextContinuationToken
	}

	return deletedObjects, nil
}

func (c *StorageClient) MoveObject(ctx context.Context, sourceKey, destinationKey string) error {
	_, err := c.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(*c.WorkspaceStorage.BucketName),
		CopySource: aws.String(fmt.Sprintf("%s/%s", *c.WorkspaceStorage.BucketName, sourceKey)),
		Key:        aws.String(destinationKey),
	})
	if err != nil {
		return fmt.Errorf("failed to copy file from %s to %s: %w", sourceKey, destinationKey, err)
	}

	// Delete the original object
	_, err = c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(sourceKey),
	})
	if err != nil {
		return fmt.Errorf("failed to delete original file %s: %w", sourceKey, err)
	}

	return nil
}

const (
	testObjectKey = "test-access-object"
)

func (c *StorageClient) ValidateBucketAccess(ctx context.Context) error {
	_, err := c.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to access bucket: %w", err)
	}

	// Check write access by attempting to put an object with no content
	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(*c.WorkspaceStorage.BucketName),
		Key:    aws.String(testObjectKey),
		Body:   bytes.NewReader([]byte{}), // Empty body
		ACL:    s3types.ObjectCannedACLBucketOwnerFullControl,
	})
	if err != nil {
		return fmt.Errorf("failed to write to bucket: %w", err)
	}

	// Clean up by deleting the test object
	if err := c.Delete(ctx, testObjectKey); err != nil {
		return fmt.Errorf("failed to delete test object: %w", err)
	}

	return nil
}
