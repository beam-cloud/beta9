package clients

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestPresignEndpointForStorage(t *testing.T) {
	tests := []struct {
		name            string
		storageEndpoint string
		presignEndpoint string
		want            string
	}{
		{
			name:            "uses storage endpoint when override is empty",
			storageEndpoint: "https://s3.amazonaws.com",
			presignEndpoint: "",
			want:            "https://s3.amazonaws.com",
		},
		{
			name:            "allows loopback override for localstack service endpoint",
			storageEndpoint: "http://localstack:4566",
			presignEndpoint: "http://127.0.0.1:4566",
			want:            "http://127.0.0.1:4566",
		},
		{
			name:            "allows loopback override for localstack fqdn endpoint",
			storageEndpoint: "http://localstack.beta9.svc.cluster.local:4566",
			presignEndpoint: "http://localhost:4566",
			want:            "http://localhost:4566",
		},
		{
			name:            "rejects loopback override for remote endpoint",
			storageEndpoint: "https://s3.amazonaws.com",
			presignEndpoint: "http://127.0.0.1:4566",
			want:            "https://s3.amazonaws.com",
		},
		{
			name:            "rejects loopback override for hostname containing localstack",
			storageEndpoint: "https://notlocalstack.example.com",
			presignEndpoint: "http://127.0.0.1:4566",
			want:            "https://notlocalstack.example.com",
		},
		{
			name:            "rejects loopback override when storage endpoint is implicit aws",
			storageEndpoint: "",
			presignEndpoint: "http://127.0.0.1:4566",
			want:            "",
		},
		{
			name:            "allows non-loopback public override for remote endpoint",
			storageEndpoint: "http://minio.internal:9000",
			presignEndpoint: "https://storage.example.com",
			want:            "https://storage.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := presignEndpointForStorage(tt.storageEndpoint, tt.presignEndpoint); got != tt.want {
				t.Fatalf("presignEndpointForStorage(%q, %q) = %q, want %q", tt.storageEndpoint, tt.presignEndpoint, got, tt.want)
			}
		})
	}
}

func TestStorageMultipartUploaderConfig(t *testing.T) {
	uploader := newStorageMultipartUploader(nil, bytes.NewReader(nil))

	if uploader.PartSize != storageMultipartUploadPartSize {
		t.Fatalf("uploader PartSize = %d, want %d", uploader.PartSize, storageMultipartUploadPartSize)
	}
	if uploader.Concurrency != storageMultipartUploadWorkers {
		t.Fatalf("uploader Concurrency = %d, want %d", uploader.Concurrency, storageMultipartUploadWorkers)
	}
}

func TestStorageMultipartUploaderLimitsStreamingConcurrency(t *testing.T) {
	uploader := newStorageMultipartUploader(nil, bytes.NewBuffer(nil))

	if uploader.PartSize != storageMultipartUploadPartSize {
		t.Fatalf("uploader PartSize = %d, want %d", uploader.PartSize, storageMultipartUploadPartSize)
	}
	if uploader.Concurrency != storageStreamingUploadWorkers {
		t.Fatalf("uploader Concurrency = %d, want %d", uploader.Concurrency, storageStreamingUploadWorkers)
	}
}

func TestWorkspaceStorageClientPresignedURLRejectsLoopbackOverrideForRemoteStorage(t *testing.T) {
	storageClient, err := NewWorkspaceStorageClientWithPresignEndpoint(
		context.Background(),
		"workspace",
		testWorkspaceStorage("https://s3.amazonaws.com"),
		"http://127.0.0.1:4566",
	)
	if err != nil {
		t.Fatalf("NewWorkspaceStorageClientWithPresignEndpoint() error = %v", err)
	}

	presignedURL, err := storageClient.GeneratePresignedPutURL(context.Background(), "objects/object-id", 60)
	if err != nil {
		t.Fatalf("GeneratePresignedPutURL() error = %v", err)
	}

	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", presignedURL, err)
	}
	if got, want := parsedURL.Hostname(), "s3.amazonaws.com"; got != want {
		t.Fatalf("presigned URL hostname = %q, want %q; url=%s", got, want, presignedURL)
	}
}

func TestWorkspaceStorageClientPresignedURLAllowsLoopbackOverrideForLocalstack(t *testing.T) {
	storageClient, err := NewWorkspaceStorageClientWithPresignEndpoint(
		context.Background(),
		"workspace",
		testWorkspaceStorage("http://localstack:4566"),
		"http://127.0.0.1:4566",
	)
	if err != nil {
		t.Fatalf("NewWorkspaceStorageClientWithPresignEndpoint() error = %v", err)
	}

	presignedURL, err := storageClient.GeneratePresignedPutURL(context.Background(), "objects/object-id", 60)
	if err != nil {
		t.Fatalf("GeneratePresignedPutURL() error = %v", err)
	}

	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", presignedURL, err)
	}
	if got, want := parsedURL.Hostname(), "127.0.0.1"; got != want {
		t.Fatalf("presigned URL hostname = %q, want %q; url=%s", got, want, presignedURL)
	}
}

func TestDefaultStorageClientPresignedURLRejectsInheritedLoopbackOverrideForRemoteStorage(t *testing.T) {
	cfg := types.AppConfig{}
	cfg.Storage.WorkspaceStorage.DefaultEndpointUrl = "https://s3.amazonaws.com"
	cfg.Storage.WorkspaceStorage.DefaultPresignedEndpointUrl = "http://127.0.0.1:4566"
	cfg.Storage.WorkspaceStorage.DefaultAccessKey = "test"
	cfg.Storage.WorkspaceStorage.DefaultSecretKey = "test"
	cfg.Storage.WorkspaceStorage.DefaultRegion = "us-east-1"

	storageClient, err := NewDefaultStorageClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewDefaultStorageClient() error = %v", err)
	}

	presignedURL, err := storageClient.GeneratePresignedPutURL(context.Background(), "objects/object-id", 60, "workspace-prod-test")
	if err != nil {
		t.Fatalf("GeneratePresignedPutURL() error = %v", err)
	}

	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", presignedURL, err)
	}
	if got, want := parsedURL.Hostname(), "s3.amazonaws.com"; got != want {
		t.Fatalf("presigned URL hostname = %q, want %q; url=%s", got, want, presignedURL)
	}
}

func TestWorkspacePresignEndpointForDefaultStorage(t *testing.T) {
	cfg := types.WorkspaceStorageConfig{
		DefaultEndpointUrl:          "http://localstack:4566",
		DefaultPresignedEndpointUrl: "http://127.0.0.1:4566",
	}

	tests := []struct {
		name    string
		storage *types.WorkspaceStorage
		want    string
	}{
		{
			name:    "uses configured presign endpoint for default storage",
			storage: testWorkspaceStorage("http://localstack:4566"),
			want:    "http://127.0.0.1:4566",
		},
		{
			name:    "does not override external storage",
			storage: testWorkspaceStorage("https://s3.amazonaws.com"),
			want:    "",
		},
		{
			name:    "handles missing workspace storage",
			storage: nil,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WorkspacePresignEndpointForDefaultStorage(tt.storage, cfg); got != tt.want {
				t.Fatalf("WorkspacePresignEndpointForDefaultStorage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsBucketAlreadyCreatedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "already owned",
			err:  &smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"},
			want: true,
		},
		{
			name: "already exists",
			err:  &smithy.GenericAPIError{Code: "BucketAlreadyExists"},
			want: true,
		},
		{
			name: "other api error",
			err:  &smithy.GenericAPIError{Code: "AccessDenied"},
			want: false,
		},
		{
			name: "plain error",
			err:  context.Canceled,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBucketAlreadyCreatedError(tt.err); got != tt.want {
				t.Fatalf("isBucketAlreadyCreatedError() = %t, want %t", got, tt.want)
			}
		})
	}
}

func testWorkspaceStorage(endpoint string) *types.WorkspaceStorage {
	bucketName := "workspace-prod-test"
	accessKey := "test"
	secretKey := "test"
	region := "us-east-1"

	return &types.WorkspaceStorage{
		BucketName:  &bucketName,
		AccessKey:   &accessKey,
		SecretKey:   &secretKey,
		EndpointUrl: &endpoint,
		Region:      &region,
	}
}
