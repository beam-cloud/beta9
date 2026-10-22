package clients

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/beam-cloud/beta9/pkg/types"
)

type storageRoundTripFunc func(*http.Request) (*http.Response, error)

func (f storageRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStorageDeleteWithPrefix(t *testing.T) {
	const quietSuccess = `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
	const objectFailure = `<Error><Key>volume/b</Key><Code>AccessDenied</Code><Message>object is locked</Message></Error>`
	for _, test := range []struct {
		name       string
		failedPage int
		status     int
		response   string
		wantError  []string
	}{
		{name: "quiet success across pages"},
		{name: "quiet object failure", failedPage: 1, response: `<DeleteResult>` + objectFailure + `</DeleteResult>`, wantError: []string{"bucket", "volume/b", "AccessDenied", "object is locked"}},
		{name: "mixed result", failedPage: 1, response: `<DeleteResult><Deleted><Key>volume/a</Key></Deleted>` + objectFailure + `</DeleteResult>`, wantError: []string{"volume/b", "AccessDenied"}},
		{name: "later page failure", failedPage: 2, response: `<DeleteResult><Error><Key>volume/c</Key><Code>InternalError</Code><Message>try again</Message></Error></DeleteResult>`, wantError: []string{"volume/c", "InternalError"}},
		{name: "request failure", failedPage: 1, status: http.StatusForbidden, response: `<Error><Code>AccessDenied</Code><Message>bucket is locked</Message></Error>`, wantError: []string{"AccessDenied", "bucket is locked"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pages := [][]string{{"volume/a", "volume/b"}, {"volume/c"}}
			lists, deletes := 0, 0
			transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/bucket" {
					t.Fatalf("request path = %q, want /bucket", r.URL.Path)
				}
				status, body := http.StatusOK, quietSuccess
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
					if lists >= len(pages) || r.URL.Query().Get("prefix") != "volume/" {
						t.Fatalf("unexpected list request: %s", r.URL)
					}
					wantToken := ""
					if lists > 0 {
						wantToken = "next"
					}
					if got := r.URL.Query().Get("continuation-token"); got != wantToken {
						t.Fatalf("continuation token = %q, want %q", got, wantToken)
					}
					body = `<ListBucketResult><IsTruncated>false</IsTruncated>`
					if lists == 0 {
						body = `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken>`
					}
					for _, key := range pages[lists] {
						body += "<Contents><Key>" + key + "</Key></Contents>"
					}
					body += "</ListBucketResult>"
					lists++
				case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
					var request struct {
						Quiet   bool     `xml:"Quiet"`
						Objects []string `xml:"Object>Key"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Fatal(err)
					}
					if deletes >= len(pages) || !request.Quiet || !reflect.DeepEqual(request.Objects, pages[deletes]) {
						t.Fatalf("delete request %d = %+v, want quiet deletion of listed keys", deletes, request)
					}
					deletes++
					if deletes == test.failedPage {
						body = test.response
						if test.status != 0 {
							status = test.status
						}
					}
				default:
					return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			cfg := aws.Config{
				Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""),
				HTTPClient: &http.Client{Transport: transport},
			}
			client := &StorageClient{s3Client: newS3Client(cfg, "https://storage.invalid")}
			deleted, err := client.DeleteWithPrefix(context.Background(), "volume/", "bucket")
			wantBatches := 2
			if len(test.wantError) > 0 {
				wantBatches = test.failedPage
				if err == nil || deleted != nil {
					t.Errorf("DeleteWithPrefix() = %v, %v; want nil and an error", deleted, err)
				}
				for _, fragment := range test.wantError {
					if err == nil || !strings.Contains(err.Error(), fragment) {
						t.Errorf("error = %v, want %q", err, fragment)
					}
				}
			} else if err != nil || !reflect.DeepEqual(deleted, []string{"a", "b", "c"}) {
				t.Errorf("DeleteWithPrefix() = %v, %v; want [a b c], nil", deleted, err)
			}
			if lists != wantBatches || deletes != wantBatches {
				t.Errorf("list/delete calls = %d/%d, want %d/%d", lists, deletes, wantBatches, wantBatches)
			}
		})
	}
}

func TestStorageResponseChecksums(t *testing.T) {
	checksums := map[string]string{"absent": "", "valid": "NhCmhg==", "corrupt": "AAAAAA=="} // CRC32 of "hello" is NhCmhg==.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Checksum-Mode") != "ENABLED" {
			t.Error("response checksum validation was not requested")
		}
		if checksum := checksums[strings.TrimPrefix(r.URL.Path, "/bucket/")]; checksum != "" {
			w.Header().Set("X-Amz-Checksum-Crc32", checksum)
		}
		_, _ = io.WriteString(w, "hello")
	}))
	defer server.Close()
	var logs bytes.Buffer
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access", "secret", "")),
		config.WithLogger(logging.NewStandardLogger(&logs)),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenSupported),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &StorageClient{s3Client: newS3Client(cfg, server.URL)}
	for _, name := range []string{"absent", "valid", "corrupt"} {
		t.Run(name, func(t *testing.T) {
			data, err := client.Download(context.Background(), name, "bucket")
			if name == "corrupt" {
				if err == nil || !strings.Contains(err.Error(), "checksum") {
					t.Fatalf("Download() error = %v, want checksum mismatch", err)
				}
			} else if err != nil || string(data) != "hello" {
				t.Fatalf("Download() = %q, %v", data, err)
			}
		})
	}
	if strings.Contains(logs.String(), "no supported checksum") {
		t.Fatalf("unexpected missing-checksum warning: %s", logs.String())
	}
}

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

	if uploader.PartSize != manager.MinUploadPartSize {
		t.Fatalf("uploader PartSize = %d, want %d", uploader.PartSize, manager.MinUploadPartSize)
	}
	if uploader.Concurrency != storageStreamingUploadWorkers {
		t.Fatalf("uploader Concurrency = %d, want %d", uploader.Concurrency, storageStreamingUploadWorkers)
	}
}

func TestStorageExistsUsesHead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("Exists() method = %q, want HEAD", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx := context.Background()
	storage := testWorkspaceStorage(server.URL)
	workspace, err := NewWorkspaceStorageClient(ctx, "workspace", storage)
	if err != nil {
		t.Fatalf("NewWorkspaceStorageClient() error = %v", err)
	}
	defaultClient, err := NewDefaultStorageClient(ctx, types.AppConfig{Storage: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
		DefaultRegion:      *storage.Region,
		DefaultAccessKey:   *storage.AccessKey,
		DefaultSecretKey:   *storage.SecretKey,
		DefaultEndpointUrl: server.URL,
	}}})
	if err != nil {
		t.Fatalf("NewDefaultStorageClient() error = %v", err)
	}
	for name, exists := range map[string]func() (bool, error){
		"workspace": func() (bool, error) { return workspace.Exists(ctx, "chunks/hash") },
		"default":   func() (bool, error) { return defaultClient.Exists(ctx, "chunks/hash", *storage.BucketName) },
	} {
		t.Run(name, func(t *testing.T) {
			ok, err := exists()
			if err != nil || !ok {
				t.Fatalf("Exists() = %t, %v; want true, nil", ok, err)
			}
		})
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

func TestWorkspaceStorageClientRejectsMissingCredentials(t *testing.T) {
	for _, storage := range []*types.WorkspaceStorage{nil, {}, {BucketName: aws.String("bucket")}} {
		client, err := NewWorkspaceStorageClient(context.Background(), "workspace", storage)
		if err == nil || client != nil {
			t.Fatal("expected missing workspace credentials to be rejected")
		}
	}
}
