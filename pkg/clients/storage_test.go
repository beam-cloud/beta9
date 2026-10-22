package clients

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

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

type storageListRoundTripFunc func(*http.Request) (*http.Response, error)

func (f storageListRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStorageListPagination(t *testing.T) {
	const fileA = `<Contents><Key>volume/a</Key><Size>9</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>`
	const fileB = `<Contents><Key>volume/b</Key><Size>9</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>`
	const marker = `<Contents><Key>volume/</Key></Contents>`
	const dirA = `<CommonPrefixes><Prefix>volume/dir-a/</Prefix></CommonPrefixes>`
	const dirB = `<CommonPrefixes><Prefix>volume/dir-b/</Prefix></CommonPrefixes>`
	for _, test := range []struct {
		name      string
		directory bool
		pages     []string
		want      []string
		fail      bool
		cancel    bool
	}{
		{name: "prefix pages", pages: []string{fileA, fileB}, want: []string{"volume/a", "volume/b"}},
		{name: "directory mixed pages", directory: true, pages: []string{marker + fileA + dirA, fileB + dirB}, want: []string{"volume/a", "volume/dir-a/", "volume/b", "volume/dir-b/"}},
		{name: "directory prefixes only first page", directory: true, pages: []string{dirA, marker + fileB}, want: []string{"volume/dir-a/", "volume/b"}},
		{name: "prefix later error", pages: []string{fileA, ""}, fail: true},
		{name: "directory later error", directory: true, pages: []string{dirA, ""}, fail: true},
		{name: "prefix cancellation", pages: []string{fileA, fileB}, cancel: true},
		{name: "directory cancellation", directory: true, pages: []string{dirA, fileB}, cancel: true},
		{name: "prefix single page", pages: []string{fileA}, want: []string{"volume/a"}},
		{name: "directory single page", directory: true, pages: []string{marker + fileA + dirA}, want: []string{"volume/a", "volume/dir-a/"}},
		{name: "prefix empty", pages: []string{""}},
		{name: "directory empty", directory: true, pages: []string{""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			requests := 0
			transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				page := requests
				requests++
				if page >= len(test.pages) {
					t.Fatalf("unexpected extra page: %s", r.URL)
				}
				wantToken, wantDelimiter := "", ""
				if page > 0 {
					wantToken = "next+/="
				}
				if test.directory {
					wantDelimiter = "/"
				}
				query := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/bucket" || query.Get("list-type") != "2" || query.Get("prefix") != "volume/" || query.Get("delimiter") != wantDelimiter || query.Get("continuation-token") != wantToken {
					t.Fatalf("unexpected list request: %s %s", r.Method, r.URL)
				}
				status := http.StatusOK
				body := "<ListBucketResult><IsTruncated>false</IsTruncated>"
				if page < len(test.pages)-1 {
					body = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next+/=</NextContinuationToken>"
				}
				body += test.pages[page] + "</ListBucketResult>"
				if test.fail && page == 1 {
					status, body = http.StatusForbidden, "<Error><Code>AccessDenied</Code><Message>page denied</Message></Error>"
				}
				if test.cancel {
					cancel()
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			cfg := aws.Config{
				Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""),
				HTTPClient: &http.Client{Transport: transport},
			}
			client := &StorageClient{s3Client: newS3Client(cfg, "https://storage.invalid")}
			list, prefix := client.ListWithPrefix, "volume/"
			if test.directory {
				list, prefix = client.ListDirectory, "volume"
			}
			objects, err := list(ctx, prefix, "bucket")
			if test.fail || test.cancel {
				if err == nil || objects != nil {
					t.Fatalf("listing = %v, %v; want nil objects and error", objects, err)
				}
				if test.cancel && !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want context cancellation", err)
				}
				if test.fail && !strings.Contains(err.Error(), "AccessDenied") {
					t.Fatalf("error = %v, want AccessDenied", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				var keys []string
				for _, object := range objects {
					keys = append(keys, aws.ToString(object.Key))
					wantSize := int64(9)
					if test.directory && strings.HasSuffix(aws.ToString(object.Key), "/") {
						wantSize = 0
					}
					if aws.ToInt64(object.Size) != wantSize || object.LastModified == nil {
						t.Fatalf("object metadata = %+v, want size %d and modification time", object, wantSize)
					}
				}
				if !reflect.DeepEqual(keys, test.want) {
					t.Fatalf("keys = %v, want %v", keys, test.want)
				}
			}
			wantRequests := len(test.pages)
			if test.cancel {
				wantRequests = 1
			}
			if requests != wantRequests {
				t.Fatalf("requests = %d, want %d", requests, wantRequests)
			}
		})
	}
}

func TestStorageTigrisConsistency(t *testing.T) {
	for _, endpoint := range []string{"https://t3.storage.dev", "https://fly.storage.tigris.dev", "https://s3.amazonaws.com"} {
		t.Run(endpoint, func(t *testing.T) {
			requests := 0
			transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				want := ""
				if endpoint != "https://s3.amazonaws.com" {
					want = "true"
				}
				if got := r.Header.Get(common.TigrisConsistencyHeader); got != want {
					t.Errorf("%s %s consistency = %q, want %q", r.Method, r.URL.Path, got, want)
				}
				if got := strings.Contains(r.Header.Get("Authorization"), "x-tigris-consistent"); got != (want != "") {
					t.Errorf("signed consistency header = %t, want %t", got, want != "")
				}
				body := ""
				switch {
				case r.URL.Query().Get("list-type") == "2":
					body = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
				case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
					body = "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>"
				case r.Method == http.MethodPost:
					body = "<CompleteMultipartUploadResult><ETag>etag</ETag></CompleteMultipartUploadResult>"
				case r.Method == http.MethodGet:
					body = "output"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"ETag": {"etag"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client := newS3Client(aws.Config{
				Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""),
				HTTPClient: &http.Client{Transport: transport},
			}, endpoint)
			ctx := context.Background()
			bucket, key := aws.String("bucket"), aws.String("output.json")
			_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("output")})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			get.Body.Close()
			_, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
			if err != nil {
				t.Fatal(err)
			}
			start, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: key, UploadId: start.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("part"))})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: key, UploadId: start.UploadId})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			if requests != 8 {
				t.Fatalf("requests = %d, want 8", requests)
			}
		})
	}
}

func TestTigrisPublishedOutputVisibility(t *testing.T) {
	const prefix = "outputs/stub/task/"
	const key = prefix + "output/prediction.json"
	published := false
	requests := 0
	transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		consistent := r.Header.Get(common.TigrisConsistencyHeader) == "true"
		body := ""
		switch r.Method {
		case http.MethodPut:
			// A successful regional write can be absent from another region's LIST.
			published = consistent
		case http.MethodGet:
			if r.URL.Query().Get("prefix") != prefix {
				t.Fatalf("unexpected prefix: %s", r.URL)
			}
			body = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
			if published && consistent {
				if r.URL.Query().Get("continuation-token") == "" {
					body = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>" + prefix + "</Key></Contents></ListBucketResult>"
				} else {
					body = "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>" + key + "</Key><Size>6</Size></Contents></ListBucketResult>"
				}
			}
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"ETag": {"etag"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	client := &StorageClient{s3Client: newS3Client(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""),
		HTTPClient: &http.Client{Transport: transport},
	}, "https://t3.storage.dev")}
	if err := client.UploadToBucketWithReader(context.Background(), key, strings.NewReader("output"), "bucket"); err != nil {
		t.Fatal(err)
	}
	objects, err := client.ListWithPrefix(context.Background(), prefix, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 || aws.ToString(objects[1].Key) != key || requests != 3 {
		t.Fatalf("published output = %v after %d requests; want directory marker then output", objects, requests)
	}
}

func TestTigrisPresignedHeaders(t *testing.T) {
	client, err := (storageClientConfig{region: "us-east-1", accessKey: "access", secretKey: "secret", endpointURL: "https://t3.storage.dev"}).newClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		var signedURL string
		if method == http.MethodGet {
			signedURL, err = client.GeneratePresignedGetURL(context.Background(), "output.json", 60, "bucket")
		} else {
			signedURL, err = client.GeneratePresignedPutURL(context.Background(), "object.zip", 60, "bucket")
		}
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(signedURL)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "x-tigris-consistent") {
			t.Fatalf("URL-only %s requires a consistency header", method)
		}
	}
	for name, metadata := range map[string]map[string]string{"full upload": {"hash": "content"}, "delta upload": nil} {
		t.Run(name, func(t *testing.T) {
			location, headers, err := client.GeneratePresignedPutURLWithMetadata(context.Background(), "object.zip", 60, "bucket", metadata)
			if err != nil {
				t.Fatal(err)
			}
			if headers[common.TigrisConsistencyHeader] != "true" {
				t.Fatalf("upload headers = %v", headers)
			}
			u, err := url.Parse(location)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "x-tigris-consistent") {
				t.Fatal("consistency header is not signed")
			}
		})
	}
}

func TestStorageArtifactReads(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		ordinary  bool
	}{
		{"output", "outputs/stub/task/output/file", true},
		{"chunk", "durable-disks/disk/chunks/hash", true},
		{"snapshot", "durable-disks/disk/snapshots/1/uuid/manifest.json", true},
		{"object", "objects/uuid", true},
		{"checkpoint", "checkpoints/uuid.tar", true},
		{"volume", "volumes/id/file", false},
		{"journal", "durable-disks/disk/journal/head.json", false},
	} {
		for _, status := range []int{200, 404, 403} {
			for _, operation := range []string{"head", "download", "stream"} {
				t.Run(fmt.Sprintf("%s/%d/%s", tc.name, status, operation), func(t *testing.T) {
					requests := 0
					transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						requests++
						consistent := r.Header.Get(common.TigrisConsistencyHeader) == "true"
						if consistent == tc.ordinary || strings.Contains(r.Header.Get("Authorization"), "x-tigris-consistent") != consistent {
							t.Error("unexpected consistency header or signature")
						}
						body := "verified bytes"
						if status != 200 {
							code := "NoSuchKey"
							if status == 403 {
								code = "AccessDenied"
							}
							body = "<Error><Code>" + code + "</Code></Error>"
						}
						return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
					})
					client := &StorageClient{s3Client: newS3Client(aws.Config{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""), HTTPClient: &http.Client{Transport: transport}}, "https://t3.storage.dev")}
					var err error
					if operation == "head" {
						var exists bool
						exists, _, err = client.Head(context.Background(), tc.key, "bucket")
						if exists != (status == 200) {
							t.Errorf("exists = %t", exists)
						}
					} else {
						var data []byte
						if operation == "download" {
							data, err = client.Download(context.Background(), tc.key, "bucket")
						} else {
							var reader io.ReadCloser
							reader, err = client.DownloadWithReader(context.Background(), tc.key, "bucket")
							if err == nil {
								data, err = io.ReadAll(reader)
								_ = reader.Close()
							}
						}
						if status == 200 && string(data) != "verified bytes" {
							t.Errorf("download = %q", data)
						}
					}
					wantError := status != 200 && !(operation == "head" && status == 404)
					if (err != nil) != wantError || requests != 1 {
						t.Fatalf("error = %v after %d requests; want error=%t, one request", err, requests, wantError)
					}
				})
			}
		}
	}
}

func TestStorageRegionalDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, region string
		pages, status          int
	}{
		{"pinned", "https://t3.storage.dev", "sjc", 2, 200},
		{"alternate", "https://t3.storage.dev", "fra", 1, 200},
		{"auto", "https://t3.storage.dev", "", 1, 200},
		{"other provider", "https://s3.amazonaws.com", "sjc", 1, 200},
		{"permission denied", "https://t3.storage.dev", "sjc", 1, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				wantRegion := tc.region
				if tc.endpoint == "https://s3.amazonaws.com" {
					wantRegion = ""
				}
				if r.Header.Get(common.TigrisRegionsHeader) != wantRegion || r.Header.Get(common.TigrisConsistencyHeader) != "" || strings.Contains(r.Header.Get("Authorization"), "x-tigris-consistent") {
					t.Errorf("unexpected listing headers or signature: region=%q", r.Header.Get(common.TigrisRegionsHeader))
				}
				if wantRegion != "" && !strings.Contains(r.Header.Get("Authorization"), "x-tigris-regions") {
					t.Error("region header is not signed")
				}
				if tc.status != 200 {
					return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<Error><Code>AccessDenied</Code></Error>`)), Request: r}, nil
				}
				token := ""
				if requests < tc.pages {
					token = "page2"
				} else if tc.pages == 2 && r.URL.Query().Get("continuation-token") != "page2" {
					t.Error("pagination token was not passed through")
				}
				body := fmt.Sprintf("<ListBucketResult><Contents><Key>outputs/%d</Key></Contents><IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken></ListBucketResult>", requests, token != "", token)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client := &StorageClient{s3Client: newS3Client(aws.Config{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""), HTTPClient: &http.Client{Transport: transport}}, tc.endpoint)}
			objects, err := client.ListWithPrefixInRegion(context.Background(), "outputs/", "bucket", tc.region)
			if (err != nil) != (tc.status != 200) || requests != tc.pages || (err == nil && len(objects) != tc.pages) {
				t.Fatalf("listing = %v, %v after %d requests", objects, err, requests)
			}
		})
	}
}

func TestStorageEmptyOutputListingsDoNotFanOut(t *testing.T) {
	requests := 0
	transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Header.Get(common.TigrisRegionsHeader) != "sjc" || r.Header.Get(common.TigrisConsistencyHeader) != "" {
			t.Error("empty output lookup did not use the single configured replica")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)), Request: r}, nil
	})
	client := &StorageClient{s3Client: newS3Client(aws.Config{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""), HTTPClient: &http.Client{Transport: transport}}, "https://t3.storage.dev")}
	for task := 0; task < 100; task++ {
		objects, err := client.ListWithPrefixInRegion(context.Background(), fmt.Sprintf("outputs/stub/task-%d/", task), "bucket", "sjc")
		if err != nil || len(objects) != 0 {
			t.Fatalf("empty output lookup = %v, %v", objects, err)
		}
	}
	if requests != 100 {
		t.Fatalf("100 empty tasks issued %d LISTs; want 100", requests)
	}
}

func TestStorageVersionReadDoesNotRecoverStaleMetadata(t *testing.T) {
	requests := 0
	transport := storageListRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Header.Get(common.TigrisConsistencyHeader) != "true" {
			t.Fatal("versioned read used an ordinary, potentially stale lookup")
		}
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<Error><Code>NoSuchKey</Code></Error>`)), Request: r}, nil
	})
	client := &WorkspaceStorageClient{
		WorkspaceStorage: &types.WorkspaceStorage{BucketName: aws.String("bucket")},
		StorageClient:    &StorageClient{s3Client: newS3Client(aws.Config{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""), HTTPClient: &http.Client{Transport: transport}}, "https://t3.storage.dev")},
	}
	data, version, err := client.ReadVersion(context.Background(), "lease")
	if err != nil || len(data) != 0 || version != "" || requests != 1 {
		t.Fatalf("missing version = %q, %q, %v after %d requests", data, version, err, requests)
	}
}
