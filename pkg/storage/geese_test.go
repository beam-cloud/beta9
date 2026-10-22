package storage

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	core "github.com/yandex-cloud/geesefs/core"
	cfg "github.com/yandex-cloud/geesefs/core/cfg"
)

type geeseReadRoundTripper func(*http.Request) (*http.Response, error)

func (f geeseReadRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestGeeseTigrisObjectReads(t *testing.T) {
	for _, tc := range []struct {
		name, key  string
		status     int
		consistent bool
	}{
		{"output", "outputs/stub/task/id/file", 200, false},
		{"disk chunk", "durable-disks/disk/chunks/digest", 200, false},
		{"volume", "volumes/id/file", 200, true},
		{"disk lease", "durable-disks/disk/lease.json", 200, true},
		{"missing output", "outputs/stub/task/id/file", 404, false},
		{"deleted volume", "volumes/id/file", 404, true},
		{"permission denied", "outputs/stub/task/id/file", 403, false},
	} {
		for _, method := range []string{"GET", "HEAD"} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				config, err := geeseS3Config(types.GeeseConfig{EndpointUrl: "https://t3.storage.dev", Region: "auto", AccessKey: "access", SecretKey: "secret"})
				if err != nil {
					t.Fatal(err)
				}
				requests := 0
				config.Session.Config.HTTPClient = &http.Client{Transport: geeseReadRoundTripper(func(r *http.Request) (*http.Response, error) {
					requests++
					if got := r.Header.Get(common.TigrisConsistencyHeader) == "true"; got != tc.consistent {
						t.Errorf("consistent request = %t, want %t", got, tc.consistent)
					}
					if got := strings.Contains(r.Header.Get("Authorization"), "x-tigris-consistent"); got != tc.consistent {
						t.Errorf("signed consistency header = %t, want %t", got, tc.consistent)
					}
					body := "artifact"
					if tc.status == 404 {
						body = `<Error><Code>NoSuchKey</Code></Error>`
					} else if tc.status == 403 {
						body = `<Error><Code>AccessDenied</Code></Error>`
					}
					return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				})}
				flags := cfg.DefaultFlags()
				flags.Endpoint = "https://t3.storage.dev"
				backend, err := core.NewS3("bucket", flags, config)
				if err != nil {
					t.Fatal(err)
				}
				backend.S3.Config.HTTPClient = config.Session.Config.HTTPClient
				if method == "HEAD" {
					_, err = backend.HeadObject(&s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String(tc.key)})
				} else {
					var result *s3.GetObjectOutput
					result, err = backend.GetObject(&s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String(tc.key)})
					if err == nil {
						data, readErr := io.ReadAll(result.Body)
						_ = result.Body.Close()
						if readErr != nil || string(data) != "artifact" {
							t.Fatalf("read = %q, %v", data, readErr)
						}
					}
				}
				if (err == nil) != (tc.status == http.StatusOK) {
					t.Fatalf("read error = %v", err)
				}
				if requests != 1 {
					t.Fatalf("requests = %d, want 1", requests)
				}
			})
		}
	}
}

type testVolumeReporter struct {
	workspaceID string
	hash        string
	sourcePath  string
	sizeBytes   int64
}

func (r *testVolumeReporter) ReportVolumeContent(workspaceID, hash, sourcePath string, sizeBytes int64) {
	r.workspaceID = workspaceID
	r.hash = hash
	r.sourcePath = sourcePath
	r.sizeBytes = sizeBytes
}

func TestEffectiveGeeseMemoryLimitMB(t *testing.T) {
	tests := []struct {
		name       string
		configured int64
		worker     string
		want       int64
	}{
		{name: "keeps configured under worker cap", configured: 256, worker: "1024", want: 256},
		{name: "caps configured above worker cap", configured: 8000, worker: "1024", want: 512},
		{name: "uses worker cap when configured unset", configured: 0, worker: "2Gi", want: 1024},
		{name: "keeps configured without worker limit", configured: 2048, worker: "", want: 2048},
		{name: "handles mib suffix", configured: 2048, worker: "1024Mi", want: 512},
		{name: "preserves minimum for tiny limits", configured: 2048, worker: "128", want: 128},
		{name: "does not exceed very small worker limit", configured: 2048, worker: "64", want: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveGeeseMemoryLimitMB(tt.configured, tt.worker)
			if got != tt.want {
				t.Fatalf("effectiveGeeseMemoryLimitMB(%d, %q) = %d, want %d", tt.configured, tt.worker, got, tt.want)
			}
		})
	}
}

func TestHandleGeeseContentEventReportsStoredContent(t *testing.T) {
	reporter := &testVolumeReporter{}
	storage := &GeeseStorage{}
	storage.SetVolumeContentReporter("workspace-id", reporter)
	const modelSizeBytes = int64(1536) << 30

	storage.handleGeeseContentEvent(map[string]interface{}{
		"content_hash": "hash",
		"inode":        "/volumes/workspace/files/data.bin",
		"size_bytes":   uint64(modelSizeBytes),
	})

	if reporter.workspaceID != "workspace-id" {
		t.Fatalf("workspaceID = %q", reporter.workspaceID)
	}
	if reporter.hash != "hash" {
		t.Fatalf("hash = %q", reporter.hash)
	}
	if reporter.sourcePath != "/volumes/workspace/files/data.bin" {
		t.Fatalf("sourcePath = %q", reporter.sourcePath)
	}
	if reporter.sizeBytes != modelSizeBytes {
		t.Fatalf("sizeBytes = %d", reporter.sizeBytes)
	}
}

func TestGeeseTigrisConsistency(t *testing.T) {
	for _, endpoint := range []string{"https://t3.storage.dev", "https://fly.storage.tigris.dev", "https://s3.amazonaws.com"} {
		t.Run(endpoint, func(t *testing.T) {
			config, err := geeseS3Config(types.GeeseConfig{
				EndpointUrl: endpoint, Region: "us-east-1", AccessKey: "access", SecretKey: "secret",
			})
			if err != nil {
				t.Fatal(err)
			}
			flags := cfg.DefaultFlags()
			flags.Endpoint = endpoint
			backend, err := core.NewS3("bucket", flags, config)
			if err != nil {
				t.Fatal(err)
			}
			bucket, key := aws.String("bucket"), aws.String("outputs/stub/task/id/output.json")
			put, _ := backend.PutObjectRequest(&s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("output")})
			head, _ := backend.HeadObjectRequest(&s3.HeadObjectInput{Bucket: bucket, Key: key})
			get, _ := backend.GetObjectRequest(&s3.GetObjectInput{Bucket: bucket, Key: key})
			list, _ := backend.ListObjectsRequest(&s3.ListObjectsInput{Bucket: bucket})
			listV2, _ := backend.ListObjectsV2Request(&s3.ListObjectsV2Input{Bucket: bucket})
			start, _ := backend.CreateMultipartUploadRequest(&s3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
			part, _ := backend.UploadPartRequest(&s3.UploadPartInput{Bucket: bucket, Key: key, UploadId: aws.String("upload"), PartNumber: aws.Int64(1), Body: strings.NewReader("part")})
			commit, _ := backend.CompleteMultipartUploadRequest(&s3.CompleteMultipartUploadInput{Bucket: bucket, Key: key, UploadId: aws.String("upload")})
			copy, _ := backend.CopyObjectRequest(&s3.CopyObjectInput{Bucket: bucket, Key: key, CopySource: aws.String("bucket/source")})
			remove, _ := backend.DeleteObjectRequest(&s3.DeleteObjectInput{Bucket: bucket, Key: key})
			for _, r := range []*request.Request{put, head, get, list, listV2, start, part, commit, copy, remove} {
				// Existing defaults must be removed before signing artifact reads.
				if endpoint != "https://s3.amazonaws.com" {
					r.HTTPRequest.Header.Set(common.TigrisConsistencyHeader, "true")
				}
				if err := r.Sign(); err != nil {
					t.Fatalf("%s: %v", r.Operation.Name, err)
				}
				want := ""
				if endpoint != "https://s3.amazonaws.com" && r.Operation.Name != "GetObject" && r.Operation.Name != "HeadObject" {
					want = "true"
				}
				if got := r.HTTPRequest.Header.Get(common.TigrisConsistencyHeader); got != want {
					t.Errorf("%s consistency = %q, want %q", r.Operation.Name, got, want)
				}
				if got := strings.Contains(r.HTTPRequest.Header.Get("Authorization"), "x-tigris-consistent"); got != (want != "") {
					t.Errorf("%s signed consistency header = %t", r.Operation.Name, got)
				}
			}
		})
	}
}
