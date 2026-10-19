package clients

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type storageDNSHTTPClient func(*http.Request) (*http.Response, error)

func (f storageDNSHTTPClient) Do(r *http.Request) (*http.Response, error) { return f(r) }

func newStorageDNSClient(send storageDNSHTTPClient, maxAttempts int) *StorageClient {
	cfg := aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""),
		HTTPClient: send,
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		},
	}
	return &StorageClient{s3Client: newS3Client(cfg, "https://storage.example")}
}

func storageLookupFailure() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
		Err: "no such host", Name: "storage.example", Server: "127.0.0.53:53", IsNotFound: true,
	}}
}

func TestStorageUploadsRecoverDNSFailureUnderBurst(t *testing.T) {
	var attempts sync.Map
	client := newStorageDNSClient(func(r *http.Request) (*http.Response, error) {
		value, _ := attempts.LoadOrStore(r.URL.Path, new(atomic.Int32))
		if value.(*atomic.Int32).Add(1) == 1 {
			return nil, storageLookupFailure()
		}
		response := ""
		if r.Method == http.MethodPut {
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "snapshot" {
				return nil, fmt.Errorf("replayed upload body = %q, error = %v", body, err)
			}
		} else {
			response = "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": {"etag"}}, Body: io.NopCloser(strings.NewReader(response))}, nil
	}, 3)
	var passed atomic.Int32
	var group sync.WaitGroup
	for i := range 100 {
		group.Go(func() {
			ctx, bucket, key := context.Background(), aws.String("bucket"), aws.String(fmt.Sprint(i))
			var err error
			switch i % 3 {
			case 0:
				err = client.UploadToBucket(ctx, *key, []byte("snapshot"), *bucket)
			case 1:
				_, err = client.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
			case 2:
				_, err = client.s3Client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: bucket, Key: key, UploadId: aws.String("upload"), PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("snapshot")),
				})
			}
			if err == nil {
				passed.Add(1)
			}
		})
	}
	group.Wait()
	if got := passed.Load(); got != 100 {
		t.Fatalf("recovered %d/100 uploads after one DNS lookup failure per object", got)
	}
	attempts.Range(func(key, value any) bool {
		if got := value.(*atomic.Int32).Load(); got != 2 {
			t.Errorf("%s: attempts = %d, want 2", key, got)
		}
		return true
	})
}

func TestStorageDNSFailureRespectsAttemptLimit(t *testing.T) {
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			var attempts int
			client := newStorageDNSClient(func(*http.Request) (*http.Response, error) {
				attempts++
				return nil, storageLookupFailure()
			}, limit)
			err := client.UploadToBucket(context.Background(), "snapshot", nil, "bucket")
			var dnsError *net.DNSError
			if !errors.As(err, &dnsError) || attempts != limit {
				t.Fatalf("attempts = %d, want %d; error = %v", attempts, limit, err)
			}
		})
	}
}

func TestStorageDNSRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts int
	client := newStorageDNSClient(func(*http.Request) (*http.Response, error) {
		attempts++
		cancel()
		return nil, storageLookupFailure()
	}, 3)
	err := client.UploadToBucket(ctx, "snapshot", nil, "bucket")
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("attempts = %d, want 1; error = %v", attempts, err)
	}
}

func TestStorageDNSRetryPreservesPermanentErrors(t *testing.T) {
	var attempts int
	client := newStorageDNSClient(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("<Error><Code>AccessDenied</Code></Error>"))}, nil
	}, 3)
	err := client.UploadToBucket(context.Background(), "snapshot", nil, "bucket")
	var apiError smithy.APIError
	if !errors.As(err, &apiError) || apiError.ErrorCode() != "AccessDenied" || attempts != 1 {
		t.Fatalf("attempts = %d, want 1; error = %v", attempts, err)
	}
	retryer := client.s3Client.Options().Retryer
	if retryer.IsErrorRetryable(&smithy.CanceledError{Err: storageLookupFailure()}) {
		t.Fatal("a canceled request must not retry even when it wraps a DNS failure")
	}
}

type storageAttemptRetryer struct {
	aws.NopRetryer
	err error
}

func (r storageAttemptRetryer) GetAttemptToken(context.Context) (func(error) error, error) {
	return nil, r.err
}

func TestStorageDNSRetryPreservesAttemptTokens(t *testing.T) {
	want := errors.New("attempt rate limited")
	retryer := storageDNSRetryer{storageAttemptRetryer{err: want}}
	if _, err := retryer.GetAttemptToken(context.Background()); !errors.Is(err, want) {
		t.Fatalf("attempt token error = %v, want %v", err, want)
	}
}
