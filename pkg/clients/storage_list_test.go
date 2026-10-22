package clients

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

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
