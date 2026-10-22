package clients

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/pkg/common"
)

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
	location, headers, err := client.GeneratePresignedPutURLWithMetadata(context.Background(), "object.zip", 60, "bucket", map[string]string{"hash": "content"})
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
}
