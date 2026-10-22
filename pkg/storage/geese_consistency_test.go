package storage

import (
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
			bucket, key := aws.String("bucket"), aws.String("output.json")
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
				if err := r.Sign(); err != nil {
					t.Fatalf("%s: %v", r.Operation.Name, err)
				}
				want := ""
				if endpoint != "https://s3.amazonaws.com" {
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
