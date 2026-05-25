package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type directUploadResult struct {
	OK             bool    `json:"ok"`
	Endpoint       string  `json:"endpoint,omitempty"`
	Region         string  `json:"region,omitempty"`
	Bucket         string  `json:"bucket,omitempty"`
	Key            string  `json:"key,omitempty"`
	Bytes          int64   `json:"bytes"`
	PartSize       int64   `json:"partSize"`
	Concurrency    int     `json:"concurrency"`
	CreateFileMs   float64 `json:"createFileMs"`
	UploadMs       float64 `json:"uploadMs"`
	HeadMs         float64 `json:"headMs"`
	DeleteMs       float64 `json:"deleteMs,omitempty"`
	UploadMBps     float64 `json:"uploadMBps"`
	HeadSizeOK     bool    `json:"headSizeOk"`
	CleanupRemote   bool    `json:"cleanupRemote"`
	CleanupRemoteOK bool    `json:"cleanupRemoteOk"`
	CleanupLocalOK  bool    `json:"cleanupLocalOk"`
	Error          string  `json:"error,omitempty"`
}

func main() {
	endpoint := flag.String("endpoint", "", "S3-compatible endpoint URL")
	region := flag.String("region", "us-east-1", "S3 region")
	bucket := flag.String("bucket", "", "bucket")
	key := flag.String("key", "", "object key")
	accessKey := flag.String("access-key", "", "access key")
	secretKey := flag.String("secret-key", "", "secret key")
	size := flag.Int64("size", 1024*1024*1024, "upload size in bytes")
	partSize := flag.Int64("part-size", 64*1024*1024, "multipart part size in bytes")
	concurrency := flag.Int("concurrency", 8, "multipart upload concurrency")
	cleanupRemote := flag.Bool("cleanup-remote", true, "delete uploaded object after HEAD proof")
	timeout := flag.Duration("timeout", 30*time.Minute, "probe timeout")
	flag.Parse()
	if *accessKey == "" {
		*accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if *secretKey == "" {
		*secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	started := time.Now()
	out := directUploadResult{
		Endpoint:      *endpoint,
		Region:        *region,
		Bucket:        *bucket,
		Key:           *key,
		Bytes:         *size,
		PartSize:      *partSize,
		Concurrency:   *concurrency,
		CleanupRemote: *cleanupRemote,
	}
	defer func() {
		_ = started
		_ = json.NewEncoder(os.Stdout).Encode(out)
	}()

	if *bucket == "" || *key == "" || *accessKey == "" || *secretKey == "" {
		out.Error = "bucket, key, access-key, and secret-key are required"
		return
	}
	if *size <= 0 || *partSize < 5*1024*1024 || *concurrency <= 0 {
		out.Error = "size must be positive, part-size must be >=5MiB, concurrency must be positive"
		return
	}

	tmp, err := os.CreateTemp("", "b9bench-direct-upload-*")
	if err != nil {
		out.Error = fmt.Sprintf("create temp file: %v", err)
		return
	}
	tmpPath := tmp.Name()
	defer func() {
		if err := os.Remove(tmpPath); err == nil {
			out.CleanupLocalOK = true
		}
	}()

	createStarted := time.Now()
	if err := tmp.Truncate(*size); err != nil {
		_ = tmp.Close()
		out.Error = fmt.Sprintf("truncate temp file: %v", err)
		return
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		_ = tmp.Close()
		out.Error = fmt.Sprintf("seek temp file: %v", err)
		return
	}
	out.CreateFileMs = float64(time.Since(createStarted).Nanoseconds()) / 1_000_000

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			*accessKey,
			*secretKey,
			"",
		)),
	)
	if err != nil {
		_ = tmp.Close()
		out.Error = fmt.Sprintf("load AWS config: %v", err)
		return
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if strings.TrimSpace(*endpoint) != "" {
			o.BaseEndpoint = aws.String(*endpoint)
			o.UsePathStyle = true
		}
	})

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = *partSize
		u.Concurrency = *concurrency
	})
	uploadStarted := time.Now()
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   tmp,
		Metadata: map[string]string{
			"b9bench-direct-upload": "true",
		},
	})
	out.UploadMs = float64(time.Since(uploadStarted).Nanoseconds()) / 1_000_000
	if out.UploadMs > 0 {
		out.UploadMBps = (float64(*size) / 1048576) / (out.UploadMs / 1000)
	}
	_ = tmp.Close()
	if err != nil {
		out.Error = fmt.Sprintf("upload object: %v", err)
		return
	}

	headStarted := time.Now()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	out.HeadMs = float64(time.Since(headStarted).Nanoseconds()) / 1_000_000
	if err != nil {
		out.Error = fmt.Sprintf("head object: %v", err)
		return
	}
	out.HeadSizeOK = aws.ToInt64(head.ContentLength) == *size

	if *cleanupRemote {
		deleteStarted := time.Now()
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: bucket,
			Key:    key,
		})
		out.DeleteMs = float64(time.Since(deleteStarted).Nanoseconds()) / 1_000_000
		out.CleanupRemoteOK = err == nil
		if err != nil {
			out.Error = fmt.Sprintf("delete object: %v", err)
			return
		}
	}
	out.OK = out.HeadSizeOK
}
