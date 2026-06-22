package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type result struct {
	OK             bool              `json:"ok"`
	Endpoint       string            `json:"endpoint,omitempty"`
	Region         string            `json:"region,omitempty"`
	Bucket         string            `json:"bucket,omitempty"`
	Key            string            `json:"key,omitempty"`
	Bytes          int64             `json:"bytes"`
	ExpectedBytes  int64             `json:"expectedBytes"`
	SizeOK         bool              `json:"sizeOK"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	MetadataHash   string            `json:"metadataHash,omitempty"`
	ExpectedSHA256 string            `json:"expectedSha256,omitempty"`
	HashOK         bool              `json:"hashOK"`
	FullRead       bool              `json:"fullRead"`
	FullReadSHA256 string            `json:"fullReadSha256,omitempty"`
	DurationMs     float64           `json:"durationMs"`
	Error          string            `json:"error,omitempty"`
}

func main() {
	endpoint := flag.String("endpoint", "", "S3-compatible endpoint URL")
	region := flag.String("region", "us-east-1", "S3 region")
	bucket := flag.String("bucket", "", "bucket")
	key := flag.String("key", "", "object key")
	accessKey := flag.String("access-key", "", "access key")
	secretKey := flag.String("secret-key", "", "secret key")
	expectedSize := flag.Int64("expected-size", -1, "expected object size")
	expectedSHA256 := flag.String("expected-sha256", "", "expected sha256 metadata")
	fullRead := flag.Bool("full-read", false, "download and hash the full object")
	timeout := flag.Duration("timeout", 5*time.Minute, "probe timeout")
	flag.Parse()

	started := time.Now()
	out := result{
		Endpoint:       *endpoint,
		Region:         *region,
		Bucket:         *bucket,
		Key:            *key,
		ExpectedBytes:  *expectedSize,
		ExpectedSHA256: *expectedSHA256,
		FullRead:       *fullRead,
	}
	defer func() {
		out.DurationMs = float64(time.Since(started).Nanoseconds()) / 1_000_000
		_ = json.NewEncoder(os.Stdout).Encode(out)
	}()

	if *bucket == "" || *key == "" || *accessKey == "" || *secretKey == "" {
		out.Error = "bucket, key, access-key, and secret-key are required"
		return
	}

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
		out.Error = fmt.Sprintf("load AWS config: %v", err)
		return
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if strings.TrimSpace(*endpoint) != "" {
			o.BaseEndpoint = aws.String(*endpoint)
			o.UsePathStyle = true
		}
	})

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		out.Error = fmt.Sprintf("head object: %v", err)
		return
	}
	out.Bytes = aws.ToInt64(head.ContentLength)
	out.SizeOK = *expectedSize < 0 || out.Bytes == *expectedSize
	out.Metadata = normalizeMetadata(head.Metadata)
	out.MetadataHash = firstMetadataHash(out.Metadata)
	out.HashOK = *expectedSHA256 == "" || out.MetadataHash == *expectedSHA256

	if *fullRead {
		body, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			out.Error = fmt.Sprintf("get object: %v", err)
			return
		}
		defer body.Body.Close()
		h := sha256.New()
		if _, err := io.Copy(h, body.Body); err != nil {
			out.Error = fmt.Sprintf("read object: %v", err)
			return
		}
		out.FullReadSHA256 = hex.EncodeToString(h.Sum(nil))
		out.HashOK = out.HashOK && (*expectedSHA256 == "" || out.FullReadSHA256 == *expectedSHA256)
	}

	out.OK = out.SizeOK && out.HashOK
}

func normalizeMetadata(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

func firstMetadataHash(metadata map[string]string) string {
	for _, key := range []string{
		"--content-sha256",
		"content-sha256",
		"sha256",
		"content_sha256",
		"x-amz-meta---content-sha256",
		"x-amz-meta-content-sha256",
	} {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			return value
		}
	}
	return ""
}
