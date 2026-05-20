package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultDownloadConcurrency = 16
	defaultDownloadChunkSize   = 64 * 1024 * 1024 // 64MB
)

type S3Client struct {
	Client              *s3.Client
	Source              S3SourceConfig
	DownloadConcurrency int64
	DownloadChunkSize   int64
}

type S3SourceConfig struct {
	BucketName  string
	Region      string
	EndpointURL string
	AccessKey   string
	SecretKey   string
}

func NewS3Client(ctx context.Context, sourceConfig S3SourceConfig, serverConfig ServerConfig) (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(sourceConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			sourceConfig.AccessKey,
			sourceConfig.SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if sourceConfig.EndpointURL != "" {
		cfg.BaseEndpoint = aws.String(sourceConfig.EndpointURL)
	}

	if serverConfig.S3DownloadConcurrency <= 0 {
		serverConfig.S3DownloadConcurrency = defaultDownloadConcurrency
	}

	if serverConfig.S3DownloadChunkSize <= 0 {
		serverConfig.S3DownloadChunkSize = defaultDownloadChunkSize
	}

	s3Client := s3.NewFromConfig(cfg)
	return &S3Client{
		Client:              s3Client,
		Source:              sourceConfig,
		DownloadConcurrency: serverConfig.S3DownloadConcurrency,
		DownloadChunkSize:   serverConfig.S3DownloadChunkSize,
	}, nil
}

func (c *S3Client) GetClient() *s3.Client {
	return c.Client
}

func (c *S3Client) BucketName() string {
	return c.Source.BucketName
}

func (c *S3Client) Head(ctx context.Context, key string) (bool, *s3.HeadObjectOutput, error) {
	output, err := c.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.Source.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return false, nil, err
	}

	return true, output, nil
}

type chunkResult struct {
	index int
	data  []byte
	err   error
}

func (c *S3Client) DownloadIntoBuffer(ctx context.Context, key string, buffer *bytes.Buffer) error {
	ok, head, err := c.Head(ctx, key)
	if err != nil || !ok {
		return err
	}
	size := aws.ToInt64(head.ContentLength)
	if size <= 0 {
		return fmt.Errorf("invalid object size: %d", size)
	}

	numChunks := int((size + c.DownloadChunkSize - 1) / c.DownloadChunkSize)
	chunkCh := make(chan chunkResult, numChunks)
	sem := make(chan struct{}, c.DownloadConcurrency)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			start := int64(i) * c.DownloadChunkSize
			end := start + c.DownloadChunkSize - 1
			if end >= size {
				end = size - 1
			}

			rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
			resp, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(c.Source.BucketName),
				Key:    aws.String(key),
				Range:  &rangeHeader,
			})
			if err != nil {
				chunkCh <- chunkResult{i, nil, fmt.Errorf("range request failed for %s: %w", rangeHeader, err)}
				cancel()
				return
			}
			defer resp.Body.Close()

			part := make([]byte, end-start+1)
			n, err := io.ReadFull(resp.Body, part)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				chunkCh <- chunkResult{i, nil, fmt.Errorf("error reading range %s: %w", rangeHeader, err)}
				cancel()
				return
			}
			chunkCh <- chunkResult{i, part[:n], nil}
		}(i)
	}

	go func() {
		wg.Wait()
		close(chunkCh)
	}()

	chunks := make([][]byte, numChunks)
	var errs []error
	for res := range chunkCh {
		if res.err != nil {
			errs = append(errs, res.err)
		}
		chunks[res.index] = res.data
	}
	if len(errs) > 0 {
		return fmt.Errorf("download errors: %v", errs)
	}

	buffer.Reset()
	for _, chunk := range chunks {
		buffer.Write(chunk)
	}
	return nil
}
