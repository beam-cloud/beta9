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
	BucketName     string
	Region         string
	EndpointURL    string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

func NewS3Client(ctx context.Context, sourceConfig S3SourceConfig, serverConfig ServerConfig) (*S3Client, error) {
	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(sourceConfig.Region),
	}
	if sourceConfig.AccessKey != "" && sourceConfig.SecretKey != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			sourceConfig.AccessKey,
			sourceConfig.SecretKey,
			"",
		)))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
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

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = sourceConfig.ForcePathStyle || sourceConfig.EndpointURL != ""
	})
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

func (c *S3Client) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Source.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (c *S3Client) ReadRange(ctx context.Context, key string, start int64, length int64) ([]byte, error) {
	if start < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid range start=%d length=%d", start, length)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, start+length-1)
	resp, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Source.BucketName),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, fmt.Errorf("range request failed for %s: %w", rangeHeader, err)
	}
	defer resp.Body.Close()

	if length > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("range too large: %d", length)
	}
	data := make([]byte, int(length))
	n, err := io.ReadFull(resp.Body, data)
	if err != nil {
		return nil, fmt.Errorf("error reading range %s: read %d/%d bytes: %w", rangeHeader, n, length, err)
	}
	if int64(n) != length {
		return nil, fmt.Errorf("short read for range %s: read %d/%d bytes", rangeHeader, n, length)
	}
	return data, nil
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
	workerCount := int(c.DownloadConcurrency)
	if workerCount <= 0 {
		workerCount = defaultDownloadConcurrency
	}
	if workerCount > numChunks {
		workerCount = numChunks
	}

	jobs := make(chan int, numChunks)
	chunkCh := make(chan chunkResult, numChunks)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		jobs <- i
	}
	close(jobs)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for index := range jobs {
				if ctx.Err() != nil {
					return
				}

				start := int64(index) * c.DownloadChunkSize
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
					chunkCh <- chunkResult{index: index, err: fmt.Errorf("range request failed for %s: %w", rangeHeader, err)}
					cancel()
					return
				}

				expected := end - start + 1
				part := make([]byte, expected)
				n, err := io.ReadFull(resp.Body, part)
				if closeErr := resp.Body.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
				if err != nil {
					chunkCh <- chunkResult{index: index, err: fmt.Errorf("error reading range %s: read %d/%d bytes: %w", rangeHeader, n, expected, err)}
					cancel()
					return
				}
				if int64(n) != expected {
					chunkCh <- chunkResult{index: index, err: fmt.Errorf("short read for range %s: read %d/%d bytes", rangeHeader, n, expected)}
					cancel()
					return
				}

				chunkCh <- chunkResult{index: index, data: part}
			}
		}()
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
		if _, err := buffer.Write(chunk); err != nil {
			return err
		}
	}
	if int64(buffer.Len()) != size {
		return fmt.Errorf("downloaded size mismatch: got %d bytes, expected %d", buffer.Len(), size)
	}
	return nil
}
