package cache

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (source S3ContentSource) open(ctx context.Context) (io.ReadCloser, error) {
	loadOptions := []func(*config.LoadOptions) error{config.WithRegion(source.Region)}
	if source.AccessKey != "" && source.SecretKey != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(source.AccessKey, source.SecretKey, ""),
		))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	if source.EndpointURL != "" {
		cfg.BaseEndpoint = aws.String(source.EndpointURL)
	}
	client := s3.NewFromConfig(cfg, func(opts *s3.Options) {
		opts.UsePathStyle = source.ForcePathStyle
		opts.DisableLogOutputChecksumValidationSkipped = true
	})
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(source.BucketName),
		Key:    aws.String(source.Path),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
