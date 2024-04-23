package common

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func GetAWSConfig(accessKey string, secretKey string, region string, dualStackModeEnabled bool) (aws.Config, error) {
	var cfg aws.Config
	var err error
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Enabling dual-stack mode if the flag is true
	if dualStackModeEnabled {
		opts = append(opts, config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled))
	}

	if accessKey == "" || secretKey == "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(), opts...)
	} else {
		credentials := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		cfg, err = config.LoadDefaultConfig(context.TODO(), append(opts, config.WithCredentialsProvider(credentials))...)
	}

	return cfg, err
}
