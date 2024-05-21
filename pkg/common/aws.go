package common

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func GetAWSConfig(accessKey string, secretKey string, region string, endpoint string) (aws.Config, error) {
	var cfg aws.Config
	var err error
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	if endpoint != "" {
		endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: endpoint,
			}, nil
		})
		opts = append(opts, config.WithEndpointResolverWithOptions(endpointResolver))
	}

	if accessKey == "" || secretKey == "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(), opts...)
	} else {
		credentials := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		opts = append(opts, config.WithCredentialsProvider(credentials))
		cfg, err = config.LoadDefaultConfig(context.TODO(), opts...)
	}

	return cfg, err
}
