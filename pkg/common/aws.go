package common

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// One connection pool for every S3 client in the process; credentials stay per client.
// Per-request clients otherwise get their own transport and the SDK caps idle conns at 10.
var awsHTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
	tr.MaxIdleConns = 1024
	tr.MaxIdleConnsPerHost = 256
})

func AWSHTTPClient() *awshttp.BuildableClient {
	return awsHTTPClient
}

func GetAWSConfig(accessKey string, secretKey string, region string, endpoint string) (aws.Config, error) {
	var cfg aws.Config
	var err error
	opts := []func(*config.LoadOptions) error{config.WithHTTPClient(awsHTTPClient)}

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
