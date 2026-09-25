package common

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// awsHTTPClient is shared by every S3 client this process builds. Credentials
// live in the per-client config, not here, so one pool can serve every
// workspace and registry. Several paths build an S3 client per request; with
// the SDK default each of those carried its own transport, so no connection
// outlived the request and every call paid a TLS handshake, while the shared
// registry client was capped at 10 idle connections per host and serialized
// bursts of parallel HEADs behind connection churn.
var awsHTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
	tr.MaxIdleConns = 1024
	tr.MaxIdleConnsPerHost = 256
})

// AWSHTTPClient returns the process-wide HTTP client for AWS SDK clients.
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
