package common

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestGetAWSConfigSharesOneHTTPClient(t *testing.T) {
	static, err := GetAWSConfig("key", "secret", "us-east-1", "")
	if err != nil {
		t.Fatal(err)
	}
	ambient, err := GetAWSConfig("", "", "us-east-1", "http://localhost:9000")
	if err != nil {
		t.Fatal(err)
	}

	shared := aws.HTTPClient(AWSHTTPClient())
	if static.HTTPClient != shared || ambient.HTTPClient != shared {
		t.Fatal("every AWS config must carry the process-wide HTTP client so connections are reused across S3 clients")
	}

	transport := AWSHTTPClient().GetTransport()
	if transport.MaxIdleConnsPerHost < 100 {
		t.Fatalf("idle connections per host is %d; the SDK default of 10 serializes bursts behind connection churn", transport.MaxIdleConnsPerHost)
	}
}
