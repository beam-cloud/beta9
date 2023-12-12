package worker

import (
	"context"
	b64 "encoding/base64"
	"fmt"
	"log"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	ecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/beam-cloud/beam/internal/common"
)

type CredentialProvider interface {
	GetUsername() string
	GetAuthorizationToken() (string, error)
	GetAuthString() (string, error)
}

// AWS auth provider
type AWSCredentialProvider struct {
}

func (p *AWSCredentialProvider) GetUsername() string {
	return ""
}

func (p *AWSCredentialProvider) GetAuthorizationToken() (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(common.Secrets().Get("AWS_REGION")))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
		return "", nil
	}

	svc := ecr.NewFromConfig(cfg)
	o, err := svc.GetAuthorizationToken(context.TODO(), &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return "", err
	}

	decodedToken, err := b64.StdEncoding.DecodeString(*o.AuthorizationData[0].AuthorizationToken)
	if err != nil {
		return "", err
	}

	return string(decodedToken), nil
}

func (p *AWSCredentialProvider) GetAuthString() (string, error) {
	token, err := p.GetAuthorizationToken()
	if err != nil {
		return "", err
	}
	return token, nil
}

// Docker provider
type DockerCredentialProvider struct {
}

func (p *DockerCredentialProvider) GetUsername() string {
	return common.Secrets().Get("DOCKER_USERNAME")
}

func (p *DockerCredentialProvider) GetAuthorizationToken() (string, error) {
	return common.Secrets().Get("DOCKER_PASSWORD"), nil
}

func (p *DockerCredentialProvider) GetAuthString() (string, error) {
	token, err := p.GetAuthorizationToken()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", p.GetUsername(), token), nil
}
