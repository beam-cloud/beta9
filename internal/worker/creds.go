package worker

import (
	"context"
	b64 "encoding/base64"
	"fmt"
	"log"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	ecr "github.com/aws/aws-sdk-go-v2/service/ecr"
)

type CredentialProvider interface {
	GetUsername() string
	GetAuthorizationToken() (string, error)
	GetAuthString() (string, error)
}

// AWS auth provider
type AWSCredentialProvider struct {
	CredentialProvider
	Region string
}

func (p *AWSCredentialProvider) GetUsername() string {
	return ""
}

func (p *AWSCredentialProvider) GetAuthorizationToken() (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(p.Region))
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
		return "", fmt.Errorf("failed to get aws provider auth string: %v", err)
	}
	return token, nil
}

// Docker provider
type DockerCredentialProvider struct {
	CredentialProvider
	Username string
	Password string
}

func (p *DockerCredentialProvider) GetUsername() string {
	return p.Username
}

func (p *DockerCredentialProvider) GetAuthorizationToken() (string, error) {
	return p.Password, nil
}

func (p *DockerCredentialProvider) GetAuthString() (string, error) {
	token, err := p.GetAuthorizationToken()
	if err != nil {
		return "", fmt.Errorf("failed to get docker provider auth string: %v", err)
	}
	return fmt.Sprintf("%s:%s", p.GetUsername(), token), nil
}
