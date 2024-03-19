package worker

import (
	"context"
	b64 "encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	Region    string
	AccessKey string
	SecretKey string
}

func (p *AWSCredentialProvider) GetUsername() string {
	return ""
}

func (p *AWSCredentialProvider) GetAuthorizationToken() (string, error) {
	var cfg aws.Config
	var err error

	// Use static credentials if they are available
	if p.AccessKey != "" && p.SecretKey != "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(p.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(p.AccessKey, p.SecretKey, "")),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(p.Region),
		)
	}
	if err != nil {
		return "", err
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
