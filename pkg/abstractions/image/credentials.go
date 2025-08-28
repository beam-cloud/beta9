package image

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

var (
	awsRegistryDomains  = []string{"amazonaws.com"}
	gcpRegistryDomains  = []string{"pkg.dev", "gcr.io"}
	ngcRegistryDomains  = []string{"nvcr.io"}
	ghcrRegistryDomains = []string{"ghcr.io"}
)

// Check which registry is passed in
func GetRegistryToken(opts *BuildOpts) (string, error) {
	var fn func(*BuildOpts) (string, error)

	switch {
	case containsAny(opts.ExistingImageUri, awsRegistryDomains...):
		fn = GetECRToken
	case containsAny(opts.ExistingImageUri, gcpRegistryDomains...):
		fn = GetGARToken
	case containsAny(opts.ExistingImageUri, ngcRegistryDomains...):
		fn = GetNGCToken
	case containsAny(opts.ExistingImageUri, ghcrRegistryDomains...):
		fn = GetGHCRToken
	default:
		fn = GetDockerHubToken
	}

	return fn(opts)
}

func containsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// Amazon Elastic Container Registry
// Gets the ECR Authorization Token on behalf of the user.
func GetECRToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	accessKey, ok := creds["AWS_ACCESS_KEY_ID"]
	if !ok {
		return "", fmt.Errorf("AWS_ACCESS_KEY_ID not found")
	}
	secretKey, ok := creds["AWS_SECRET_ACCESS_KEY"]
	if !ok {
		return "", fmt.Errorf("AWS_SECRET_ACCESS_KEY not found")
	}
	sessionToken, ok := creds["AWS_SESSION_TOKEN"]
	if !ok {
		sessionToken = ""
	}
	region, ok := creds["AWS_REGION"]
	if !ok {
		return "", fmt.Errorf("AWS_REGION not found")
	}

	credentials := credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithCredentialsProvider(credentials))
	if err != nil {
		return "", fmt.Errorf("error loading AWS configuration: %v", err)
	}

	// Create an ECR client with the custom configuration
	client := ecr.NewFromConfig(cfg)

	// Get authorization token
	output, err := client.GetAuthorizationToken(context.TODO(), &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return "", fmt.Errorf("error getting ECR authorization token: %v", err)
	}

	if len(output.AuthorizationData) == 0 || output.AuthorizationData[0].AuthorizationToken == nil {
		return "", fmt.Errorf("no authorization data returned")
	}

	// Decode the authorization token to get the username and password
	base64Token := aws.ToString(output.AuthorizationData[0].AuthorizationToken)
	decodedToken, err := base64.StdEncoding.DecodeString(base64Token)

	if err != nil {
		return "", fmt.Errorf("error decoding token: %v", err)
	}

	parts := strings.SplitN(string(decodedToken), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("decoded token format invalid, expected username:password")
	}

	// Return the username and password
	username := parts[0]
	password := parts[1]

	token := fmt.Sprintf("%s:%s", username, password)
	return token, nil
}

// Google Artifact Registry
func GetGARToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	password, ok := creds["GCP_ACCESS_TOKEN"]
	if !ok {
		return "", fmt.Errorf("GCP_ACCESS_TOKEN not found")
	}
	if password == "" {
		return "", fmt.Errorf("GCP_ACCESS_TOKEN is empty")
	}

	username := "oauth2accesstoken"

	return fmt.Sprintf("%s:%s", username, password), nil
}

func GetNGCToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	password, ok := creds["NGC_API_KEY"]
	if !ok {
		return "", fmt.Errorf("NGC_API_KEY not found")
	}
	if password == "" {
		return "", fmt.Errorf("NGC_API_KEY is empty")
	}

	username := "$oauthtoken"

	token := fmt.Sprintf("%s:%s", username, password)
	return token, nil
}

// GitHub Container Registry
func GetGHCRToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	username, hasUsername := creds["GITHUB_USERNAME"]
	password, hasPassword := creds["GITHUB_TOKEN"]

	// Check if username and password are set
	if hasUsername && username == "" {
		return "", fmt.Errorf("GITHUB_USERNAME set but is empty")
	}
	if hasPassword && password == "" {
		return "", fmt.Errorf("GITHUB_TOKEN set but is empty")
	}

	// If either username or password is missing, assume no credentials are needed
	if !hasUsername || !hasPassword {
		return "", nil
	}

	token := fmt.Sprintf("%s:%s", username, password)
	return token, nil
}

// Docker Hub
func GetDockerHubToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds

	username, hasUsername := creds["DOCKERHUB_USERNAME"]
	password, hasPassword := creds["DOCKERHUB_PASSWORD"]

	// Check if username and password are set
	if hasUsername && username == "" {
		return "", fmt.Errorf("DOCKERHUB_USERNAME set but is empty")
	}
	if hasPassword && password == "" {
		return "", fmt.Errorf("DOCKERHUB_PASSWORD set but is empty")
	}

	// If either username or password is missing, assume no credentials are needed
	if !hasUsername || !hasPassword {
		return "", nil
	}

	token := fmt.Sprintf("%s:%s", username, password)
	return token, nil
}
