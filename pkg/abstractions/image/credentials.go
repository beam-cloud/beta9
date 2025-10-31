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

func buildBasicAuthToken(username, password string) string {
	return fmt.Sprintf("%s:%s", username, password)
}

func validateRequiredCredential(creds map[string]string, key string) (string, error) {
	if value := creds[key]; value != "" {
		return value, nil
	}
	return "", fmt.Errorf("%s not found or empty", key)
}

func validateOptionalCredentials(creds map[string]string, usernameKey, passwordKey string) (string, string, bool, error) {
	username := creds[usernameKey]
	password := creds[passwordKey]
	
	if (username == "" && password != "") || (username != "" && password == "") {
		return "", "", false, fmt.Errorf("both %s and %s must be provided together", usernameKey, passwordKey)
	}
	
	return username, password, username != "" && password != "", nil
}

// Amazon Elastic Container Registry - retrieves ECR authorization token
func GetECRToken(opts *BuildOpts) (string, error) {
	creds := opts.ExistingImageCreds
	
	// Validate required credentials
	accessKey, err := validateRequiredCredential(creds, "AWS_ACCESS_KEY_ID")
	if err != nil {
		return "", err
	}
	secretKey, err := validateRequiredCredential(creds, "AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return "", err
	}
	region, err := validateRequiredCredential(creds, "AWS_REGION")
	if err != nil {
		return "", err
	}
	sessionToken := creds["AWS_SESSION_TOKEN"] // Optional

	// Configure AWS client
	credProvider := credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
	cfg, err := config.LoadDefaultConfig(context.TODO(), 
		config.WithRegion(region), 
		config.WithCredentialsProvider(credProvider))
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Get ECR authorization token
	client := ecr.NewFromConfig(cfg)
	output, err := client.GetAuthorizationToken(context.TODO(), &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return "", fmt.Errorf("failed to get ECR token: %w", err)
	}

	if len(output.AuthorizationData) == 0 || output.AuthorizationData[0].AuthorizationToken == nil {
		return "", fmt.Errorf("no authorization data returned from ECR")
	}

	// Decode base64 token (format: username:password)
	base64Token := aws.ToString(output.AuthorizationData[0].AuthorizationToken)
	decodedToken, err := base64.StdEncoding.DecodeString(base64Token)
	if err != nil {
		return "", fmt.Errorf("failed to decode ECR token: %w", err)
	}

	parts := strings.SplitN(string(decodedToken), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid ECR token format")
	}

	return buildBasicAuthToken(parts[0], parts[1]), nil
}

// Google Artifact Registry - uses OAuth2 access token
func GetGARToken(opts *BuildOpts) (string, error) {
	token, err := validateRequiredCredential(opts.ExistingImageCreds, "GCP_ACCESS_TOKEN")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("oauth2accesstoken", token), nil
}

// NVIDIA GPU Cloud - uses API key
func GetNGCToken(opts *BuildOpts) (string, error) {
	apiKey, err := validateRequiredCredential(opts.ExistingImageCreds, "NGC_API_KEY")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("$oauthtoken", apiKey), nil
}

// GitHub Container Registry - optional username/token authentication
func GetGHCRToken(opts *BuildOpts) (string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(opts.ExistingImageCreds, "GITHUB_USERNAME", "GITHUB_TOKEN")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	return "", nil // Public access
}

// Docker Hub - optional username/password authentication
func GetDockerHubToken(opts *BuildOpts) (string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(opts.ExistingImageCreds, "DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	return "", nil // Public access
}
