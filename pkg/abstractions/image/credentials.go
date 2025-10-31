package image

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	reg "github.com/beam-cloud/beta9/pkg/registry"
)

var (
	awsRegistryDomains  = []string{"amazonaws.com"}
	gcpRegistryDomains  = []string{"pkg.dev", "gcr.io"}
	ngcRegistryDomains  = []string{"nvcr.io"}
	ghcrRegistryDomains = []string{"ghcr.io"}
)

// GetRegistryCredentials returns structured credentials for the registry
// Returns both the registry name and credentials in a format compatible with CLIP
func GetRegistryCredentials(opts *BuildOpts) (registry string, creds map[string]string, err error) {
	registry = reg.ParseRegistry(opts.ExistingImageUri)
	if registry == "" {
		return "", nil, fmt.Errorf("failed to parse registry from image URI: %s", opts.ExistingImageUri)
	}

	var fn func(*BuildOpts) (map[string]string, error)

	switch {
	case containsAny(opts.ExistingImageUri, awsRegistryDomains...):
		fn = GetECRCredentials
	case containsAny(opts.ExistingImageUri, gcpRegistryDomains...):
		fn = GetGARCredentials
	case containsAny(opts.ExistingImageUri, ngcRegistryDomains...):
		fn = GetNGCCredentials
	case containsAny(opts.ExistingImageUri, ghcrRegistryDomains...):
		fn = GetGHCRCredentials
	default:
		fn = GetDockerHubCredentials
	}

	creds, err = fn(opts)
	if err != nil {
		return registry, nil, err
	}

	return registry, creds, nil
}

// GetRegistryToken returns credentials as username:password format (legacy)
// Deprecated: Use GetRegistryCredentials for structured credentials
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

// MarshalCredentialsToJSON converts credentials to JSON format for CLIP
func MarshalCredentialsToJSON(registry string, creds map[string]string) (string, error) {
	if len(creds) == 0 {
		return "", nil
	}

	credType := reg.DetectCredentialType(registry, creds)
	return reg.MarshalCredentials(registry, credType, creds)
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
	username, usernameExists := creds[usernameKey]
	password, passwordExists := creds[passwordKey]
	
	// If neither key exists, credentials were not provided (ok for public registries)
	if !usernameExists && !passwordExists {
		return "", "", false, nil
	}
	
	// If one or both keys exist but are empty, this is an error
	if (usernameExists && username == "") || (passwordExists && password == "") {
		return "", "", false, fmt.Errorf("both %s and %s must be non-empty if provided", usernameKey, passwordKey)
	}
	
	// If only one credential is provided (non-empty), both are required
	if (username == "" && password != "") || (username != "" && password == "") {
		return "", "", false, fmt.Errorf("both %s and %s must be provided together", usernameKey, passwordKey)
	}
	
	return username, password, username != "" && password != "", nil
}

// GetECRCredentials returns structured AWS ECR credentials
func GetECRCredentials(opts *BuildOpts) (map[string]string, error) {
	creds := opts.ExistingImageCreds
	
	// Validate required credentials
	accessKey, err := validateRequiredCredential(creds, "AWS_ACCESS_KEY_ID")
	if err != nil {
		return nil, err
	}
	secretKey, err := validateRequiredCredential(creds, "AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return nil, err
	}
	region, err := validateRequiredCredential(creds, "AWS_REGION")
	if err != nil {
		return nil, err
	}

	// Return credentials in structured format for CLIP
	result := map[string]string{
		"AWS_ACCESS_KEY_ID":     accessKey,
		"AWS_SECRET_ACCESS_KEY": secretKey,
		"AWS_REGION":            region,
	}

	if sessionToken := creds["AWS_SESSION_TOKEN"]; sessionToken != "" {
		result["AWS_SESSION_TOKEN"] = sessionToken
	}

	return result, nil
}

// Amazon Elastic Container Registry - retrieves ECR authorization token (legacy format)
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

// GetGARCredentials returns structured GCP GAR credentials
func GetGARCredentials(opts *BuildOpts) (map[string]string, error) {
	token, err := validateRequiredCredential(opts.ExistingImageCreds, "GCP_ACCESS_TOKEN")
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"GCP_ACCESS_TOKEN": token,
	}, nil
}

// GetNGCCredentials returns structured NGC credentials
func GetNGCCredentials(opts *BuildOpts) (map[string]string, error) {
	apiKey, err := validateRequiredCredential(opts.ExistingImageCreds, "NGC_API_KEY")
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"NGC_API_KEY": apiKey,
	}, nil
}

// GetGHCRCredentials returns structured GHCR credentials
func GetGHCRCredentials(opts *BuildOpts) (map[string]string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(opts.ExistingImageCreds, "GITHUB_USERNAME", "GITHUB_TOKEN")
	if err != nil {
		return nil, err
	}
	if hasAuth {
		return map[string]string{
			"GITHUB_USERNAME": username,
			"GITHUB_TOKEN":    password,
		}, nil
	}
	return nil, nil // Public access
}

// GetDockerHubCredentials returns structured Docker Hub credentials
func GetDockerHubCredentials(opts *BuildOpts) (map[string]string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(opts.ExistingImageCreds, "DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD")
	if err != nil {
		return nil, err
	}
	if hasAuth {
		return map[string]string{
			"DOCKERHUB_USERNAME": username,
			"DOCKERHUB_PASSWORD": password,
		}, nil
	}
	return nil, nil // Public access
}

// Google Artifact Registry - uses OAuth2 access token (legacy format)
func GetGARToken(opts *BuildOpts) (string, error) {
	token, err := validateRequiredCredential(opts.ExistingImageCreds, "GCP_ACCESS_TOKEN")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("oauth2accesstoken", token), nil
}

// NVIDIA GPU Cloud - uses API key (legacy format)
func GetNGCToken(opts *BuildOpts) (string, error) {
	apiKey, err := validateRequiredCredential(opts.ExistingImageCreds, "NGC_API_KEY")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("$oauthtoken", apiKey), nil
}

// GitHub Container Registry - optional username/token authentication (legacy format)
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

// Docker Hub - optional username/password authentication (legacy format)
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

// ParseCredentialsFromJSON parses credentials from JSON string or username:password format
// Returns structured credentials compatible with CLIP
func ParseCredentialsFromJSON(credStr string) (map[string]string, error) {
	if credStr == "" {
		return nil, nil
	}

	// Try JSON format first
	var credMap map[string]string
	if err := json.Unmarshal([]byte(credStr), &credMap); err == nil {
		return reg.ParseCredentialsFromEnv(credMap), nil
	}

	// Try legacy username:password format
	parts := strings.SplitN(credStr, ":", 2)
	if len(parts) == 2 {
		return map[string]string{
			"USERNAME": parts[0],
			"PASSWORD": parts[1],
		}, nil
	}

	return nil, fmt.Errorf("unable to parse credentials: invalid format")
}
