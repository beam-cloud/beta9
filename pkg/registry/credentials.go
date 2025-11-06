package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/beam-cloud/clip/pkg/common"
	"github.com/google/go-containerregistry/pkg/authn"
)

// CredType represents the type of registry credentials
type CredType string

const (
	CredTypePublic  CredType = "public"
	CredTypeBasic   CredType = "basic"
	CredTypeAWS     CredType = "aws"
	CredTypeGCP     CredType = "gcp"
	CredTypeAzure   CredType = "azure"
	CredTypeToken   CredType = "token"
	CredTypeUnknown CredType = "unknown"
)

// Known credential environment variable keys
var knownCredKeys = map[string]bool{
	"USERNAME":                       true,
	"PASSWORD":                       true,
	"AWS_ACCESS_KEY_ID":              true,
	"AWS_SECRET_ACCESS_KEY":          true,
	"AWS_SESSION_TOKEN":              true,
	"AWS_REGION":                     true,
	"GOOGLE_APPLICATION_CREDENTIALS": true,
	"GCP_PROJECT_ID":                 true,
	"GCP_ACCESS_TOKEN":               true,
	"AZURE_CLIENT_ID":                true,
	"AZURE_CLIENT_SECRET":            true,
	"AZURE_TENANT_ID":                true,
	"DOCKER_USERNAME":                true,
	"DOCKER_PASSWORD":                true,
	"REGISTRY_USERNAME":              true,
	"REGISTRY_PASSWORD":              true,
	"NGC_API_KEY":                    true,
	"GITHUB_TOKEN":                   true,
	"DOCKERHUB_TOKEN":                true,
}

// Token credential keys
var tokenCredKeys = []string{"NGC_API_KEY", "GITHUB_TOKEN", "DOCKERHUB_TOKEN"}

// ParseRegistry extracts the registry hostname from an image reference
// Examples:
//   - "docker.io/library/ubuntu:20.04" -> "docker.io"
//   - "gcr.io/project/image:tag" -> "gcr.io"
//   - "registry.example.com:5000/image:tag" -> "registry.example.com:5000"
//   - "ubuntu:20.04" -> "docker.io" (default)
func ParseRegistry(imageRef string) string {
	if imageRef == "" {
		return ""
	}

	// Remove any transport prefix (docker://, oci://, etc.)
	imageRef = strings.TrimPrefix(imageRef, "docker://")
	imageRef = strings.TrimPrefix(imageRef, "oci://")

	// Split by / to separate registry from repository
	parts := strings.Split(imageRef, "/")

	if len(parts) == 1 {
		// No slash means it's a Docker Hub official image (e.g., "ubuntu:20.04")
		return "docker.io"
	}

	// Check if first part looks like a registry (contains . or : or is localhost)
	firstPart := parts[0]
	if strings.Contains(firstPart, ".") ||
		strings.Contains(firstPart, ":") ||
		firstPart == "localhost" {
		return firstPart
	}

	// Otherwise it's a Docker Hub user image (e.g., "username/image")
	return "docker.io"
}

// ParseCredentialsFromEnv filters a map to only include known credential keys
func ParseCredentialsFromEnv(envMap map[string]string) map[string]string {
	creds := make(map[string]string)
	for key, value := range envMap {
		if knownCredKeys[key] {
			creds[key] = value
		}
	}
	return creds
}

// DetectCredentialType determines the type of credentials based on the registry and credential keys
func DetectCredentialType(registry string, creds map[string]string) CredType {
	if len(creds) == 0 {
		return CredTypePublic
	}

	// Check for AWS credentials
	if _, hasAwsKey := creds["AWS_ACCESS_KEY_ID"]; hasAwsKey {
		if _, hasAwsSecret := creds["AWS_SECRET_ACCESS_KEY"]; hasAwsSecret {
			return CredTypeAWS
		}
	}

	// Check for GCP credentials
	if _, hasGcp := creds["GOOGLE_APPLICATION_CREDENTIALS"]; hasGcp {
		return CredTypeGCP
	}
	if _, hasGcpProject := creds["GCP_PROJECT_ID"]; hasGcpProject {
		return CredTypeGCP
	}
	if _, hasGcpToken := creds["GCP_ACCESS_TOKEN"]; hasGcpToken {
		return CredTypeGCP
	}

	// Check for Azure credentials
	if _, hasAzureClientId := creds["AZURE_CLIENT_ID"]; hasAzureClientId {
		if _, hasAzureSecret := creds["AZURE_CLIENT_SECRET"]; hasAzureSecret {
			return CredTypeAzure
		}
	}

	// Check for token-based auth (before basic auth)
	for _, key := range tokenCredKeys {
		if _, hasToken := creds[key]; hasToken {
			return CredTypeToken
		}
	}

	// Check for basic auth (username/password)
	hasUsername := false
	hasPassword := false

	for key := range creds {
		keyUpper := strings.ToUpper(key)
		if strings.Contains(keyUpper, "USERNAME") {
			hasUsername = true
		}
		if strings.Contains(keyUpper, "PASSWORD") {
			hasPassword = true
		}
	}

	if hasUsername && hasPassword {
		return CredTypeBasic
	}

	// Detect based on registry
	registry = strings.ToLower(registry)
	if strings.Contains(registry, "ecr") || strings.Contains(registry, "amazonaws.com") {
		return CredTypeAWS
	}
	if strings.Contains(registry, "gcr.io") || strings.Contains(registry, "pkg.dev") {
		return CredTypeGCP
	}
	if strings.Contains(registry, "azurecr.io") {
		return CredTypeAzure
	}

	return CredTypeUnknown
}

// MarshalCredentials serializes credentials to JSON format
// The JSON structure includes the registry and credential type for easier retrieval
func MarshalCredentials(registry string, credType CredType, creds map[string]string) (string, error) {
	data := map[string]interface{}{
		"registry":    registry,
		"type":        string(credType),
		"credentials": creds,
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal credentials: %w", err)
	}

	return string(jsonBytes), nil
}

// CreateSecretName generates a consistent secret name for a registry
// Format: "oci-registry-<normalized-registry-name>"
func CreateSecretName(registry string) string {
	// Normalize registry name for use in secret name
	normalized := strings.ToLower(registry)
	normalized = strings.ReplaceAll(normalized, ".", "-")
	normalized = strings.ReplaceAll(normalized, ":", "-")
	normalized = strings.ReplaceAll(normalized, "/", "-")

	return fmt.Sprintf("oci-registry-%s", normalized)
}

// CreateProviderFromCredentials creates a CLIP-compatible credential provider from a credential map
// This function creates an appropriate credential provider without setting environment variables
// Returns common.RegistryCredentialProvider
func CreateProviderFromCredentials(ctx context.Context, registry string, credType CredType, creds map[string]string) common.RegistryCredentialProvider {
	// Delegate directly to CLIP's implementation which has proper support for:
	// - ECRProvider for AWS (calls ECR API to get tokens)
	// - GARProvider for GCP (proper OAuth2 token handling)
	// - BasicAuthProvider for username/password registries
	// - TokenProvider for token-based auth
	// Beta9 should not reimplement this logic!
	return common.CredentialsToProvider(ctx, registry, creds)
}

// CreateProviderFromEnv creates a CLIP-compatible credential provider from environment variables
// This function reads credentials from the environment based on the provided keys and creates
// an appropriate credential provider for OCI registries
// Returns common.RegistryCredentialProvider
func CreateProviderFromEnv(ctx context.Context, registry string, credKeys []string) (common.RegistryCredentialProvider, error) {
	if len(credKeys) == 0 {
		return common.NewPublicOnlyProvider(), nil
	}

	// Build credential map from environment
	creds := make(map[string]string)
	for _, key := range credKeys {
		if value := os.Getenv(key); value != "" {
			creds[key] = value
		}
	}

	if len(creds) == 0 {
		return common.NewPublicOnlyProvider(), nil
	}

	// Detect credential type
	credType := DetectCredentialType(registry, creds)

	// Create a callback provider that handles credentials based on type
	callback := func(ctx context.Context, reg string, scope string) (*authn.AuthConfig, error) {
		// Only handle the registry we're configured for
		if reg != registry {
			return nil, common.ErrNoCredentials
		}

		switch credType {
		case CredTypeBasic:
			// Basic auth with username/password
			username := ""
			password := ""

			// Try different username keys
			for key, value := range creds {
				keyUpper := strings.ToUpper(key)
				if strings.Contains(keyUpper, "USERNAME") {
					username = value
				}
				if strings.Contains(keyUpper, "PASSWORD") {
					password = value
				}
			}

			if username != "" && password != "" {
				return &authn.AuthConfig{
					Username: username,
					Password: password,
				}, nil
			}
			return nil, common.ErrNoCredentials

		case CredTypeAWS:
			// For AWS ECR, set AWS credentials and use keychain (handles ECR automatically)
			if accessKey, ok := creds["AWS_ACCESS_KEY_ID"]; ok {
				os.Setenv("AWS_ACCESS_KEY_ID", accessKey)
			}
			if secretKey, ok := creds["AWS_SECRET_ACCESS_KEY"]; ok {
				os.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
			}
			if sessionToken, ok := creds["AWS_SESSION_TOKEN"]; ok {
				os.Setenv("AWS_SESSION_TOKEN", sessionToken)
			}
			if region, ok := creds["AWS_REGION"]; ok {
				os.Setenv("AWS_REGION", region)
			}

			// Use keychain provider which handles ECR
			keychain := common.NewKeychainProvider()
			return keychain.GetCredentials(ctx, reg, scope)

		case CredTypeGCP:
			// For GCP GCR, set GCP credentials and use keychain (handles GCR automatically)
			if token, ok := creds["GCP_ACCESS_TOKEN"]; ok {
				os.Setenv("GCP_ACCESS_TOKEN", token)
			}
			if credFile, ok := creds["GOOGLE_APPLICATION_CREDENTIALS"]; ok {
				os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)
			}
			if project, ok := creds["GCP_PROJECT_ID"]; ok {
				os.Setenv("GCP_PROJECT_ID", project)
			}

			// Use keychain provider which handles GCR
			keychain := common.NewKeychainProvider()
			return keychain.GetCredentials(ctx, reg, scope)

		case CredTypeAzure:
			// For Azure ACR, set Azure credentials and use keychain
			if clientID, ok := creds["AZURE_CLIENT_ID"]; ok {
				os.Setenv("AZURE_CLIENT_ID", clientID)
			}
			if clientSecret, ok := creds["AZURE_CLIENT_SECRET"]; ok {
				os.Setenv("AZURE_CLIENT_SECRET", clientSecret)
			}
			if tenantID, ok := creds["AZURE_TENANT_ID"]; ok {
				os.Setenv("AZURE_TENANT_ID", tenantID)
			}

			// Use keychain provider which handles ACR
			keychain := common.NewKeychainProvider()
			return keychain.GetCredentials(ctx, reg, scope)

		case CredTypeToken:
			// For token-based auth, use token as password with oauth2accesstoken username
			for _, key := range tokenCredKeys {
				if token, ok := creds[key]; ok && token != "" {
					return &authn.AuthConfig{
						Username: "oauth2accesstoken",
						Password: token,
					}, nil
				}
			}
			return nil, common.ErrNoCredentials

		default:
			return nil, common.ErrNoCredentials
		}
	}

	return common.NewCallbackProviderWithName(fmt.Sprintf("env-%s", registry), callback), nil
}

// UnmarshalCredentials parses a JSON credential string
func UnmarshalCredentials(credentialJSON string) (registry string, credType CredType, creds map[string]string, err error) {
	var data map[string]interface{}

	if err := json.Unmarshal([]byte(credentialJSON), &data); err != nil {
		return "", "", nil, fmt.Errorf("failed to unmarshal credentials: %w", err)
	}

	registry, _ = data["registry"].(string)
	credTypeStr, _ := data["type"].(string)
	credType = CredType(credTypeStr)

	creds = make(map[string]string)
	if credMap, ok := data["credentials"].(map[string]interface{}); ok {
		for k, v := range credMap {
			if strVal, ok := v.(string); ok {
				creds[k] = strVal
			}
		}
	}

	return registry, credType, creds, nil
}

// RegistryCredentialGetter is a function that retrieves credentials for a registry
// It takes a map of raw credentials and returns structured credentials
type RegistryCredentialGetter func(creds map[string]string) (map[string]string, error)

// RegistryTokenGetter is a function that retrieves credentials as username:password format (legacy)
type RegistryTokenGetter func(creds map[string]string) (string, error)

// Helper functions for credential validation
func validateRequiredCredential(creds map[string]string, key string) (string, error) {
	if value := creds[key]; value != "" {
		return value, nil
	}
	return "", ErrMissingCredential(key)
}

// ErrMissingCredential returns an error for a missing required credential
func ErrMissingCredential(key string) error {
	return fmt.Errorf("%s not found or empty", key)
}

// ErrBothCredentialsRequired returns an error when both credentials are required
func ErrBothCredentialsRequired(key1, key2 string) error {
	return fmt.Errorf("both %s and %s must be provided together", key1, key2)
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
		return "", "", false, ErrBothCredentialsRequired(usernameKey, passwordKey)
	}

	// If only one credential is provided (non-empty), both are required
	if (username == "" && password != "") || (username != "" && password == "") {
		return "", "", false, ErrBothCredentialsRequired(usernameKey, passwordKey)
	}

	return username, password, username != "" && password != "", nil
}

func buildBasicAuthToken(username, password string) string {
	return fmt.Sprintf("%s:%s", username, password)
}

func containsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// ContainsAny checks if string s contains any of the given substrings (exported for testing)
func ContainsAny(s string, substrs ...string) bool {
	return containsAny(s, substrs...)
}

// Registry domain constants
var (
	awsRegistryDomains  = []string{"amazonaws.com"}
	gcpRegistryDomains  = []string{"pkg.dev", "gcr.io"}
	ngcRegistryDomains  = []string{"nvcr.io"}
	ghcrRegistryDomains = []string{"ghcr.io"}
)

// GetECRCredentials returns structured AWS ECR credentials
func GetECRCredentials(creds map[string]string) (map[string]string, error) {
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

// GetECRToken retrieves ECR authorization token (legacy format: username:password)
func GetECRToken(creds map[string]string) (string, error) {
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
func GetGARCredentials(creds map[string]string) (map[string]string, error) {
	token, err := validateRequiredCredential(creds, "GCP_ACCESS_TOKEN")
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"GCP_ACCESS_TOKEN": token,
	}, nil
}

// GetGARToken returns GCP GAR token (legacy format: username:password)
func GetGARToken(creds map[string]string) (string, error) {
	token, err := validateRequiredCredential(creds, "GCP_ACCESS_TOKEN")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("oauth2accesstoken", token), nil
}

// GetNGCCredentials returns structured NGC credentials
func GetNGCCredentials(creds map[string]string) (map[string]string, error) {
	apiKey, err := validateRequiredCredential(creds, "NGC_API_KEY")
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"NGC_API_KEY": apiKey,
	}, nil
}

// GetNGCToken returns NGC token (legacy format: username:password)
func GetNGCToken(creds map[string]string) (string, error) {
	apiKey, err := validateRequiredCredential(creds, "NGC_API_KEY")
	if err != nil {
		return "", err
	}
	return buildBasicAuthToken("$oauthtoken", apiKey), nil
}

// GetGHCRCredentials returns structured GHCR credentials
func GetGHCRCredentials(creds map[string]string) (map[string]string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(creds, "GITHUB_USERNAME", "GITHUB_TOKEN")
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

// GetGHCRToken returns GHCR token (legacy format: username:password)
func GetGHCRToken(creds map[string]string) (string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(creds, "GITHUB_USERNAME", "GITHUB_TOKEN")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	return "", nil // Public access
}

// GetDockerHubCredentials returns structured Docker Hub credentials
func GetDockerHubCredentials(creds map[string]string) (map[string]string, error) {
	username, password, hasAuth, err := validateOptionalCredentials(creds, "DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD")
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

// GetDockerHubToken returns Docker Hub token (legacy format: username:password)
// Also serves as the default handler for generic registries with basic auth
func GetDockerHubToken(creds map[string]string) (string, error) {
	// Try Docker Hub specific keys first
	username, password, hasAuth, err := validateOptionalCredentials(creds, "DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	
	// Fall back to generic USERNAME/PASSWORD for non-Docker Hub registries
	username, password, hasAuth, err = validateOptionalCredentials(creds, "USERNAME", "PASSWORD")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	
	// Also try REGISTRY_USERNAME/REGISTRY_PASSWORD
	username, password, hasAuth, err = validateOptionalCredentials(creds, "REGISTRY_USERNAME", "REGISTRY_PASSWORD")
	if err != nil {
		return "", err
	}
	if hasAuth {
		return buildBasicAuthToken(username, password), nil
	}
	
	return "", nil // Public access
}

// GetRegistryCredentialsForImage determines the appropriate credentials for an image URI
// Returns both the registry name and structured credentials
func GetRegistryCredentialsForImage(imageURI string, creds map[string]string) (registry string, structuredCreds map[string]string, err error) {
	registry = ParseRegistry(imageURI)
	if registry == "" {
		return "", nil, fmt.Errorf("failed to parse registry from image URI: %s", imageURI)
	}

	var fn RegistryCredentialGetter

	switch {
	case containsAny(imageURI, awsRegistryDomains...):
		fn = GetECRCredentials
	case containsAny(imageURI, gcpRegistryDomains...):
		fn = GetGARCredentials
	case containsAny(imageURI, ngcRegistryDomains...):
		fn = GetNGCCredentials
	case containsAny(imageURI, ghcrRegistryDomains...):
		fn = GetGHCRCredentials
	default:
		fn = GetDockerHubCredentials
	}

	structuredCreds, err = fn(creds)
	if err != nil {
		return registry, nil, err
	}

	return registry, structuredCreds, nil
}

// GetRegistryTokenForImage determines the appropriate credentials for an image URI
// Returns credentials in legacy username:password format
func GetRegistryTokenForImage(imageURI string, creds map[string]string) (string, error) {
	var fn RegistryTokenGetter

	switch {
	case containsAny(imageURI, awsRegistryDomains...):
		fn = GetECRToken
	case containsAny(imageURI, gcpRegistryDomains...):
		fn = GetGARToken
	case containsAny(imageURI, ngcRegistryDomains...):
		fn = GetNGCToken
	case containsAny(imageURI, ghcrRegistryDomains...):
		fn = GetGHCRToken
	default:
		fn = GetDockerHubToken
	}

	return fn(creds)
}

// ParseCredentialsFromJSON parses credentials from JSON string or username:password format
// Returns structured credentials
func ParseCredentialsFromJSON(credStr string) (map[string]string, error) {
	if credStr == "" {
		return nil, nil
	}

	// Try structured format first (from MarshalCredentials)
	// Format: {"registry":"...", "type":"...", "credentials":{...}}
	if _, _, creds, err := UnmarshalCredentials(credStr); err == nil && len(creds) > 0 {
		return creds, nil
	}

	// Try flat JSON map format
	var credMap map[string]string
	if err := json.Unmarshal([]byte(credStr), &credMap); err == nil {
		return ParseCredentialsFromEnv(credMap), nil
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

// CredentialsToProvider converts a credential map and registry to a CLIP-compatible provider
// This is the main function that should be used to create providers for CLIP
// It delegates directly to CLIP's implementation which has proper ECRProvider, GARProvider, etc.
func CredentialsToProvider(ctx context.Context, registry string, creds map[string]string) common.RegistryCredentialProvider {
	// Just delegate to CLIP's implementation - it will:
	// 1. Auto-detect credential type (AWS, GCP, basic auth, token, etc.)
	// 2. Create the appropriate provider (ECRProvider for AWS, GARProvider for GCP, etc.)
	// 3. Handle token fetching and authentication properly
	return common.CredentialsToProvider(ctx, registry, creds)
}
