package oci

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

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
	"AZURE_CLIENT_ID":                true,
	"AZURE_CLIENT_SECRET":            true,
	"AZURE_TENANT_ID":                true,
	"DOCKER_USERNAME":                true,
	"DOCKER_PASSWORD":                true,
	"REGISTRY_USERNAME":              true,
	"REGISTRY_PASSWORD":              true,
}

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

	// Check for Azure credentials
	if _, hasAzureClientId := creds["AZURE_CLIENT_ID"]; hasAzureClientId {
		if _, hasAzureSecret := creds["AZURE_CLIENT_SECRET"]; hasAzureSecret {
			return CredTypeAzure
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

// CreateProviderFromEnv creates a CLIP-compatible credential provider from environment variables
// This function reads credentials from the environment based on the provided keys and creates
// an appropriate credential provider for OCI registries
// Returns interface{} to match CLIP's MountOptions.RegistryCredProvider type
func CreateProviderFromEnv(ctx context.Context, registry string, credKeys []string) (interface{}, error) {
	if len(credKeys) == 0 {
		return nil, nil
	}

	// Build credential map from environment
	creds := make(map[string]string)
	for _, key := range credKeys {
		if value := os.Getenv(key); value != "" {
			creds[key] = value
		}
	}

	if len(creds) == 0 {
		return nil, nil
	}

	// Detect credential type
	credType := DetectCredentialType(registry, creds)

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
			// Return an authn.Authenticator (compatible with go-containerregistry)
			return &authn.Basic{
				Username: username,
				Password: password,
			}, nil
		}

	case CredTypeAWS:
		// For AWS ECR, we'd need to implement AWS-specific authentication
		// For now, return nil and let default AWS chain handle it
		// TODO: Implement proper ECR authentication using AWS SDK
		return nil, nil

	case CredTypeGCP:
		// For GCP, we'd use the credentials file path
		// The go-containerregistry library can handle this through default keychain
		return nil, nil

	case CredTypeAzure:
		// For Azure ACR, similar to AWS, we'd need Azure-specific auth
		// TODO: Implement proper ACR authentication
		return nil, nil
	}

	return nil, fmt.Errorf("unable to create credential provider for type: %s", credType)
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
