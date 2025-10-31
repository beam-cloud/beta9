package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/clip/pkg/provider"
	"github.com/beam-cloud/clip/pkg/provider/aws"
	"github.com/beam-cloud/clip/pkg/provider/gcp"
	"github.com/beam-cloud/clip/pkg/provider/generic"
)

// CredentialType represents the type of credentials detected
type CredentialType string

const (
	CredTypePublic   CredentialType = "public"
	CredTypeAWS      CredentialType = "aws"
	CredTypeGCP      CredentialType = "gcp"
	CredTypeBasic    CredentialType = "basic"
	CredTypeToken    CredentialType = "token"
)

// Known credential environment variable keys
var (
	awsCredKeys   = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION"}
	gcpCredKeys   = []string{"GCP_ACCESS_TOKEN"}
	basicCredKeys = []string{"USERNAME", "PASSWORD"}
	tokenCredKeys = []string{"NGC_API_KEY", "GITHUB_TOKEN", "DOCKERHUB_TOKEN"}
)

// ParseRegistry extracts the registry hostname from an image reference
func ParseRegistry(imageRef string) string {
	parts := strings.SplitN(imageRef, "/", 2)
	if len(parts) < 2 {
		return "docker.io"
	}
	
	// Check if first part looks like a registry (contains a dot or colon)
	if strings.Contains(parts[0], ".") || strings.Contains(parts[0], ":") {
		return parts[0]
	}
	
	return "docker.io"
}

// ParseCredentialsFromEnv extracts known credential keys from a credential map
func ParseCredentialsFromEnv(envMap map[string]string) map[string]string {
	creds := make(map[string]string)
	allKnownKeys := append(append(append(awsCredKeys, gcpCredKeys...), basicCredKeys...), tokenCredKeys...)
	
	for _, key := range allKnownKeys {
		if val, ok := envMap[key]; ok && val != "" {
			creds[key] = val
		}
	}
	
	return creds
}

// DetectCredentialType determines the type of credentials based on what keys are present
func DetectCredentialType(registry string, creds map[string]string) CredentialType {
	if len(creds) == 0 {
		return CredTypePublic
	}
	
	// Check for AWS credentials
	if _, hasAccessKey := creds["AWS_ACCESS_KEY_ID"]; hasAccessKey {
		if _, hasSecretKey := creds["AWS_SECRET_ACCESS_KEY"]; hasSecretKey {
			return CredTypeAWS
		}
	}
	
	// Check for GCP credentials
	if _, hasToken := creds["GCP_ACCESS_TOKEN"]; hasToken {
		return CredTypeGCP
	}
	
	// Check for token-based auth
	for _, key := range tokenCredKeys {
		if _, hasToken := creds[key]; hasToken {
			return CredTypeToken
		}
	}
	
	// Check for basic auth
	if _, hasUser := creds["USERNAME"]; hasUser {
		if _, hasPass := creds["PASSWORD"]; hasPass {
			return CredTypeBasic
		}
	}
	
	return CredTypePublic
}

// MarshalCredentials converts credentials to JSON for storage
func MarshalCredentials(registry string, credType CredentialType, creds map[string]string) (string, error) {
	payload := map[string]interface{}{
		"registry":    registry,
		"credentials": creds,
	}
	
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	
	return string(data), nil
}

// CreateSecretName generates a consistent secret name for a registry
func CreateSecretName(registry string) string {
	// Normalize the registry name to create a valid secret name
	normalized := strings.ReplaceAll(registry, ".", "-")
	normalized = strings.ReplaceAll(normalized, ":", "-")
	normalized = strings.ToLower(normalized)
	return fmt.Sprintf("oci-registry-%s", normalized)
}

// CreateProviderFromEnv creates a CLIP credential provider from environment variables
func CreateProviderFromEnv(ctx context.Context, registry string, credKeys []string) (provider.Provider, error) {
	// Collect credentials from environment
	creds := make(map[string]string)
	for _, key := range credKeys {
		// Keys are already set in environment by caller
		creds[key] = ""  // Will be read by provider from env
	}
	
	// Detect credential type and create appropriate provider
	credType := DetectCredentialType(registry, creds)
	
	switch credType {
	case CredTypeAWS:
		if region, ok := creds["AWS_REGION"]; ok && region != "" {
			return aws.NewProvider(ctx, registry, region)
		}
		return nil, fmt.Errorf("AWS_REGION required for ECR authentication")
		
	case CredTypeGCP:
		return gcp.NewProvider(ctx, registry)
		
	default:
		// For basic auth and tokens, use generic provider
		// The generic provider reads from environment variables
		return generic.NewProvider(ctx, registry)
	}
}
