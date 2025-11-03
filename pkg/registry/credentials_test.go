package registry

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRegistry(t *testing.T) {
	tests := []struct {
		name     string
		imageRef string
		expected string
	}{
		{
			name:     "docker hub official image",
			imageRef: "ubuntu:20.04",
			expected: "docker.io",
		},
		{
			name:     "docker hub user image",
			imageRef: "username/image:tag",
			expected: "docker.io",
		},
		{
			name:     "explicit docker.io",
			imageRef: "docker.io/library/ubuntu:20.04",
			expected: "docker.io",
		},
		{
			name:     "gcr.io registry",
			imageRef: "gcr.io/project/image:tag",
			expected: "gcr.io",
		},
		{
			name:     "custom registry with port",
			imageRef: "registry.example.com:5000/image:tag",
			expected: "registry.example.com:5000",
		},
		{
			name:     "localhost registry",
			imageRef: "localhost:5000/image:tag",
			expected: "localhost:5000",
		},
		{
			name:     "ecr registry",
			imageRef: "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
			expected: "123456789012.dkr.ecr.us-east-1.amazonaws.com",
		},
		{
			name:     "with docker:// prefix",
			imageRef: "docker://ubuntu:20.04",
			expected: "docker.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseRegistry(tt.imageRef)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCredentialsFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name: "basic auth credentials",
			input: map[string]string{
				"USERNAME": "myuser",
				"PASSWORD": "mypass",
				"RANDOM":   "ignored",
			},
			expected: map[string]string{
				"USERNAME": "myuser",
				"PASSWORD": "mypass",
			},
		},
		{
			name: "aws credentials",
			input: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"AWS_REGION":            "us-east-1",
				"OTHER_VAR":             "ignored",
			},
			expected: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"AWS_REGION":            "us-east-1",
			},
		},
		{
			name:     "no credentials",
			input:    map[string]string{"RANDOM": "value"},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCredentialsFromEnv(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectCredentialType(t *testing.T) {
	tests := []struct {
		name     string
		registry string
		creds    map[string]string
		expected CredType
	}{
		{
			name:     "public registry (no creds)",
			registry: "docker.io",
			creds:    map[string]string{},
			expected: CredTypePublic,
		},
		{
			name:     "basic auth",
			registry: "docker.io",
			creds: map[string]string{
				"USERNAME": "user",
				"PASSWORD": "pass",
			},
			expected: CredTypeBasic,
		},
		{
			name:     "aws ecr",
			registry: "123456789012.dkr.ecr.us-east-1.amazonaws.com",
			creds: map[string]string{
				"AWS_ACCESS_KEY_ID":     "key",
				"AWS_SECRET_ACCESS_KEY": "secret",
			},
			expected: CredTypeAWS,
		},
		{
			name:     "aws ecr by registry name",
			registry: "123456789012.dkr.ecr.us-east-1.amazonaws.com",
			creds:    map[string]string{"SOME_KEY": "value"},
			expected: CredTypeAWS,
		},
		{
			name:     "gcr",
			registry: "gcr.io",
			creds:    map[string]string{"GCP_PROJECT_ID": "project"},
			expected: CredTypeGCP,
		},
		{
			name:     "azure acr",
			registry: "myregistry.azurecr.io",
			creds: map[string]string{
				"AZURE_CLIENT_ID":     "client",
				"AZURE_CLIENT_SECRET": "secret",
			},
			expected: CredTypeAzure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectCredentialType(tt.registry, tt.creds)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMarshalCredentials(t *testing.T) {
	registry := "docker.io"
	credType := CredTypeBasic
	creds := map[string]string{
		"USERNAME": "myuser",
		"PASSWORD": "mypass",
	}

	result, err := MarshalCredentials(registry, credType, creds)
	require.NoError(t, err)
	assert.NotEmpty(t, result)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(result), &parsed)
	require.NoError(t, err)

	assert.Equal(t, registry, parsed["registry"])
	assert.Equal(t, string(credType), parsed["type"])
	assert.NotNil(t, parsed["credentials"])
}

func TestCreateSecretName(t *testing.T) {
	tests := []struct {
		name     string
		registry string
		expected string
	}{
		{
			name:     "docker.io",
			registry: "docker.io",
			expected: "oci-registry-docker-io",
		},
		{
			name:     "registry with port",
			registry: "registry.example.com:5000",
			expected: "oci-registry-registry-example-com-5000",
		},
		{
			name:     "gcr.io",
			registry: "gcr.io",
			expected: "oci-registry-gcr-io",
		},
		{
			name:     "ecr registry",
			registry: "123456789012.dkr.ecr.us-east-1.amazonaws.com",
			expected: "oci-registry-123456789012-dkr-ecr-us-east-1-amazonaws-com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateSecretName(tt.registry)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateProviderFromEnv(t *testing.T) {
	ctx := context.Background()

	t.Run("basic auth", func(t *testing.T) {
		// Set environment variables
		os.Setenv("USERNAME", "testuser")
		os.Setenv("PASSWORD", "testpass")
		defer os.Unsetenv("USERNAME")
		defer os.Unsetenv("PASSWORD")

		provider, err := CreateProviderFromEnv(ctx, "docker.io", []string{"USERNAME", "PASSWORD"})
		require.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("no credentials", func(t *testing.T) {
		provider, err := CreateProviderFromEnv(ctx, "docker.io", []string{})
		require.NoError(t, err)
		assert.NotNil(t, provider) // Returns PublicOnlyProvider for public registry access
	})

	t.Run("credentials not in environment", func(t *testing.T) {
		provider, err := CreateProviderFromEnv(ctx, "docker.io", []string{"NONEXISTENT_KEY"})
		require.NoError(t, err)
		assert.NotNil(t, provider) // Returns PublicOnlyProvider for public registry access
	})
}

func TestUnmarshalCredentials(t *testing.T) {
	registry := "docker.io"
	credType := CredTypeBasic
	creds := map[string]string{
		"USERNAME": "myuser",
		"PASSWORD": "mypass",
	}

	// Marshal first
	jsonStr, err := MarshalCredentials(registry, credType, creds)
	require.NoError(t, err)

	// Now unmarshal
	parsedRegistry, parsedCredType, parsedCreds, err := UnmarshalCredentials(jsonStr)
	require.NoError(t, err)

	assert.Equal(t, registry, parsedRegistry)
	assert.Equal(t, credType, parsedCredType)
	assert.Equal(t, creds, parsedCreds)
}

func TestGetDockerHubToken(t *testing.T) {
	tests := []struct {
		name          string
		creds         map[string]string
		expectedToken string
		expectError   bool
	}{
		{
			name: "DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD",
			creds: map[string]string{
				"DOCKERHUB_USERNAME": "user1",
				"DOCKERHUB_PASSWORD": "pass1",
			},
			expectedToken: "user1:pass1",
		},
		{
			name: "USERNAME and PASSWORD (generic)",
			creds: map[string]string{
				"USERNAME": "user2",
				"PASSWORD": "pass2",
			},
			expectedToken: "user2:pass2",
		},
		{
			name: "REGISTRY_USERNAME and REGISTRY_PASSWORD",
			creds: map[string]string{
				"REGISTRY_USERNAME": "user3",
				"REGISTRY_PASSWORD": "pass3",
			},
			expectedToken: "user3:pass3",
		},
		{
			name:          "no credentials returns empty",
			creds:         map[string]string{},
			expectedToken: "",
		},
		{
			name: "missing password returns error",
			creds: map[string]string{
				"USERNAME": "user",
			},
			expectedToken: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GetDockerHubToken(tt.creds)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedToken, token)
			}
		})
	}
}
