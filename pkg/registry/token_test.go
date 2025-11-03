package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRegistryTokenForImageGenericRegistry(t *testing.T) {
	tests := []struct {
		name          string
		imageURI      string
		creds         map[string]string
		expectedToken string
		expectError   bool
	}{
		{
			name:     "generic registry with USERNAME/PASSWORD",
			imageURI: "registry.example.com/repo:tag",
			creds: map[string]string{
				"USERNAME": "testuser",
				"PASSWORD": "testpass",
			},
			expectedToken: "testuser:testpass",
		},
		{
			name:     "docker.io with DOCKERHUB credentials",
			imageURI: "docker.io/library/ubuntu:20.04",
			creds: map[string]string{
				"DOCKERHUB_USERNAME": "dockeruser",
				"DOCKERHUB_PASSWORD": "dockerpass",
			},
			expectedToken: "dockeruser:dockerpass",
		},
		{
			name:     "docker.io with generic USERNAME/PASSWORD",
			imageURI: "docker.io/myorg/myrepo:latest",
			creds: map[string]string{
				"USERNAME": "user",
				"PASSWORD": "pass",
			},
			expectedToken: "user:pass",
		},
		{
			name:          "no credentials",
			imageURI:      "registry.example.com/repo:tag",
			creds:         map[string]string{},
			expectedToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GetRegistryTokenForImage(tt.imageURI, tt.creds)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedToken, token)
			}
		})
	}
}
