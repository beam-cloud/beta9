package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
