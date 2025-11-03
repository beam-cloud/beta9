package image

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestGenerateBuildRegistryCredentials(t *testing.T) {
	tests := []struct {
		name           string
		buildRegistry  string
		credConfig     types.BuildRegistryCredentialsConfig
		expectedToken  string // empty means we expect no token
		expectError    bool
	}{
		{
			name:          "localhost registry returns empty",
			buildRegistry: "localhost",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "user",
					"PASSWORD": "pass",
				},
			},
			expectedToken: "",
		},
		{
			name:          "empty registry returns empty",
			buildRegistry: "",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "user",
					"PASSWORD": "pass",
				},
			},
			expectedToken: "",
		},
		{
			name:          "no credentials configured returns empty",
			buildRegistry: "registry.example.com",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type:        "",
				Credentials: nil,
			},
			expectedToken: "",
		},
		{
			name:          "empty credentials map returns empty",
			buildRegistry: "registry.example.com",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type:        "basic",
				Credentials: map[string]string{},
			},
			expectedToken: "",
		},
		{
			name:          "basic auth generates token",
			buildRegistry: "registry.example.com",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "testuser",
					"PASSWORD": "testpass",
				},
			},
			expectedToken: "testuser:testpass",
		},
		{
			name:          "docker hub credentials with DOCKERHUB prefix",
			buildRegistry: "docker.io",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"DOCKERHUB_USERNAME": "dockeruser",
					"DOCKERHUB_PASSWORD": "dockerpass",
				},
			},
			expectedToken: "dockeruser:dockerpass",
		},
		{
			name:          "docker hub credentials with USERNAME/PASSWORD",
			buildRegistry: "index.docker.io",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "dockeruser2",
					"PASSWORD": "dockerpass2",
				},
			},
			expectedToken: "dockeruser2:dockerpass2",
		},
		{
			name:          "127.0.0.1 registry returns empty",
			buildRegistry: "127.0.0.1:5000",
			credConfig: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "user",
					"PASSWORD": "pass",
				},
			},
			expectedToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authInfo := &auth.AuthInfo{
				Workspace: &types.Workspace{
					Name:       "test",
					ExternalId: "test-workspace",
				},
			}

			config := types.AppConfig{
				ImageService: types.ImageServiceConfig{
					BuildRegistry:            tt.buildRegistry,
					BuildRegistryCredentials: tt.credConfig,
					ClipVersion:              2,
				},
			}

			build := &Build{
				config:   config,
				authInfo: authInfo,
			}

			token := build.generateBuildRegistryCredentials()

			if tt.expectedToken == "" {
				assert.Empty(t, token, "expected empty token")
			} else {
				assert.Equal(t, tt.expectedToken, token, "token mismatch")
			}
		})
	}
}

func TestGenerateBuildRegistryCredentialsIntegration(t *testing.T) {
	// This test verifies the full flow with generateContainerRequest
	ctx := context.Background()
	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Name:       "test",
			ExternalId: "test-workspace-id",
		},
		Token: &types.Token{
			Id: 1,
		},
	}
	ctx = context.WithValue(ctx, "auth", authInfo)

	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			BuildRegistry: "registry.example.com",
			BuildRegistryCredentials: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "builduser",
					"PASSWORD": "buildpass",
				},
			},
			BuildContainerCpu:    1000,
			BuildContainerMemory: 1024,
			ClipVersion:          2,
		},
	}

	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "20.04",
		PythonVersion:     "python3.11",
		ClipVersion:       2,
	}

	outputChan := make(chan common.OutputMsg, 10)
	build, err := NewBuild(ctx, opts, outputChan, config)
	assert.NoError(t, err)

	req, err := build.generateContainerRequest()
	assert.NoError(t, err)
	assert.NotNil(t, req)

	// Verify BuildRegistryCreds was set at top-level (not in BuildOptions)
	assert.Equal(t, "builduser:buildpass", req.BuildRegistryCreds)
}

func TestGenerateBuildRegistryCredentialsNoConfig(t *testing.T) {
	// Test that when no build registry is configured, no credentials are generated
	ctx := context.Background()
	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Name:       "test",
			ExternalId: "test-workspace-id",
		},
		Token: &types.Token{
			Id: 1,
		},
	}
	ctx = context.WithValue(ctx, "auth", authInfo)

	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			BuildRegistry: "", // No build registry
			BuildRegistryCredentials: types.BuildRegistryCredentialsConfig{
				Type: "basic",
				Credentials: map[string]string{
					"USERNAME": "builduser",
					"PASSWORD": "buildpass",
				},
			},
			BuildContainerCpu:    1000,
			BuildContainerMemory: 1024,
			ClipVersion:          2,
		},
	}

	opts := &BuildOpts{
		BaseImageRegistry: "docker.io",
		BaseImageName:     "library/ubuntu",
		BaseImageTag:      "20.04",
		PythonVersion:     "python3.11",
		ClipVersion:       2,
	}

	outputChan := make(chan common.OutputMsg, 10)
	build, err := NewBuild(ctx, opts, outputChan, config)
	assert.NoError(t, err)

	req, err := build.generateContainerRequest()
	assert.NoError(t, err)
	assert.NotNil(t, req)

	// Verify BuildRegistryCreds is empty when no build registry configured
	assert.Empty(t, req.BuildRegistryCreds)
}
