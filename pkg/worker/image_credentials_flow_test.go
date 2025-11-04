package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestCredentialFlowForBuiltImage(t *testing.T) {
	ctx := context.Background()
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			BuildRegistry: "registry.example.com",
			ClipVersion:   2,
		},
	}

	client := &ImageClient{
		config:      config,
		v2ImageRefs: common.NewSafeMap[string](),
	}

	imageId := "test-image-123"
	pushedImageRef := "registry.example.com/userimages:workspace-456-test-image-123"
	client.v2ImageRefs.Set(imageId, pushedImageRef)

	request := &types.ContainerRequest{
		ImageId:                  imageId,
		BuildRegistryCredentials: "builduser:buildpass",
	}

	credProvider := client.getCredentialProviderForImage(ctx, imageId, request)
	assert.NotNil(t, credProvider, "should use build registry credentials")
}

func TestCredentialPriority(t *testing.T) {
	ctx := context.Background()
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			BuildRegistry: "registry.example.com",
			ClipVersion:   2,
		},
	}

	imageId := "test-image-123"

	tests := []struct {
		name                     string
		imageRef                 string
		imageCredentials         string
		sourceImageCreds         string
		buildRegistryCredentials string
		expectCredentials        bool
	}{
		{
			name:                     "Runtime credentials take priority",
			imageRef:                 "registry.example.com/userimages:ws-img",
			imageCredentials:         "runtime:secret",
			sourceImageCreds:         "source:creds",
			buildRegistryCredentials: "build:creds",
			expectCredentials:        true,
		},
		{
			name:                     "Source image credentials if no runtime creds",
			imageRef:                 "custom-registry.com/myimage:latest",
			imageCredentials:         "",
			sourceImageCreds:         "custom:creds",
			buildRegistryCredentials: "build:creds",
			expectCredentials:        true,
		},
		{
			name:                     "Build registry credentials for built images",
			imageRef:                 "registry.example.com/userimages:ws-img",
			imageCredentials:         "",
			sourceImageCreds:         "",
			buildRegistryCredentials: "build:creds",
			expectCredentials:        true,
		},
		{
			name:                     "Ambient auth when no credentials",
			imageRef:                 "registry.example.com/userimages:ws-img",
			imageCredentials:         "",
			sourceImageCreds:         "",
			buildRegistryCredentials: "",
			expectCredentials:        false,
		},
		{
			name:                     "No build creds for non-build-registry images",
			imageRef:                 "docker.io/library/ubuntu:20.04",
			imageCredentials:         "",
			sourceImageCreds:         "",
			buildRegistryCredentials: "build:creds",
			expectCredentials:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ImageClient{
				config:      config,
				v2ImageRefs: common.NewSafeMap[string](),
			}

			client.v2ImageRefs.Set(imageId, tt.imageRef)

			request := &types.ContainerRequest{
				ImageId:                  imageId,
				ImageCredentials:         tt.imageCredentials,
				BuildRegistryCredentials: tt.buildRegistryCredentials,
				BuildOptions: types.BuildOptions{
					SourceImageCreds: tt.sourceImageCreds,
				},
			}

			credProvider := client.getCredentialProviderForImage(ctx, imageId, request)

			if tt.expectCredentials {
				assert.NotNil(t, credProvider, "should have credentials")
			} else {
				assert.Nil(t, credProvider, "should use ambient auth")
			}
		})
	}
}

func TestBuildRegistryCredentialUsage(t *testing.T) {
	ctx := context.Background()
	imageId := "test-image"
	buildRegistryCredentials := "token:abc123"

	tests := []struct {
		name                string
		buildRegistry       string
		cachedImageRef      string
		shouldUseBuildCreds bool
	}{
		{
			name:                "Built image in ECR",
			buildRegistry:       "123456789012.dkr.ecr.us-east-1.amazonaws.com",
			cachedImageRef:      "123456789012.dkr.ecr.us-east-1.amazonaws.com/userimages:ws-img",
			shouldUseBuildCreds: true,
		},
		{
			name:                "Built image in GCR",
			buildRegistry:       "us-central1-docker.pkg.dev/project/repo",
			cachedImageRef:      "us-central1-docker.pkg.dev/project/repo/userimages:ws-img",
			shouldUseBuildCreds: true,
		},
		{
			name:                "Base image from Docker Hub",
			buildRegistry:       "registry.company.com",
			cachedImageRef:      "docker.io/library/python:3.11",
			shouldUseBuildCreds: false,
		},
		{
			name:                "Base image from custom registry",
			buildRegistry:       "registry.company.com",
			cachedImageRef:      "custom-registry.io/myorg/myimage:v1",
			shouldUseBuildCreds: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.AppConfig{
				ImageService: types.ImageServiceConfig{
					BuildRegistry: tt.buildRegistry,
					ClipVersion:   2,
				},
			}

			client := &ImageClient{
				config:      config,
				v2ImageRefs: common.NewSafeMap[string](),
			}

			client.v2ImageRefs.Set(imageId, tt.cachedImageRef)

			request := &types.ContainerRequest{
				ImageId:                  imageId,
				BuildRegistryCredentials: buildRegistryCredentials,
			}

			credProvider := client.getCredentialProviderForImage(ctx, imageId, request)

			if tt.shouldUseBuildCreds {
				assert.NotNil(t, credProvider, "should use build registry credentials")
			} else {
				assert.Nil(t, credProvider, "should NOT use build registry credentials")
			}
		})
	}
}
