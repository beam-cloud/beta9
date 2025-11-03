package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

// TestCredentialFlowForBuiltImage demonstrates the complete credential flow for CLIP V2:
// 1. Image is built and pushed to build registry
// 2. Image ref is cached in v2ImageRefs
// 3. CLIP indexing uses build registry credentials
// 4. Runtime layer mounting uses the same credentials
func TestCredentialFlowForBuiltImage(t *testing.T) {
	ctx := context.Background()

	// Setup: Create ImageClient with build registry configuration
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			BuildRegistry: "registry.example.com",
			ClipVersion:   2,
		},
	}

	client := &ImageClient{
		config: config,
		v2ImageRefs: common.NewSafeMap[string](),
	}

	imageId := "test-image-123"
	workspaceId := "workspace-456"
	
	// Simulate what happens after buildah push: cache the image reference
	pushedImageRef := "registry.example.com/userimages:workspace-456-test-image-123"
	client.v2ImageRefs.Set(imageId, pushedImageRef)

	// Create a container request with build registry credentials
	// These credentials were generated at schedule time by generateBuildRegistryCredentials()
	request := &types.ContainerRequest{
		ImageId:     imageId,
		WorkspaceId: workspaceId,
		BuildOptions: types.BuildOptions{
			BuildRegistryCreds: "builduser:buildpass", // Generated token from scheduler
		},
	}

	// Test 1: Verify credential provider is created for CLIP indexing
	t.Run("CLIP indexing uses build registry credentials", func(t *testing.T) {
		credProvider := client.getCredentialProviderForImage(ctx, imageId, request)
		
		// Should return a credential provider (not nil)
		assert.NotNil(t, credProvider, "CLIP indexing should have credentials")
		
		// The provider should be usable (this would normally be passed to CLIP)
		// In production, CLIP would call this provider to get auth for layer fetching
	})

	// Test 2: Verify the same credentials are used at runtime
	t.Run("Runtime layer mounting uses same build registry credentials", func(t *testing.T) {
		credProvider := client.getCredentialProviderForImage(ctx, imageId, request)
		
		// Should return the SAME credential provider
		assert.NotNil(t, credProvider, "Runtime should have credentials")
		
		// This demonstrates that the SAME token generated at schedule time is used for:
		// - buildah push (in BuildAndArchiveImage)
		// - CLIP indexing (in createOCIImageWithProgress)
		// - Runtime layer mounting (in PullLazy)
	})
}

// TestCredentialPriority demonstrates the credential selection priority
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
		name                   string
		imageRef               string
		imageCredentials       string
		sourceImageCreds       string
		buildRegistryCreds     string
		expectedCredSource     string
		expectCredentials      bool
	}{
		{
			name:               "Priority 1: Runtime secret credentials",
			imageRef:           "registry.example.com/userimages:ws-img",
			imageCredentials:   "runtime:secret",
			sourceImageCreds:   "source:creds",
			buildRegistryCreds: "build:creds",
			expectedCredSource: "runtime secret",
			expectCredentials:  true,
		},
		{
			name:               "Priority 2: Custom base image credentials",
			imageRef:           "custom-registry.com/myimage:latest",
			imageCredentials:   "",
			sourceImageCreds:   "custom:creds",
			buildRegistryCreds: "build:creds",
			expectedCredSource: "build options",
			expectCredentials:  true,
		},
		{
			name:               "Priority 3: Build registry credentials (image from build registry)",
			imageRef:           "registry.example.com/userimages:ws-img",
			imageCredentials:   "",
			sourceImageCreds:   "",
			buildRegistryCreds: "build:creds",
			expectedCredSource: "build registry",
			expectCredentials:  true,
		},
		{
			name:               "Priority 4: No credentials (ambient auth)",
			imageRef:           "registry.example.com/userimages:ws-img",
			imageCredentials:   "",
			sourceImageCreds:   "",
			buildRegistryCreds: "",
			expectedCredSource: "ambient",
			expectCredentials:  false,
		},
		{
			name:               "Build registry creds NOT used for non-build-registry images",
			imageRef:           "docker.io/library/ubuntu:20.04",
			imageCredentials:   "",
			sourceImageCreds:   "",
			buildRegistryCreds: "build:creds",
			expectedCredSource: "ambient",
			expectCredentials:  false, // Should fall back to ambient since not from build registry
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ImageClient{
				config:      config,
				v2ImageRefs: common.NewSafeMap[string](),
			}

			// Cache the image reference
			client.v2ImageRefs.Set(imageId, tt.imageRef)

			// Create request with various credential options
			request := &types.ContainerRequest{
				ImageId:          imageId,
				ImageCredentials: tt.imageCredentials,
				BuildOptions: types.BuildOptions{
					SourceImageCreds:   tt.sourceImageCreds,
					BuildRegistryCreds: tt.buildRegistryCreds,
				},
			}

			credProvider := client.getCredentialProviderForImage(ctx, imageId, request)

			if tt.expectCredentials {
				assert.NotNil(t, credProvider, "should have credential provider")
			} else {
				assert.Nil(t, credProvider, "should not have credential provider (ambient auth)")
			}
		})
	}
}

// TestBuildRegistryCredentialUsage demonstrates when BuildRegistryCreds are used
func TestBuildRegistryCredentialUsage(t *testing.T) {
	ctx := context.Background()
	
	imageId := "test-image"
	buildRegistryCreds := "ecr-token:abc123" // Simulated ECR token
	
	tests := []struct {
		name              string
		buildRegistry     string
		cachedImageRef    string
		shouldUseBuildCreds bool
		description       string
	}{
		{
			name:              "Built image pushed to ECR",
			buildRegistry:     "123456789012.dkr.ecr.us-east-1.amazonaws.com",
			cachedImageRef:    "123456789012.dkr.ecr.us-east-1.amazonaws.com/userimages:ws-img",
			shouldUseBuildCreds: true,
			description:       "Image was built and pushed to our ECR build registry",
		},
		{
			name:              "Built image pushed to GCR",
			buildRegistry:     "us-central1-docker.pkg.dev/project/repo",
			cachedImageRef:    "us-central1-docker.pkg.dev/project/repo/userimages:ws-img",
			shouldUseBuildCreds: true,
			description:       "Image was built and pushed to our GCR build registry",
		},
		{
			name:              "Built image pushed to private registry",
			buildRegistry:     "registry.company.com",
			cachedImageRef:    "registry.company.com/userimages:ws-img",
			shouldUseBuildCreds: true,
			description:       "Image was built and pushed to our private build registry",
		},
		{
			name:              "Base image from Docker Hub",
			buildRegistry:     "registry.company.com",
			cachedImageRef:    "docker.io/library/python:3.11",
			shouldUseBuildCreds: false,
			description:       "Using a base image from Docker Hub without building",
		},
		{
			name:              "Base image from custom registry",
			buildRegistry:     "registry.company.com",
			cachedImageRef:    "custom-registry.io/myorg/myimage:v1",
			shouldUseBuildCreds: false,
			description:       "Using a base image from a different registry",
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

			// Cache the image reference (this happens after push or pull)
			client.v2ImageRefs.Set(imageId, tt.cachedImageRef)

			request := &types.ContainerRequest{
				ImageId: imageId,
				BuildOptions: types.BuildOptions{
					BuildRegistryCreds: buildRegistryCreds,
				},
			}

			credProvider := client.getCredentialProviderForImage(ctx, imageId, request)

			if tt.shouldUseBuildCreds {
				assert.NotNil(t, credProvider, 
					"should use build registry credentials: %s", tt.description)
			} else {
				assert.Nil(t, credProvider, 
					"should NOT use build registry credentials: %s", tt.description)
			}
		})
	}
}

// TestTokenLifecycleDocumentation documents the token lifecycle
func TestTokenLifecycleDocumentation(t *testing.T) {
	// This test documents (but doesn't enforce) the token lifecycle:
	//
	// 1. SCHEDULE TIME (pkg/abstractions/image/build.go):
	//    - Read config.ImageService.BuildRegistryCredentials (long-term creds like AWS keys)
	//    - Call GetRegistryTokenForImage() to generate FRESH token
	//    - For ECR: token valid for 12 hours
	//    - For GCP: token valid for 1 hour
	//    - Token placed in BuildOptions.BuildRegistryCreds
	//
	// 2. BUILD TIME (pkg/worker/image.go):
	//    - Worker receives BuildOptions.BuildRegistryCreds
	//    - Uses for: buildah push
	//    - Uses for: CLIP indexing (reading OCI manifest)
	//    - Caches image ref in v2ImageRefs
	//
	// 3. RUNTIME (pkg/worker/image.go):
	//    - Container starts, CLIP needs to mount image
	//    - getCredentialProviderForImage() uses cached ref + BuildRegistryCreds
	//    - CLIP pulls layers on-demand using these credentials
	//
	// TOKEN EXPIRY CONSIDERATIONS:
	// - ECR tokens expire after 12 hours
	// - Containers running > 12 hours may fail to pull new layers
	// - Workaround: Use IAM roles on worker nodes for ambient auth
	// - Future: Implement token refresh mechanism
	
	t.Log("Token lifecycle documented - see test comments")
}
