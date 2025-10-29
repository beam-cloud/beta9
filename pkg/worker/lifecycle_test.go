package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV2ImageEnvironmentFlow tests the complete flow of environment variable handling for v2 images
func TestV2ImageEnvironmentFlow(t *testing.T) {
	// Create a test config
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			ClipVersion: 2,
		},
		Worker: types.WorkerConfig{},
	}

	// Create a mock skopeo client that returns ubuntu:20.04 metadata with PATH
	mockSkopeo := &mockSkopeoClient{
		inspectFunc: func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
			return common.ImageMetadata{
				Name:         "ubuntu",
				Architecture: "amd64",
				Os:           "linux",
				Config: &common.ImageConfig{
					Env: []string{
						"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
						"UBUNTU_CODENAME=focal",
					},
					WorkingDir: "/",
					Cmd:        []string{"/bin/bash"},
				},
			}, nil
		},
	}

	// Create a test worker with mock dependencies
	worker := &Worker{
		config:             config,
		imageMountPath:     "/tmp/test-images",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		imageClient: &ImageClient{
			skopeoClient: mockSkopeo,
		},
		runcServer: &RunCServer{
			baseConfigSpec: getTestBaseSpec(),
		},
	}

	// Create a test container request for a v2 image
	sourceImage := "docker.io/library/ubuntu:20.04"
	request := &types.ContainerRequest{
		ContainerId: "test-container-123",
		ImageId:     "test-image-456",
		BuildOptions: types.BuildOptions{
			SourceImage:      &sourceImage,
			SourceImageCreds: "",
		},
		Env: []string{
			"BETA9_TOKEN=test-token",
			"STUB_ID=test-stub",
		},
	}

	// Step 1: Read bundle config (should derive from source image for v2)
	t.Run("ReadBundleConfig", func(t *testing.T) {
		initialSpec, err := worker.readBundleConfig(request)
		require.NoError(t, err)
		require.NotNil(t, initialSpec, "InitialSpec should not be nil for v2 images")

		t.Logf("InitialSpec.Process.Env: %v", initialSpec.Process.Env)

		// Check that PATH is in the initial spec
		hasPath := false
		for _, env := range initialSpec.Process.Env {
			if containsStr(env, "PATH=") {
				hasPath = true
				t.Logf("Found PATH in InitialSpec: %s", env)
				break
			}
		}
		assert.True(t, hasPath, "InitialSpec should contain PATH from base image")
	})

	// Step 2: Create container options with initial spec
	t.Run("ContainerOptions", func(t *testing.T) {
		initialSpec, _ := worker.readBundleConfig(request)
		
		options := &ContainerOptions{
			BundlePath:   "/tmp/test-bundle",
			HostBindPort: 8001,
			BindPorts:    []int{8001},
			InitialSpec:  initialSpec,
		}

		// Step 3: Get container environment
		env := worker.getContainerEnvironment(request, options)
		
		t.Logf("Assembled environment (%d vars):", len(env))
		for i, e := range env {
			t.Logf("  [%d] %s", i, e)
		}

		// Check that PATH is in the assembled environment
		hasPath := false
		pathValue := ""
		for _, envVar := range env {
			if containsStr(envVar, "PATH=") {
				hasPath = true
				pathValue = envVar
				break
			}
		}
		assert.True(t, hasPath, "Assembled environment should contain PATH")
		if hasPath {
			t.Logf("✅ PATH found: %s", pathValue)
		} else {
			t.Errorf("❌ PATH missing from assembled environment")
		}

		// Check order: InitialSpec vars should be first
		assert.True(t, len(env) > 0, "Environment should not be empty")
		// First few should be from InitialSpec (PATH, UBUNTU_CODENAME)
		// Followed by request env (BETA9_TOKEN, STUB_ID)
		// Followed by system env (BIND_PORT, etc)
	})

	// Step 4: Generate full spec from request
	t.Run("SpecFromRequest", func(t *testing.T) {
		initialSpec, _ := worker.readBundleConfig(request)
		
		options := &ContainerOptions{
			BundlePath:   "/tmp/test-bundle",
			HostBindPort: 8001,
			BindPorts:    []int{8001},
			InitialSpec:  initialSpec,
		}

		spec, err := worker.specFromRequest(request, options)
		require.NoError(t, err)
		require.NotNil(t, spec)

		t.Logf("Final spec.Process.Env (%d vars):", len(spec.Process.Env))
		for i, e := range spec.Process.Env {
			t.Logf("  [%d] %s", i, e)
		}

		// Check that PATH is in the final spec
		hasPath := false
		pathValue := ""
		for _, envVar := range spec.Process.Env {
			if containsStr(envVar, "PATH=") {
				hasPath = true
				pathValue = envVar
				break
			}
		}
		require.True(t, hasPath, "Final spec should contain PATH from base image")
		t.Logf("✅ Final spec has PATH: %s", pathValue)

		// Verify the expected env vars are present
		envMap := make(map[string]bool)
		for _, e := range spec.Process.Env {
			envMap[e] = true
		}

		assert.Contains(t, envMap, "BETA9_TOKEN=test-token", "Should have request env")
		assert.True(t, hasPath, "Should have PATH from base image")
	})

	// Step 5: Test writing config.json and verify PATH is persisted
	t.Run("WriteConfigJSON", func(t *testing.T) {
		initialSpec, _ := worker.readBundleConfig(request)
		
		options := &ContainerOptions{
			BundlePath:   "/tmp/test-bundle",
			HostBindPort: 8001,
			BindPorts:    []int{8001},
			InitialSpec:  initialSpec,
		}

		spec, err := worker.specFromRequest(request, options)
		require.NoError(t, err)

		// Write spec to temp file (simulating config.json write)
		tmpDir, err := os.MkdirTemp("", "test-config-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		configPath := tmpDir + "/config.json"
		configBytes, err := json.MarshalIndent(spec, "", "  ")
		require.NoError(t, err)

		err = os.WriteFile(configPath, configBytes, 0644)
		require.NoError(t, err)

		t.Logf("Wrote config.json to: %s", configPath)

		// Read it back and verify PATH is there
		readBytes, err := os.ReadFile(configPath)
		require.NoError(t, err)

		var readSpec map[string]interface{}
		err = json.Unmarshal(readBytes, &readSpec)
		require.NoError(t, err)

		process := readSpec["process"].(map[string]interface{})
		env := process["env"].([]interface{})

		t.Logf("Read back %d env vars from config.json", len(env))

		hasPath := false
		for _, e := range env {
			envStr := e.(string)
			if containsStr(envStr, "PATH=") {
				hasPath = true
				t.Logf("✅ PATH persisted in config.json: %s", envStr)
				break
			}
		}
		require.True(t, hasPath, "PATH should be persisted in config.json")
	})
}

// TestV2ImageEnvironmentFlow_NonBuildContainer tests that non-build containers (sandboxes, deployments)
// can retrieve the source image reference from cache and get proper PATH
func TestV2ImageEnvironmentFlow_NonBuildContainer(t *testing.T) {
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			ClipVersion: 2,
		},
		Worker: types.WorkerConfig{},
	}

	mockSkopeo := &mockSkopeoClient{
		inspectFunc: func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
			t.Logf("Inspecting image: %s", image)
			return common.ImageMetadata{
				Name:         "ubuntu",
				Architecture: "amd64",
				Os:           "linux",
				Config: &common.ImageConfig{
					Env: []string{
						"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
						"HOME=/root",
					},
					WorkingDir: "/",
				},
			}, nil
		},
	}

	imageClient := &ImageClient{
		skopeoClient: mockSkopeo,
		config:       config,
		v2ImageRefs:  common.NewSafeMap[string](),
	}

	worker := &Worker{
		config:             config,
		imageMountPath:     "/tmp/test-images",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		imageClient:        imageClient,
		runcServer: &RunCServer{
			baseConfigSpec: getTestBaseSpec(),
		},
	}

	// Simulate a build that caches the source image reference
	imageId := "v2-image-abc123"
	sourceImage := "docker.io/library/ubuntu:20.04"
	imageClient.v2ImageRefs.Set(imageId, sourceImage)

	// Create a non-build container request (like a sandbox)
	// Note: BuildOptions.SourceImage is NOT set - this simulates a regular container run
	request := &types.ContainerRequest{
		ContainerId: "sandbox-xyz",
		ImageId:     imageId,
		Env: []string{
			"USER_VAR=test",
		},
	}

	t.Run("NonBuildContainer_HasPATH", func(t *testing.T) {
		initialSpec, err := worker.readBundleConfig(request)
		require.NoError(t, err)
		require.NotNil(t, initialSpec, "Should derive spec from cached source image")

		// Verify PATH is present
		hasPath := false
		for _, env := range initialSpec.Process.Env {
			if containsStr(env, "PATH=") {
				hasPath = true
				t.Logf("✅ PATH retrieved for non-build container: %s", env)
				break
			}
		}
		assert.True(t, hasPath, "Non-build container should have PATH from base image")

		// Test the full flow
		options := &ContainerOptions{
			BundlePath:   "/tmp/test-bundle",
			HostBindPort: 8001,
			BindPorts:    []int{8001},
			InitialSpec:  initialSpec,
		}

		env := worker.getContainerEnvironment(request, options)
		
		hasPathInEnv := false
		for _, e := range env {
			if containsStr(e, "PATH=") {
				hasPathInEnv = true
				t.Logf("✅ PATH in final environment: %s", e)
				break
			}
		}
		assert.True(t, hasPathInEnv, "Final environment should contain PATH")
	})
}

// Helper function
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

// Mock skopeo client for testing
type mockSkopeoClient struct {
	inspectFunc          func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error)
	inspectSizeFunc      func(ctx context.Context, image string, creds string) (int64, error)
	copyFunc             func(ctx context.Context, source, dest, creds string, logger *slog.Logger) error
}

func (m *mockSkopeoClient) Inspect(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
	if m.inspectFunc != nil {
		return m.inspectFunc(ctx, image, creds, logger)
	}
	return common.ImageMetadata{}, nil
}

func (m *mockSkopeoClient) InspectSizeInBytes(ctx context.Context, image string, creds string) (int64, error) {
	if m.inspectSizeFunc != nil {
		return m.inspectSizeFunc(ctx, image, creds)
	}
	return 0, nil
}

func (m *mockSkopeoClient) Copy(ctx context.Context, source, dest, creds string, logger *slog.Logger) error {
	if m.copyFunc != nil {
		return m.copyFunc(ctx, source, dest, creds, logger)
	}
	return nil
}

// Get a base test spec
func getTestBaseSpec() specs.Spec {
	return specs.Spec{
		Version: "1.0.2-dev",
		Process: &specs.Process{
			Terminal: false,
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Args: []string{"sh"},
			Env: []string{
				"TERM=xterm",
			},
			Cwd: "/workspace",
		},
		Root: &specs.Root{
			Path:     "/",
			Readonly: false,
		},
		Hostname: "beta9",
		Mounts:   []specs.Mount{},
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{},
		},
	}
}
