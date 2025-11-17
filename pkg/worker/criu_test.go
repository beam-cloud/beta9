package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// MockRuntime is a mock implementation of runtime.Runtime for testing
type MockRuntime struct {
	name              string
	checkpointCalled  bool
	restoreCalled     bool
	checkpointError   error
	restoreError      error
	restoreExitCode   int
	capabilities      runtime.Capabilities
}

func NewMockRuntime(name string, caps runtime.Capabilities) *MockRuntime {
	return &MockRuntime{
		name:         name,
		capabilities: caps,
	}
}

func (m *MockRuntime) Name() string {
	return m.name
}

func (m *MockRuntime) Capabilities() runtime.Capabilities {
	return m.capabilities
}

func (m *MockRuntime) Prepare(ctx context.Context, spec *specs.Spec) error {
	return nil
}

func (m *MockRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	return 0, nil
}

func (m *MockRuntime) Exec(ctx context.Context, containerID string, proc specs.Process, opts *runtime.ExecOpts) error {
	return nil
}

func (m *MockRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *runtime.KillOpts) error {
	return nil
}

func (m *MockRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	return nil
}

func (m *MockRuntime) State(ctx context.Context, containerID string) (runtime.State, error) {
	return runtime.State{}, nil
}

func (m *MockRuntime) Events(ctx context.Context, containerID string) (<-chan runtime.Event, error) {
	ch := make(chan runtime.Event)
	close(ch)
	return ch, nil
}

func (m *MockRuntime) Checkpoint(ctx context.Context, containerID string, opts *runtime.CheckpointOpts) error {
	m.checkpointCalled = true
	if m.checkpointError != nil {
		return m.checkpointError
	}
	// Create checkpoint directory
	if opts != nil && opts.ImagePath != "" {
		return os.MkdirAll(opts.ImagePath, 0755)
	}
	return nil
}

func (m *MockRuntime) Restore(ctx context.Context, containerID string, opts *runtime.RestoreOpts) (int, error) {
	m.restoreCalled = true
	if m.restoreError != nil {
		return m.restoreExitCode, m.restoreError
	}
	if opts != nil && opts.Started != nil {
		opts.Started <- 12345
	}
	return m.restoreExitCode, nil
}

func (m *MockRuntime) Close() error {
	return nil
}

// Reset clears the mock runtime state flags for reuse in subtests
func (m *MockRuntime) Reset() {
	m.checkpointCalled = false
	m.restoreCalled = false
}

// TestNvidiaCRIUManager tests NVIDIA CRIU manager with different runtimes
func TestNvidiaCRIUManager(t *testing.T) {
	if os.Getenv("SKIP_CRIU_TESTS") == "1" {
		t.Skip("Skipping CRIU tests")
	}

	testCases := []struct {
		name         string
		runtimeName  string
		capabilities runtime.Capabilities
		extraTests   func(t *testing.T, manager CRIUManager, mockRuntime *MockRuntime, tmpDir string)
	}{
		{
			name:        "runc",
			runtimeName: "runc",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
			},
		},
		{
			name:        "gvisor",
			runtimeName: "gvisor",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
			},
			extraTests: func(t *testing.T, manager CRIUManager, mockRuntime *MockRuntime, tmpDir string) {
				t.Run("checkpoint with CUDA support", func(t *testing.T) {
					// Reset mock state to ensure this test's assertions are independent
					mockRuntime.Reset()

					request := &types.ContainerRequest{
						ContainerId: "cuda-container",
						Gpu:         "nvidia-tesla-v100",
						GpuCount:    1,
					}

					checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "cuda-checkpoint", request)
					if err != nil {
						t.Errorf("CreateCheckpoint with CUDA support failed: %v", err)
					}

					if !mockRuntime.checkpointCalled {
						t.Error("Expected Checkpoint to be called for CUDA container")
					}

					if checkpointPath == "" {
						t.Error("Expected non-empty checkpoint path for CUDA container")
					}
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("criu-%s-test-*", tc.runtimeName))
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			config := types.CRIUConfig{
				Storage: types.CheckpointStorageConfig{
					MountPath: tmpDir,
				},
			}

			manager, err := InitializeNvidiaCRIU(context.Background(), config)
			if err != nil {
				t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
			}

			mockRuntime := NewMockRuntime(tc.runtimeName, tc.capabilities)

			t.Run("CreateCheckpoint", func(t *testing.T) {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-1", tc.runtimeName),
				}

				checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "checkpoint-1", request)
				if err != nil {
					t.Errorf("CreateCheckpoint failed: %v", err)
				}

				if !mockRuntime.checkpointCalled {
					t.Error("Expected Checkpoint to be called on runtime")
				}

				if checkpointPath == "" {
					t.Error("Expected non-empty checkpoint path")
				}
			})

			t.Run("RestoreCheckpoint", func(t *testing.T) {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-2", tc.runtimeName),
				}

				checkpoint := &types.Checkpoint{
					CheckpointId: "checkpoint-1",
				}

				// Create checkpoint directory
				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:    request,
					checkpoint: checkpoint,
					configPath: filepath.Join(tmpDir, "config.json"),
					started:    make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if !mockRuntime.restoreCalled {
					t.Error("Expected Restore to be called on runtime")
				}

				if exitCode != 0 {
					t.Errorf("Expected exit code 0, got %d", exitCode)
				}
			})

			// Run any extra tests specific to this runtime
			if tc.extraTests != nil {
				tc.extraTests(t, manager, mockRuntime, tmpDir)
			}
		})
	}
}

// TestCedanaCRIUManagerRuntimeCheck tests that Cedana only works with runc
func TestCedanaCRIUManagerRuntimeCheck(t *testing.T) {
	if os.Getenv("SKIP_CRIU_TESTS") == "1" {
		t.Skip("Skipping CRIU tests")
	}

	// Skip if cedana is not available
	// Note: We can't easily test Cedana without the daemon running
	// This test just checks runtime compatibility checking
	t.Skip("Skipping Cedana test - requires daemon")

}

// TestCheckpointRestoreErrorHandling tests error handling in checkpoint/restore
func TestCheckpointRestoreErrorHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "criu-error-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := types.CRIUConfig{
		Storage: types.CheckpointStorageConfig{
			MountPath: tmpDir,
		},
	}

	manager, err := InitializeNvidiaCRIU(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
	}

	t.Run("checkpoint failure", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		mockRuntime.checkpointError = fmt.Errorf("checkpoint failed")

		request := &types.ContainerRequest{
			ContainerId: "failing-container",
		}

		_, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "failing-checkpoint", request)
		if err == nil {
			t.Error("Expected error when checkpoint fails")
		}
	})

	t.Run("restore failure", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		mockRuntime.restoreError = fmt.Errorf("restore failed")
		mockRuntime.restoreExitCode = -1

		request := &types.ContainerRequest{
			ContainerId: "failing-container",
		}

		checkpoint := &types.Checkpoint{
			CheckpointId: "failing-checkpoint",
		}

		// Create checkpoint directory
		checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
		os.MkdirAll(checkpointPath, 0755)

		opts := &RestoreOpts{
			request:    request,
			checkpoint: checkpoint,
			configPath: filepath.Join(tmpDir, "config.json"),
		}

		exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
		if err == nil {
			t.Error("Expected error when restore fails")
		}
		if exitCode != -1 {
			t.Errorf("Expected exit code -1, got %d", exitCode)
		}
	})

	t.Run("CRIU restore specific error", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		// Simulate CRIU restore failure
		mockRuntime.restoreError = &ErrCRIURestoreFailed{
			Stderr: "criu failed: type RESTORE",
		}
		mockRuntime.restoreExitCode = -1

		request := &types.ContainerRequest{
			ContainerId: "criu-failing-container",
		}

		checkpoint := &types.Checkpoint{
			CheckpointId: "criu-failing-checkpoint",
		}

		// Create checkpoint directory
		checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
		os.MkdirAll(checkpointPath, 0755)

		opts := &RestoreOpts{
			request:    request,
			checkpoint: checkpoint,
			configPath: filepath.Join(tmpDir, "config.json"),
		}

		_, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
		if !IsCRIURestoreError(err) {
			t.Error("Expected CRIU restore error")
		}
	})
}

// TestRuntimeCompatibility tests that the CRIU manager works with different runtimes
func TestRuntimeCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "criu-compat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := types.CRIUConfig{
		Storage: types.CheckpointStorageConfig{
			MountPath: tmpDir,
		},
	}

	manager, err := InitializeNvidiaCRIU(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
	}

	runtimes := []struct {
		name         string
		capabilities runtime.Capabilities
	}{
		{
			name: "runc",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
				CDI:               true,
			},
		},
		{
			name: "gvisor",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
				CDI:               true,
			},
		},
	}

	for _, rtInfo := range runtimes {
		t.Run(fmt.Sprintf("checkpoint_with_%s", rtInfo.name), func(t *testing.T) {
			mockRuntime := NewMockRuntime(rtInfo.name, rtInfo.capabilities)

			request := &types.ContainerRequest{
				ContainerId: fmt.Sprintf("%s-container", rtInfo.name),
			}

			checkpointID := fmt.Sprintf("%s-checkpoint", rtInfo.name)
			checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, checkpointID, request)
			if err != nil {
				t.Errorf("CreateCheckpoint with %s failed: %v", rtInfo.name, err)
			}

			if !mockRuntime.checkpointCalled {
				t.Errorf("Expected Checkpoint to be called for %s", rtInfo.name)
			}

			if checkpointPath == "" {
				t.Errorf("Expected non-empty checkpoint path for %s", rtInfo.name)
			}

			// Verify checkpoint directory was created
			if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
				t.Errorf("Checkpoint directory not created for %s", rtInfo.name)
			}
		})
	}
}
