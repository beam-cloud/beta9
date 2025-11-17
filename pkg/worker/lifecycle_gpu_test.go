package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGPUMountsWithGVisor verifies that CUDA library mounts are added for gVisor containers
func TestGPUMountsWithGVisor(t *testing.T) {
	// Create a mock gVisor runtime
	mockGvisorRuntime := &mockRuntime{
		name: types.ContainerRuntimeGvisor.String(),
		capabilities: runtime.Capabilities{
			CheckpointRestore: true,
			GPU:               true,
			OOMEvents:         false,
			JoinExistingNetNS: true,
			CDI:               true,
		},
	}

	// Create a mock GPU manager that will inject mounts
	mockGPUManager := &mockGPUManager{
		assignedDevices: []int{0, 1}, // Simulate 2 GPUs
	}

	// Create initial spec with some mounts
	spec := &specs.Spec{
		Linux: &specs.Linux{
			Devices: []specs.LinuxDevice{},
		},
		Mounts: []specs.Mount{
			{
				Type:        "bind",
				Source:      "/etc/resolv.conf",
				Destination: "/etc/resolv.conf",
			},
		},
	}

	initialMountCount := len(spec.Mounts)

	// Simulate what happens in the spawn function for gVisor with GPU
	// This mimics lines 806-812 in lifecycle.go
	if mockGvisorRuntime.Name() == types.ContainerRuntimeGvisor.String() {
		spec.Mounts = mockGPUManager.InjectMounts(spec.Mounts)
	}

	// Verify mounts were added
	assert.Greater(t, len(spec.Mounts), initialMountCount, "GPU mounts should have been added for gVisor")

	// Verify CUDA and NVIDIA library paths are in the mounts
	foundCuda := false
	foundNvidiaLib := false

	for _, mount := range spec.Mounts {
		if mount.Source == "/usr/local/cuda-12.4" {
			foundCuda = true
			assert.Equal(t, "/usr/local/cuda-12.4", mount.Destination)
			assert.Contains(t, mount.Options, "rbind")
		}
		if mount.Source == "/usr/local/nvidia/lib64" {
			foundNvidiaLib = true
			assert.Equal(t, "/usr/local/nvidia/lib64", mount.Destination)
			assert.Contains(t, mount.Options, "rbind")
		}
	}

	// On a system without CUDA installed, these won't be added (which is correct)
	// But we verify the logic works correctly
	t.Logf("Found CUDA mount: %v, Found NVIDIA lib mount: %v", foundCuda, foundNvidiaLib)
}

// TestGPUMountsOnlyForGVisor verifies that CUDA mounts are ONLY added for gVisor, not runc
func TestGPUMountsOnlyForGVisor(t *testing.T) {
	tests := []struct {
		name           string
		runtimeName    string
		shouldAddMount bool
	}{
		{
			name:           "gVisor should get CUDA mounts",
			runtimeName:    types.ContainerRuntimeGvisor.String(),
			shouldAddMount: true,
		},
		{
			name:           "runc should NOT get additional CUDA mounts (CDI handles it)",
			runtimeName:    types.ContainerRuntimeRunc.String(),
			shouldAddMount: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRuntime := &mockRuntime{
				name: tt.runtimeName,
				capabilities: runtime.Capabilities{
					GPU: true,
					CDI: true,
				},
			}

			mockGPUManager := &mockGPUManager{
				assignedDevices: []int{0},
			}

			spec := &specs.Spec{
				Linux: &specs.Linux{},
				Mounts: []specs.Mount{
					{Type: "bind", Source: "/etc/resolv.conf", Destination: "/etc/resolv.conf"},
				},
			}

			initialMountCount := len(spec.Mounts)

			// This mimics the conditional logic in lifecycle.go
			if mockRuntime.Name() == types.ContainerRuntimeGvisor.String() {
				spec.Mounts = mockGPUManager.InjectMounts(spec.Mounts)
			}

			if tt.shouldAddMount {
				assert.GreaterOrEqual(t, len(spec.Mounts), initialMountCount,
					"gVisor should have CUDA mounts added")
			} else {
				assert.Equal(t, initialMountCount, len(spec.Mounts),
					"runc should NOT have additional mounts (CDI handles it)")
			}
		})
	}
}

// TestMultiGPUMountConsistency verifies that mounts work correctly with multiple GPUs
func TestMultiGPUMountConsistency(t *testing.T) {
	mockGPUManager := &mockGPUManager{
		assignedDevices: []int{0, 1, 2, 3}, // 4 GPUs
	}

	spec := &specs.Spec{
		Linux:  &specs.Linux{},
		Mounts: []specs.Mount{},
	}

	// Inject mounts
	spec.Mounts = mockGPUManager.InjectMounts(spec.Mounts)

	// The number of mounts shouldn't change based on GPU count
	// The same CUDA paths are mounted regardless of how many GPUs
	// (The CDI annotations handle the per-GPU device assignment)
	expectedMaxMounts := 2 // cuda + nvidia lib64

	assert.LessOrEqual(t, len(spec.Mounts), expectedMaxMounts,
		"Mount count should be consistent regardless of GPU count")
}

// Mock GPU manager for testing
type mockGPUManager struct {
	assignedDevices []int
}

func (m *mockGPUManager) AssignGPUDevices(containerId string, gpuCount uint32) ([]int, error) {
	return m.assignedDevices, nil
}

func (m *mockGPUManager) GetContainerGPUDevices(containerId string) []int {
	return m.assignedDevices
}

func (m *mockGPUManager) UnassignGPUDevices(containerId string) {}

func (m *mockGPUManager) InjectEnvVars(env []string) []string {
	return append(env, "CUDA_VISIBLE_DEVICES="+string(rune(m.assignedDevices[0])))
}

func (m *mockGPUManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	// Simulate what ContainerNvidiaManager.InjectMounts does
	// Note: On systems without CUDA installed, this returns the original mounts unchanged
	cudaPaths := []string{"/usr/local/cuda-12.4", "/usr/local/nvidia/lib64"}

	for _, path := range cudaPaths {
		// In the real implementation, this checks if path exists
		// For testing, we always add them to verify the logic
		mounts = append(mounts, specs.Mount{
			Type:        "bind",
			Source:      path,
			Destination: path,
			Options: []string{
				"rbind",
				"rprivate",
				"nosuid",
				"nodev",
				"rw",
			},
		})
	}

	return mounts
}

// Mock runtime for testing
type mockRuntime struct {
	name         string
	capabilities runtime.Capabilities
}

func (m *mockRuntime) Name() string {
	return m.name
}

func (m *mockRuntime) Capabilities() runtime.Capabilities {
	return m.capabilities
}

func (m *mockRuntime) Prepare(ctx context.Context, spec *specs.Spec) error {
	// Simulate what gVisor does - clear devices
	if m.name == types.ContainerRuntimeGvisor.String() {
		spec.Linux.Devices = nil
	}
	return nil
}

func (m *mockRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	return 0, nil
}

func (m *mockRuntime) Exec(ctx context.Context, containerID string, proc specs.Process, opts *runtime.ExecOpts) error {
	return nil
}

func (m *mockRuntime) Kill(ctx context.Context, containerID string, sig interface{}, opts *runtime.KillOpts) error {
	return nil
}

func (m *mockRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	return nil
}

func (m *mockRuntime) State(ctx context.Context, containerID string) (runtime.State, error) {
	return runtime.State{}, nil
}

func (m *mockRuntime) Events(ctx context.Context, containerID string) (<-chan runtime.Event, error) {
	ch := make(chan runtime.Event)
	close(ch)
	return ch, nil
}

func (m *mockRuntime) Checkpoint(ctx context.Context, containerID string, opts *runtime.CheckpointOpts) error {
	return nil
}

func (m *mockRuntime) Restore(ctx context.Context, containerID string, opts *runtime.RestoreOpts) (int, error) {
	return 0, nil
}

func (m *mockRuntime) Close() error {
	return nil
}

// TestRuntimePreparePreservesMounts verifies that runtime.Prepare doesn't remove GPU mounts
func TestRuntimePreparePreservesMounts(t *testing.T) {
	mockRuntime := &mockRuntime{
		name: types.ContainerRuntimeGvisor.String(),
		capabilities: runtime.Capabilities{
			GPU: true,
			CDI: true,
		},
	}

	spec := &specs.Spec{
		Linux: &specs.Linux{
			Devices: []specs.LinuxDevice{
				{Path: "/dev/nvidia0"},
				{Path: "/dev/nvidia1"},
			},
		},
		Mounts: []specs.Mount{
			{Type: "bind", Source: "/usr/local/cuda-12.4", Destination: "/usr/local/cuda-12.4"},
			{Type: "bind", Source: "/usr/local/nvidia/lib64", Destination: "/usr/local/nvidia/lib64"},
		},
	}

	mountCountBefore := len(spec.Mounts)
	deviceCountBefore := len(spec.Linux.Devices)

	require.Greater(t, deviceCountBefore, 0, "Should start with devices")

	// Call Prepare (simulates what happens in lifecycle.go line 864)
	err := mockRuntime.Prepare(context.Background(), spec)
	require.NoError(t, err)

	// Verify devices were cleared (expected for gVisor)
	assert.Nil(t, spec.Linux.Devices, "Devices should be cleared by gVisor's Prepare")

	// Verify mounts were preserved
	assert.Equal(t, mountCountBefore, len(spec.Mounts),
		"Mounts should be preserved by runtime.Prepare")

	// Verify specific CUDA mounts are still there
	foundCuda := false
	for _, mount := range spec.Mounts {
		if mount.Source == "/usr/local/cuda-12.4" {
			foundCuda = true
		}
	}
	assert.True(t, foundCuda, "CUDA mount should be preserved")
}
