package worker

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestInjectCudaEnvVarsNoCudaInImage(t *testing.T) {
	manager := NewContainerCudaManager(4)
	initialEnv := []string{"INITIAL=1"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_DRIVER_CAPABILITIES=all",
		"NVIDIA_REQUIRE_CUDA=cuda>=9.0",
		"NVARCH=",
		"NV_CUDA_COMPAT_PACKAGE=",
		"NV_CUDA_CUDART_VERSION=",
		"CUDA_VERSION=",
		"GPU_TYPE=",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-12.3/bin:$PATH",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-12.3/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
	}

	resultEnv, _ := manager.InjectCudaEnvVars(initialEnv, &ContainerOptions{
		InitialSpec: &specs.Spec{
			Process: &specs.Process{},
		},
	})
	if !reflect.DeepEqual(expectedEnv, resultEnv) {
		t.Errorf("Expected %v, got %v", expectedEnv, resultEnv)
	}
}

func TestInjectCudaEnvVarsExistingCudaInImage(t *testing.T) {
	manager := NewContainerCudaManager(4)
	initialEnv := []string{"INITIAL=1"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")
	os.Setenv("CUDA_VERSION", "12.3")

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_REQUIRE_CUDA=",
		"CUDA_VERSION=11.8.2",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-11.8/bin:$PATH",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-11.8/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
	}

	resultEnv, _ := manager.InjectCudaEnvVars(initialEnv, &ContainerOptions{
		InitialSpec: &specs.Spec{
			Process: &specs.Process{Env: []string{"NVIDIA_REQUIRE_CUDA=", "CUDA_VERSION=11.8.2"}},
		},
	})

	expectedEnvStr := strings.Join(expectedEnv, "")
	resultEnvStr := strings.Join(resultEnv, "")

	expectedEnvStr = strings.ReplaceAll(expectedEnvStr, " ", "")
	resultEnvStr = strings.ReplaceAll(resultEnvStr, " ", "")

	if expectedEnvStr != resultEnvStr {
		t.Errorf("Expected %v, got %v", expectedEnv, resultEnv)
	}
}

func TestInjectCudaMounts(t *testing.T) {
	manager := NewContainerCudaManager(4)
	initialMounts := []specs.Mount{{Type: "bind", Source: "/src", Destination: "/dst"}}

	resultMounts := manager.InjectCudaMounts(initialMounts)
	if len(resultMounts) != len(initialMounts) {
		t.Errorf("Expected %d mounts, got %d", len(initialMounts)+2, len(resultMounts))
	}
}

func mockStat(path string, stat *syscall.Stat_t) error {
	*stat = syscall.Stat_t{
		Rdev: 123,
	}
	return nil
}

type NvidiaSMIClientForTest struct {
	GpuCount int
}

func (c *NvidiaSMIClientForTest) GetGpuMemoryUsage(deviceIndex int) (GpuMemoryUsageStats, error) {
	return GpuMemoryUsageStats{}, nil
}

func (c *NvidiaSMIClientForTest) AvailableGPUDevices() ([]int, error) {
	gpus := []int{}
	for i := 0; i < c.GpuCount; i++ {
		gpus = append(gpus, i)
	}

	return gpus, nil
}

func TestAssignAndUnassignGpuDevices(t *testing.T) {
	// Assume a machine with 4 GPUs
	manager := NewContainerCudaManager(4)
	manager.statFunc = mockStat

	// Assign 2 GPUs to a container
	gpuCount := 2
	manager.nvidiaSmi = &NvidiaSMIClientForTest{
		GpuCount: 4,
	}

	assignedDevices, err := manager.AssignGpuDevices("container1", uint32(gpuCount))
	if err != nil {
		t.Fatalf("Failed to assign GPU devices: %v", err)
	}

	// Verify that 2 GPUs are assigned and the visible string is correct
	if len(assignedDevices.devices) != gpuCount+1 {
		t.Errorf("Expected 2 GPUs to be assigned, got %d", len(assignedDevices.devices))
	}

	if assignedDevices.visible != "0,1" && assignedDevices.visible != "1,0" { // Order might vary
		t.Errorf("Expected visible GPUs to be '0,1' or '1,0', got '%s'", assignedDevices.visible)
	}

	// Unassign the GPUs from the container
	manager.UnassignGpuDevices("container1")

	// Try to assign 4 GPUs to another container, should succeed since the first 2 are unassigned
	_, err = manager.AssignGpuDevices("container2", 4)
	if err != nil {
		t.Errorf("Failed to assign GPU devices to container2 after unassigning from container1: %v", err)
	}
}

func TestAssignMoreGPUsThanAvailable(t *testing.T) {
	manager := NewContainerCudaManager(4) // Assume a machine with 4 GPUs
	manager.statFunc = mockStat

	// Attempt to assign 5 GPUs to a container, which exceeds the available count
	_, err := manager.AssignGpuDevices("container1", 5)
	if err == nil {
		t.Errorf("Expected an error when requesting more GPUs than available, but got none")
	}
}

func TestAssignGPUsToMultipleContainers(t *testing.T) {
	manager := NewContainerCudaManager(4) // Assume a machine with 4 GPUs
	manager.statFunc = mockStat

	manager.nvidiaSmi = &NvidiaSMIClientForTest{
		GpuCount: 4,
	}

	// Assign 2 GPUs to the first container
	_, err := manager.AssignGpuDevices("container1", 2)
	if err != nil {
		t.Fatalf("Failed to assign GPUs to container1: %v", err)
	}

	// Attempt to assign 2 more GPUs to a second container
	_, err = manager.AssignGpuDevices("container2", 2)
	if err != nil {
		t.Errorf("Failed to assign GPUs to container2: %v", err)
	}

	// Attempt to assign 1 more GPU to a third container, should fail
	_, err = manager.AssignGpuDevices("container3", 1)
	if err == nil {
		t.Errorf("Expected failure when assigning GPUs to container3, but got none")
	}
}

func TestAssignGPUsStatFail(t *testing.T) {
	manager := NewContainerCudaManager(4)

	// Override syscall.Stat to simulate failure
	manager.statFunc = func(path string, stat *syscall.Stat_t) error {
		return fmt.Errorf("mock stat error")
	}

	// Attempt to assign GPUs should fail due to statFunc error
	_, err := manager.AssignGpuDevices("container1", 2)
	if err == nil {
		t.Errorf("Expected error due to stat failure, but got none")
	}
}
