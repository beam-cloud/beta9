package worker

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tj/assert"
)

type GPUInfoClientForTest struct {
	GpuCount int
}

func NewContainerNvidiaManagerForTest(gpuCount int) GPUManager {
	manager := NewContainerNvidiaManager(uint32(gpuCount))
	gpuManager := manager.(*ContainerNvidiaManager)
	gpuManager.infoClient = &GPUInfoClientForTest{GpuCount: gpuCount}
	gpuManager.statFunc = mockStat

	return gpuManager
}

func (c *GPUInfoClientForTest) AvailableGPUDevices() ([]int, error) {
	gpus := []int{}
	for i := 0; i < c.GpuCount; i++ {
		gpus = append(gpus, i)
	}

	return gpus, nil
}

func (c *GPUInfoClientForTest) GetGPUMemoryUsage(gpuId int) (GPUMemoryUsageStats, error) {
	return GPUMemoryUsageStats{}, nil
}

func TestInjectNvidiaEnvVarsNoCudaInImage(t *testing.T) {
	manager := NewContainerNvidiaManager(4)
	initialEnv := []string{"INITIAL=1"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")
	os.Setenv("CUDA_HOME", "/usr/local/cuda-12.4")

	defer func() {
		os.Unsetenv("NVIDIA_DRIVER_CAPABILITIES")
		os.Unsetenv("NVIDIA_REQUIRE_CUDA")
		os.Unsetenv("CUDA_HOME")
	}()

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_DRIVER_CAPABILITIES=all",
		"NVIDIA_REQUIRE_CUDA=cuda>=9.0",
		"CUDA_HOME=/usr/local/cuda-12.4",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-12.4/bin",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-12.4/targets/x86_64-linux/lib",
	}

	resultEnv, _ := manager.InjectEnvVars(initialEnv)
	sort.Strings(expectedEnv)
	sort.Strings(resultEnv)
	assert.Equal(t, expectedEnv, resultEnv)
}

func TestInjectNvidiaEnvVarsExistingCudaInImage(t *testing.T) {
	manager := NewContainerNvidiaManager(4)
	initialEnv := []string{"INITIAL=1", "CUDA_VERSION=12.4"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")
	os.Setenv("CUDA_VERSION", "12.4")

	defer func() {
		os.Unsetenv("NVIDIA_DRIVER_CAPABILITIES")
		os.Unsetenv("NVIDIA_REQUIRE_CUDA")
		os.Unsetenv("CUDA_VERSION")
	}()

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_DRIVER_CAPABILITIES=all",
		"NVIDIA_REQUIRE_CUDA=cuda>=9.0",
		"CUDA_VERSION=12.4",
		"CUDA_HOME=/usr/local/cuda-12.4",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-12.4/bin",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-12.4/targets/x86_64-linux/lib",
	}

	resultEnv, _ := manager.InjectEnvVars(initialEnv)
	sort.Strings(expectedEnv)
	sort.Strings(resultEnv)
	assert.Equal(t, expectedEnv, resultEnv)
}

func TestInjectNvidiaMounts(t *testing.T) {
	manager := NewContainerNvidiaManager(4)
	initialMounts := []specs.Mount{{Type: "bind", Source: "/src", Destination: "/dst"}}

	resultMounts := manager.InjectMounts(initialMounts)
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

func TestAssignAndUnassignGPUDevices(t *testing.T) {
	// Assume a machine with 4 GPUs
	manager := NewContainerNvidiaManagerForTest(4)
	// Assign 2 GPUs to a container
	gpuCount := 2

	assignedDevices, err := manager.AssignGPUDevices("container1", uint32(gpuCount))
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
	manager.UnassignGPUDevices("container1")

	// Try to assign 4 GPUs to another container, should succeed since the first 2 are unassigned
	_, err = manager.AssignGPUDevices("container2", 4)
	if err != nil {
		t.Errorf("Failed to assign GPU devices to container2 after unassigning from container1: %v", err)
	}
}

func TestAssignMoreGPUsThanAvailable(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4) // Assume a machine with 4 GPUs
	// manager.statFunc = mockStat

	// Attempt to assign 5 GPUs to a container, which exceeds the available count
	_, err := manager.AssignGPUDevices("container1", 5)
	if err == nil {
		t.Errorf("Expected an error when requesting more GPUs than available, but got none")
	}
}

func TestAssignGPUsToMultipleContainers(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4) // Assume a machine with 4 GPUs

	// Assign 2 GPUs to the first container
	_, err := manager.AssignGPUDevices("container1", 2)
	if err != nil {
		t.Fatalf("Failed to assign GPUs to container1: %v", err)
	}

	// Attempt to assign 2 more GPUs to a second container
	_, err = manager.AssignGPUDevices("container2", 2)
	if err != nil {
		t.Errorf("Failed to assign GPUs to container2: %v", err)
	}

	// Attempt to assign 1 more GPU to a third container, should fail
	_, err = manager.AssignGPUDevices("container3", 1)
	if err == nil {
		t.Errorf("Expected failure when assigning GPUs to container3, but got none")
	}
}

func TestAssignGPUsStatFail(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4)
	gpuManager := manager.(*ContainerNvidiaManager)
	// Override syscall.Stat to simulate failure

	gpuManager.statFunc = func(path string, stat *syscall.Stat_t) error {
		return fmt.Errorf("mock stat error")
	}

	// Attempt to assign GPUs should fail due to statFunc error
	_, err := manager.AssignGPUDevices("container1", 2)
	if err == nil {
		t.Errorf("Expected error due to stat failure, but got none")
	}
}

func TestConcurrentlyAssignGPUDevices(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4)
	numContainers := 4
	iterations := 10
	gpusPerContainer := 1 // Number of GPUs to assign per container

	// seed the random number generator
	rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup

	// channel to collect errors from all iterations
	errCh := make(chan error, numContainers*iterations)

	// map to track currently assigned GPU strings
	currentlyAssignedGPUs := make(map[string]string) // Use assignedRes.String() as key
	var currentlyAssignedGPUsMu sync.Mutex

	for i := 0; i < numContainers; i++ {
		containerID := fmt.Sprintf("concurrent_container_%d", i)
		wg.Add(1)

		go func(id string) {
			defer wg.Done()

			for iter := 0; iter < iterations; iter++ {
				// retry up to maxRetries times
				maxRetries := 3
				attempt := 0
				var assignedRes *AssignedGpuDevices
				var err error

				for {
					assignedRes, err = manager.AssignGPUDevices(id, uint32(gpusPerContainer))
					if err != nil {
						attempt++

						if attempt >= maxRetries {
							errCh <- fmt.Errorf("failed to assign GPU devices for %s on iteration %d after %d retries: %w", id, iter, attempt, err)
							break
						}

						time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)
						continue
					}

					// Use assignedRes.String() to check for duplicate GPU assignment
					key := assignedRes.String()
					currentlyAssignedGPUsMu.Lock()
					if owner, exists := currentlyAssignedGPUs[key]; exists {
						errCh <- fmt.Errorf("GPU assignment %s is already assigned to container %s, but was also assigned to container %s", key, owner, id)
						currentlyAssignedGPUsMu.Unlock()
						manager.UnassignGPUDevices(id)
						break // Exit the loop to avoid infinite retry
					}

					// Mark GPUs as assigned
					currentlyAssignedGPUs[key] = id
					currentlyAssignedGPUsMu.Unlock()
					break
				}
				if err != nil {
					// move on to the next iteration if we couldn't get an assignment
					continue
				}

				// validate that the NVIDIA_VISIBLE_DEVICES string isn't empty
				if assignedRes.visible == "" {
					errCh <- fmt.Errorf("empty NVIDIA_VISIBLE_DEVICES for container %s on iteration %d", id, iter)
					manager.UnassignGPUDevices(id)
					continue
				}

				// retrieve the GPUs assigned to this container - it should be exactly gpusPerContainer
				gpus := manager.GetContainerGPUDevices(id)
				if len(gpus) != gpusPerContainer {
					errCh <- fmt.Errorf("expected %d GPUs for container %s on iteration %d but got %d", gpusPerContainer, id, iter, len(gpus))
					manager.UnassignGPUDevices(id)
					continue
				}

				// hold the assignment for a longer time interval to increase concurrency issues
				time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)

				// unassign the GPU and then wait a short random period before the next iteration.
				manager.UnassignGPUDevices(id)

				// Unmark GPUs as assigned
				currentlyAssignedGPUsMu.Lock()
				delete(currentlyAssignedGPUs, assignedRes.String())
				currentlyAssignedGPUsMu.Unlock()
			}
		}(containerID)
	}

	wg.Wait()
	close(errCh)

	// report any errors encountered during the test
	for err := range errCh {
		t.Error(err)
	}
}
