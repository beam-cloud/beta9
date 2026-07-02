package worker

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tj/assert"

	"gvisor.dev/gvisor/pkg/sync"
)

type GPUInfoClientForTest struct {
	GpuCount int
	UUIDs    map[int]string
}

func NewContainerNvidiaManagerForTest(gpuCount int) GPUManager {
	gpuManager := &ContainerNvidiaManager{
		gpuAllocationMap: common.NewSafeMap[[]int](),
		gpuCount:         uint32(gpuCount),
		mu:               sync.Mutex{},
		statFunc:         syscall.Stat,
		infoClient:       &NvidiaInfoClient{},
	}

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

func (c *GPUInfoClientForTest) DeviceUUID(deviceIndex int) (string, bool) {
	if c.UUIDs != nil {
		uuid, ok := c.UUIDs[deviceIndex]
		return uuid, ok
	}
	if deviceIndex < 0 || deviceIndex >= c.GpuCount {
		return "", false
	}
	return fmt.Sprintf("GPU-%d", deviceIndex), true
}

func (c *GPUInfoClientForTest) GetGPUMemoryUsage(gpuId int) (GPUMemoryUsageStats, error) {
	return GPUMemoryUsageStats{}, nil
}

func TestEnsureNvidiaCDIConfigFallsBackToRuntimeDir(t *testing.T) {
	originalPaths := nvidiaCDIConfigPaths
	originalGenerate := runNvidiaCTKCDIGenerate
	originalConfigure := configureNvidiaCDICache
	originalResolvable := nvidiaCDIDeviceResolvable
	t.Cleanup(func() {
		nvidiaCDIConfigPaths = originalPaths
		runNvidiaCTKCDIGenerate = originalGenerate
		configureNvidiaCDICache = originalConfigure
		nvidiaCDIDeviceResolvable = originalResolvable
	})

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "beam", "cdi", "nvidia.yaml")
	secondPath := filepath.Join(dir, "run", "cdi", "nvidia.yaml")
	nvidiaCDIConfigPaths = []string{firstPath, secondPath}

	calls := []string{}
	configureCalls := []string{}
	runNvidiaCTKCDIGenerate = func(outputPath string) ([]byte, error) {
		calls = append(calls, outputPath)
		if outputPath == firstPath {
			return []byte("first path failed"), errors.New("exit status 1")
		}
		return writeTestNvidiaCDISpec(outputPath)
	}
	configureNvidiaCDICache = func(specPath string) error {
		configureCalls = append(configureCalls, specPath)
		return nil
	}
	nvidiaCDIDeviceResolvable = func() bool {
		return true
	}

	if err := ensureNvidiaCDIConfig(); err != nil {
		t.Fatalf("ensureNvidiaCDIConfig() error = %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("nvidia-ctk calls = %v, want both paths", calls)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("fallback cdi config was not written: %v", err)
	}
	if len(configureCalls) != 1 || configureCalls[0] != secondPath {
		t.Fatalf("configure calls = %v, want only fallback path", configureCalls)
	}
}

func TestEnsureNvidiaCDIConfigUsesBeamRuntimeDir(t *testing.T) {
	originalPaths := nvidiaCDIConfigPaths
	originalGenerate := runNvidiaCTKCDIGenerate
	originalConfigure := configureNvidiaCDICache
	originalResolvable := nvidiaCDIDeviceResolvable
	t.Cleanup(func() {
		nvidiaCDIConfigPaths = originalPaths
		runNvidiaCTKCDIGenerate = originalGenerate
		configureNvidiaCDICache = originalConfigure
		nvidiaCDIDeviceResolvable = originalResolvable
	})

	dir := t.TempDir()
	beamPath := filepath.Join(dir, "beam", "cdi", "nvidia.yaml")
	standardPath := filepath.Join(dir, "run", "cdi", "nvidia.yaml")
	nvidiaCDIConfigPaths = []string{beamPath, standardPath}

	calls := []string{}
	configureCalls := []string{}
	runNvidiaCTKCDIGenerate = func(outputPath string) ([]byte, error) {
		calls = append(calls, outputPath)
		return writeTestNvidiaCDISpec(outputPath)
	}
	configureNvidiaCDICache = func(specPath string) error {
		configureCalls = append(configureCalls, specPath)
		return nil
	}
	nvidiaCDIDeviceResolvable = func() bool {
		return true
	}

	if err := ensureNvidiaCDIConfig(); err != nil {
		t.Fatalf("ensureNvidiaCDIConfig() error = %v", err)
	}
	if len(calls) != 1 || calls[0] != beamPath {
		t.Fatalf("nvidia-ctk calls = %v, want only Beam runtime path", calls)
	}
	if len(configureCalls) != 1 || configureCalls[0] != beamPath {
		t.Fatalf("configure calls = %v, want only Beam runtime path", configureCalls)
	}
	if _, err := os.Stat(standardPath); !os.IsNotExist(err) {
		t.Fatalf("standard CDI path should be untouched, stat err = %v", err)
	}
}

func TestEnsureNvidiaCDIConfigFallsBackWhenGeneratedDeviceIsUnresolvable(t *testing.T) {
	originalPaths := nvidiaCDIConfigPaths
	originalGenerate := runNvidiaCTKCDIGenerate
	originalConfigure := configureNvidiaCDICache
	originalResolvable := nvidiaCDIDeviceResolvable
	t.Cleanup(func() {
		nvidiaCDIConfigPaths = originalPaths
		runNvidiaCTKCDIGenerate = originalGenerate
		configureNvidiaCDICache = originalConfigure
		nvidiaCDIDeviceResolvable = originalResolvable
	})

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "beam", "cdi", "nvidia.yaml")
	secondPath := filepath.Join(dir, "run", "cdi", "nvidia.yaml")
	nvidiaCDIConfigPaths = []string{firstPath, secondPath}

	calls := []string{}
	runNvidiaCTKCDIGenerate = func(outputPath string) ([]byte, error) {
		calls = append(calls, outputPath)
		return writeTestNvidiaCDISpec(outputPath)
	}
	resolveCalls := 0
	configureNvidiaCDICache = func(specPath string) error {
		return nil
	}
	nvidiaCDIDeviceResolvable = func() bool {
		resolveCalls++
		return resolveCalls > 1
	}

	if err := ensureNvidiaCDIConfig(); err != nil {
		t.Fatalf("ensureNvidiaCDIConfig() error = %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("nvidia-ctk calls = %v, want retry after unresolvable generated spec", calls)
	}
}

func TestSanitizeNvidiaCDIConfigRemovesIncompatibleGVisorEdits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nvidia.yaml")
	if _, err := writeTestNvidiaCDISpec(path); err != nil {
		t.Fatalf("write test spec: %v", err)
	}

	if err := sanitizeNvidiaCDIConfig(path); err != nil {
		t.Fatalf("sanitizeNvidiaCDIConfig() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sanitized spec: %v", err)
	}
	spec := string(data)
	if strings.Contains(spec, "/run/nvidia-persistenced/socket") {
		t.Fatalf("persistenced socket mount was not removed:\n%s", spec)
	}
	if strings.Contains(spec, "update-ldcache") {
		t.Fatalf("ldcache hook was not removed:\n%s", spec)
	}
	if !strings.Contains(spec, "/usr/bin/nvidia-persistenced") {
		t.Fatalf("non-socket NVIDIA mount should remain:\n%s", spec)
	}
	if !strings.Contains(spec, "/dev/nvidia0") {
		t.Fatalf("GPU device node should remain:\n%s", spec)
	}
	if !strings.Contains(spec, "create-symlinks") {
		t.Fatalf("compatible NVIDIA CDI hook should remain:\n%s", spec)
	}
}

func writeTestNvidiaCDISpec(path string) ([]byte, error) {
	spec := []byte(`---
cdiVersion: 0.7.0
kind: nvidia.com/gpu
devices:
  - name: "0"
    containerEdits:
      deviceNodes:
        - path: /dev/nvidia0
          major: 195
          minor: 0
          fileMode: 438
          permissions: rwm
      mounts:
        - hostPath: /run/nvidia-persistenced/socket
          containerPath: /run/nvidia-persistenced/socket
        - hostPath: /usr/bin/nvidia-smi
          containerPath: /usr/bin/nvidia-smi
      hooks:
        - hookName: createContainer
          path: /usr/bin/nvidia-cdi-hook
          args:
            - nvidia-cdi-hook
            - update-ldcache
            - --folder
            - /usr/lib/x86_64-linux-gnu
        - hookName: createContainer
          path: /usr/bin/nvidia-cdi-hook
          args:
            - nvidia-cdi-hook
            - create-symlinks
            - --link
            - ../libGLX_nvidia.so.0::/usr/lib/x86_64-linux-gnu/libGLX_indirect.so.0
containerEdits:
  mounts:
    - hostPath: /run/nvidia-persistenced/socket
      containerPath: /run/nvidia-persistenced/socket
    - hostPath: /usr/bin/nvidia-persistenced
      containerPath: /usr/bin/nvidia-persistenced
  hooks:
    - hookName: createContainer
      path: /usr/bin/nvidia-cdi-hook
      args:
        - nvidia-cdi-hook
        - update-ldcache
        - --folder
        - /usr/lib/x86_64-linux-gnu
`)
	if err := os.WriteFile(path, spec, 0644); err != nil {
		return nil, err
	}
	return []byte("ok"), nil
}

func TestInjectNvidiaEnvVarsNoCudaInImage(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4)
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

	resultEnv := manager.InjectEnvVars(initialEnv)
	sort.Strings(expectedEnv)
	sort.Strings(resultEnv)
	assert.Equal(t, expectedEnv, resultEnv)
}

func TestInjectNvidiaEnvVarsExistingCudaInImage(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4)
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

	resultEnv := manager.InjectEnvVars(initialEnv)
	sort.Strings(expectedEnv)
	sort.Strings(resultEnv)
	assert.Equal(t, expectedEnv, resultEnv)
}

func TestInjectNvidiaMounts(t *testing.T) {
	manager := NewContainerNvidiaManagerForTest(4)
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
	if len(assignedDevices) != gpuCount {
		t.Errorf("Expected 2 GPUs to be assigned, got %d", len(assignedDevices))
	}

	if assignedDevices[0] != 0 && assignedDevices[1] != 1 { // Order might vary
		t.Errorf("Expected visible GPUs to be '0,1' or '1,0', got '%d'", assignedDevices)
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

func TestCDIDevicesPrefersAssignedUUIDs(t *testing.T) {
	manager := &ContainerNvidiaManager{
		infoClient: &GPUInfoClientForTest{
			GpuCount: 2,
			UUIDs: map[int]string{
				0: "GPU-a",
				1: "GPU-b",
			},
		},
		resolvedVisibleDevices: "GPU-a",
	}

	assert.Equal(t, []string{"nvidia.com/gpu=GPU-a"}, manager.CDIDevices([]int{0}))
	assert.Equal(t, []string{"nvidia.com/gpu=1"}, manager.CDIDevices([]int{1}))
}

func TestInjectAssignedEnvVarsRestoresPinnedGPU(t *testing.T) {
	manager := &ContainerNvidiaManager{
		infoClient: &GPUInfoClientForTest{
			GpuCount: 1,
			UUIDs:    map[int]string{0: "GPU-a"},
		},
		resolvedVisibleDevices: "GPU-a",
	}

	env := manager.InjectAssignedEnvVars([]string{
		"A=1",
		"NVIDIA_VISIBLE_DEVICES=void",
	}, []int{0})

	sort.Strings(env)
	assert.Equal(t, []string{
		"A=1",
		"NVIDIA_VISIBLE_DEVICES=GPU-a",
		"WORKER_GPU_DEVICES=GPU-a",
	}, env)
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
				var key string
				var err error

				for {
					assignedDevices, err := manager.AssignGPUDevices(id, uint32(gpusPerContainer))
					if err != nil {
						attempt++

						if attempt >= maxRetries {
							errCh <- fmt.Errorf("failed to assign GPU devices for %s on iteration %d after %d retries: %w", id, iter, attempt, err)
							break
						}

						time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)
						continue
					}

					nums := make([]string, len(assignedDevices))
					for i, num := range assignedDevices {
						nums[i] = strconv.Itoa(num)
					}
					key = strings.Join(nums, ",")
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
				delete(currentlyAssignedGPUs, key)
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
