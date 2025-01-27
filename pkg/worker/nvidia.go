package worker

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/rs/zerolog/log"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/pkg/sync"
)

var (
	defaultContainerCudaVersion string   = "12.4"
	defaultContainerPath        []string = []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}
	defaultContainerLibrary     []string = []string{"/usr/lib/x86_64-linux-gnu", "/usr/lib/worker/x86_64-linux-gnu", "/usr/local/nvidia/lib64"}
)

type GPUManager interface {
	AssignGPUDevices(containerId string, gpuCount uint32) (*AssignedGpuDevices, error)
	GetContainerGPUDevices(containerId string) []int
	UnassignGPUDevices(containerId string)
	InjectEnvVars(env []string, options *ContainerOptions) ([]string, bool)
	InjectMounts(mounts []specs.Mount) []specs.Mount
}

type ContainerNvidiaManager struct {
	gpuAllocationMap *common.SafeMap[[]int]
	gpuCount         uint32
	mu               sync.Mutex
	statFunc         func(path string, stat *syscall.Stat_t) (err error)
	infoClient       GPUInfoClient
}

func NewContainerNvidiaManager(gpuCount uint32) GPUManager {
	return &ContainerNvidiaManager{
		gpuAllocationMap: common.NewSafeMap[[]int](),
		gpuCount:         gpuCount,
		mu:               sync.Mutex{},
		statFunc:         syscall.Stat,
		infoClient:       &NvidiaInfoClient{},
	}
}

type AssignedGpuDevices struct {
	devices []specs.LinuxDeviceCgroup
	visible string // Visible devices (for NVIDIA_VISIBLE_DEVICES env var)
}

func (d *AssignedGpuDevices) String() string {
	return d.visible
}

func (c *ContainerNvidiaManager) UnassignGPUDevices(containerId string) {
	c.gpuAllocationMap.Delete(containerId)
}

func (c *ContainerNvidiaManager) AssignGPUDevices(containerId string, gpuCount uint32) (*AssignedGpuDevices, error) {
	gpuIds, err := c.chooseDevices(containerId, gpuCount)
	if err != nil {
		return nil, err
	}

	// Device cgroup rules for the specific GPUs
	var devices []specs.LinuxDeviceCgroup
	var visibleGPUs []string // To collect the IDs for NVIDIA_VISIBLE_DEVICES

	for _, gpuId := range gpuIds {
		gpuDeviceNode := fmt.Sprintf("/dev/nvidia%d", gpuId)

		majorNum, err := c.getDeviceMajorNumber(gpuDeviceNode)
		if err != nil {
			return nil, err
		}

		minorNum, err := c.getDeviceMinorNumber(gpuDeviceNode)
		if err != nil {
			return nil, err
		}

		// Add the specific GPU device node
		devices = append(devices, specs.LinuxDeviceCgroup{
			Allow:  true,
			Type:   "c",
			Major:  majorNum,
			Minor:  minorNum,
			Access: "rwm",
		})

		// Collect GPU IDs for NVIDIA_VISIBLE_DEVICES
		visibleGPUs = append(visibleGPUs, fmt.Sprintf("%d", gpuId))
	}

	// Assuming control and UVM devices are shared across all GPUs and required
	majorNum, err := c.getDeviceMajorNumber("/dev/nvidiactl")
	if err != nil {
		return nil, err
	}

	minorNum, err := c.getDeviceMinorNumber("/dev/nvidiactl")
	if err != nil {
		return nil, err
	}
	devices = append(devices, specs.LinuxDeviceCgroup{
		Allow:  true,
		Type:   "c",
		Major:  majorNum,
		Minor:  minorNum,
		Access: "rwm",
	})

	// Join the GPU IDs with commas for the NVIDIA_VISIBLE_DEVICES variable
	visible := strings.Join(visibleGPUs, ",")

	return &AssignedGpuDevices{
		visible: visible,
		devices: devices,
	}, nil
}

func (c *ContainerNvidiaManager) GetContainerGPUDevices(containerId string) []int {
	gpuDevices, ok := c.gpuAllocationMap.Get(containerId)
	if !ok {
		return []int{}
	}

	return gpuDevices
}

func (c *ContainerNvidiaManager) chooseDevices(containerId string, requestedGpuCount uint32) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentAllocations := make(map[int]bool)
	c.gpuAllocationMap.Range(func(_ string, value []int) bool {
		for _, gpuId := range value {
			currentAllocations[gpuId] = true
		}
		return true // Continue iteration
	})

	availableDevices, err := c.infoClient.AvailableGPUDevices()
	if err != nil {
		return nil, err
	}

	allocableDevices := []int{}
	// Find available GPUs and allocate to the current container
	if len(currentAllocations) < len(availableDevices) {
		for _, gpuId := range availableDevices {
			if !currentAllocations[gpuId] {
				allocableDevices = append(allocableDevices, gpuId)
			}
		}
	}

	// Check if we managed to allocate the requested number of GPUs
	if len(allocableDevices) < int(requestedGpuCount) {
		return nil, fmt.Errorf("not enough GPUs available, requested: %d, allocable: %d out of %d", requestedGpuCount, int(c.gpuCount)-len(allocableDevices), len(availableDevices))
	}

	// Allocate the requested number of GPUs
	devicesToAllocate := allocableDevices[:requestedGpuCount]

	// Save the allocation in the SafeMap
	c.gpuAllocationMap.Set(containerId, devicesToAllocate)

	return devicesToAllocate, nil
}

// getDeviceMajorNumber returns the major device number for the given device node path
func (c *ContainerNvidiaManager) getDeviceMajorNumber(devicePath string) (*int64, error) {
	stat := syscall.Stat_t{}
	if err := c.statFunc(devicePath, &stat); err != nil {
		return nil, err
	}

	major := int64(major(uint64(stat.Rdev))) // Extract major number
	return &major, nil
}

// getDeviceMinorNumber returns the minor device number for the given device node path
func (c *ContainerNvidiaManager) getDeviceMinorNumber(devicePath string) (*int64, error) {
	stat := syscall.Stat_t{}
	if err := c.statFunc(devicePath, &stat); err != nil {
		return nil, err
	}

	minor := int64(minor(uint64(stat.Rdev))) // Extract minor number
	return &minor, nil
}

// major extracts the major device number from the raw device number
func major(dev uint64) uint64 {
	return (dev >> 8) & 0xfff
}

// minor extracts the minor device number from the raw device number
func minor(dev uint64) uint64 {
	return (dev & 0xff) | ((dev >> 12) & 0xfff00)
}

func (c *ContainerNvidiaManager) InjectEnvVars(env []string, options *ContainerOptions) ([]string, bool) {
	existingCudaFound := false
	cudaEnvVarDefaults := map[string]string{
		"NVIDIA_DRIVER_CAPABILITIES": "all",
		"NVIDIA_REQUIRE_CUDA":        "",
		"NVARCH":                     "",
		"NV_CUDA_COMPAT_PACKAGE":     "",
		"NV_CUDA_CUDART_VERSION":     "",
		"CUDA_VERSION":               "",
		"GPU_TYPE":                   "",
		"CUDA_HOME":                  "/usr/local/cuda-12.4",
	}

	initialEnvVars := make(map[string]string)
	if options.InitialSpec != nil {
		for _, m := range options.InitialSpec.Process.Env {

			// Only split on the first "=" in the env var
			// incase the value has any "=" in it
			splitVar := strings.SplitN(m, "=", 2)
			if len(splitVar) < 2 {
				continue
			}

			name := splitVar[0]
			value := splitVar[1]
			initialEnvVars[name] = value
		}
	}

	cudaVersion := defaultContainerCudaVersion
	existingCudaVersion, existingCudaFound := initialEnvVars["CUDA_VERSION"]
	if existingCudaFound {
		splitVersion := strings.Split(existingCudaVersion, ".")
		if len(splitVersion) >= 2 {
			major := splitVersion[0]
			minor := splitVersion[1]

			formattedVersion := major + "." + minor

			log.Info().Str("cuda_version", existingCudaVersion).Str("formatted_version", formattedVersion).Msg("found existing cuda version in container image")

			cudaVersion = formattedVersion
			existingCudaFound = true
		}
	}

	var cudaEnvVars []string
	for key, defaultValue := range cudaEnvVarDefaults {
		cudaEnvVarValue := os.Getenv(key)

		if existingCudaFound {
			if value, exists := initialEnvVars[key]; exists {
				cudaEnvVarValue = value
			} else {
				continue
			}
		}

		if cudaEnvVarValue == "" {
			cudaEnvVarValue = defaultValue
		}

		cudaEnvVars = append(cudaEnvVars, fmt.Sprintf("%s=%s", key, cudaEnvVarValue))
	}

	env = append(env, cudaEnvVars...)

	env = append(env,
		fmt.Sprintf("PATH=%s:/usr/local/cuda-%s/bin:$PATH",
			strings.Join(defaultContainerPath, ":"),
			cudaVersion))

	env = append(env,
		fmt.Sprintf("LD_LIBRARY_PATH=%s:/usr/local/cuda-%s/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
			strings.Join(defaultContainerLibrary, ":"),
			cudaVersion))

	return env, existingCudaFound
}

func (c *ContainerNvidiaManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	cudaPaths := []string{fmt.Sprintf("/usr/local/cuda-%s", defaultContainerCudaVersion), "/usr/local/nvidia/lib64"}

	for _, path := range cudaPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		mounts = append(mounts, []specs.Mount{
			{
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
			},
		}...)
	}

	return mounts
}
