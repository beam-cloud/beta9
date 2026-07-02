package worker

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"sigs.k8s.io/yaml"
	"tags.cncf.io/container-device-interface/pkg/cdi"
	cdispecs "tags.cncf.io/container-device-interface/specs-go"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/pkg/sync"
)

const (
	nvidiaDeviceKindPrefix     string = "nvidia.com/gpu"
	nvidiaFirstDevice          string = nvidiaDeviceKindPrefix + "=0"
	nvidiaCDIHookBinary        string = "nvidia-cdi-hook"
	nvidiaCDIUpdateLdcacheHook string = "update-ldcache"
)

var (
	defaultContainerCudaVersion string   = "12.4"
	defaultContainerPath        []string = []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}
	defaultContainerLibrary     []string = []string{"/usr/lib/x86_64-linux-gnu", "/usr/lib/worker/x86_64-linux-gnu", "/usr/local/nvidia/lib64"}
	// Keep generated CDI specs isolated from provider or host-managed CDI specs.
	nvidiaCDIConfigPaths    []string = []string{"/var/run/beam/cdi/nvidia.yaml", "/var/run/cdi/nvidia.yaml", "/etc/cdi/nvidia.yaml"}
	nvidiaCDIMountDenylist  []string = []string{"/run/nvidia-persistenced/socket", "/var/run/nvidia-persistenced/socket"}
	runNvidiaCTKCDIGenerate          = func(outputPath string) ([]byte, error) {
		return exec.Command("nvidia-ctk", "cdi", "generate", "--output", outputPath).CombinedOutput()
	}
	configureNvidiaCDICache = func(specPath string) error {
		return cdi.Configure(cdi.WithSpecDirs(filepath.Dir(specPath)))
	}
	nvidiaCDIDeviceResolvable = func() bool {
		return cdi.GetDefaultCache().GetDevice(nvidiaFirstDevice) != nil
	}
)

type GPUManager interface {
	AssignGPUDevices(containerId string, gpuCount uint32) ([]int, error)
	GetContainerGPUDevices(containerId string) []int
	UnassignGPUDevices(containerId string)
	CDIDevices(assignedDevices []int) []string
	InjectEnvVars(env []string) []string
	InjectAssignedEnvVars(env []string, assignedDevices []int) []string
	InjectMounts(mounts []specs.Mount) []specs.Mount
}

type ContainerNvidiaManager struct {
	gpuAllocationMap       *common.SafeMap[[]int]
	gpuCount               uint32
	mu                     sync.Mutex
	statFunc               func(path string, stat *syscall.Stat_t) (err error)
	infoClient             GPUInfoClient
	resolvedVisibleDevices string
}

func NewContainerNvidiaManager(gpuCount uint32) GPUManager {
	if gpuCount > 0 {
		if err := ensureNvidiaCDIConfig(); err != nil {
			log.Fatal().Msgf("failed to generate cdi config: %v", err)
		}
	}

	visibleDevices := resolveVisibleDevices()
	log.Info().Str("resolved_visible_devices", visibleDevices).Msg("resolved NVIDIA_VISIBLE_DEVICES for GPU filtering")

	return &ContainerNvidiaManager{
		gpuAllocationMap:       common.NewSafeMap[[]int](),
		gpuCount:               gpuCount,
		mu:                     sync.Mutex{},
		statFunc:               syscall.Stat,
		infoClient:             &NvidiaInfoClient{visibleDevices: visibleDevices},
		resolvedVisibleDevices: visibleDevices,
	}
}

func ensureNvidiaCDIConfig() error {
	failures := make([]string, 0, len(nvidiaCDIConfigPaths))
	for _, path := range nvidiaCDIConfigPaths {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			failures = append(failures, fmt.Sprintf("%s: create dir: %v", path, err))
			continue
		}

		output, err := runNvidiaCTKCDIGenerate(path)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %s", path, nvidiaCDICommandError(output, err)))
			continue
		}
		if err := sanitizeNvidiaCDIConfig(path); err != nil {
			failures = append(failures, fmt.Sprintf("%s: sanitize generated spec: %v", path, err))
			continue
		}

		if err := configureNvidiaCDICache(path); err != nil {
			failures = append(failures, fmt.Sprintf("%s: configure cache: %v", path, err))
			continue
		}
		if !nvidiaCDIDeviceResolvable() {
			failures = append(failures, fmt.Sprintf("%s: %s is not resolvable after generation", path, nvidiaFirstDevice))
			continue
		}

		log.Info().
			Str("path", path).
			Str("spec_dir", filepath.Dir(path)).
			Msg("generated NVIDIA CDI config")
		return nil
	}

	return fmt.Errorf("%s", strings.Join(failures, "; "))
}

func sanitizeNvidiaCDIConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var spec cdispecs.Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return err
	}

	changed := sanitizeNvidiaCDIContainerEdits(&spec.ContainerEdits)
	for i := range spec.Devices {
		if sanitizeNvidiaCDIContainerEdits(&spec.Devices[i].ContainerEdits) {
			changed = true
		}
	}
	if !changed {
		return nil
	}

	output, err := yaml.Marshal(spec)
	if err != nil {
		return err
	}
	return os.WriteFile(path, output, 0644)
}

func sanitizeNvidiaCDIContainerEdits(edits *cdispecs.ContainerEdits) bool {
	if edits == nil {
		return false
	}

	changed := false

	if len(edits.Mounts) > 0 {
		count := len(edits.Mounts)
		edits.Mounts = slices.DeleteFunc(edits.Mounts, isDeniedNvidiaCDIMount)
		changed = changed || len(edits.Mounts) != count
	}

	if len(edits.Hooks) > 0 {
		count := len(edits.Hooks)
		edits.Hooks = slices.DeleteFunc(edits.Hooks, isDeniedNvidiaCDIHook)
		changed = changed || len(edits.Hooks) != count
	}

	return changed
}

func isDeniedNvidiaCDIMount(mount *cdispecs.Mount) bool {
	if mount == nil {
		return false
	}
	for _, denied := range nvidiaCDIMountDenylist {
		if mount.HostPath == denied || mount.ContainerPath == denied {
			return true
		}
	}
	return false
}

func isDeniedNvidiaCDIHook(hook *cdispecs.Hook) bool {
	if hook == nil || hook.HookName != cdi.CreateContainerHook {
		return false
	}
	if filepath.Base(hook.Path) != nvidiaCDIHookBinary {
		return false
	}
	return slices.Contains(hook.Args, nvidiaCDIUpdateLdcacheHook)
}

func nvidiaCDICommandError(output []byte, err error) string {
	msg := strings.TrimSpace(string(output))
	if msg == "" {
		return err.Error()
	}
	return fmt.Sprintf("%v: %s", err, msg)
}

type AssignedGpuDevices struct {
}

func (c *ContainerNvidiaManager) UnassignGPUDevices(containerId string) {
	c.gpuAllocationMap.Delete(containerId)
}

func (c *ContainerNvidiaManager) AssignGPUDevices(containerId string, gpuCount uint32) ([]int, error) {
	gpuIds, err := c.chooseDevices(containerId, gpuCount)
	if err != nil {
		return nil, err
	}
	return gpuIds, nil
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
		return nil, fmt.Errorf("not enough GPUs available: requested=%d, allocable=%d, visible=%d, configured=%d, already_allocated=%d, NVIDIA_VISIBLE_DEVICES=%q",
			requestedGpuCount, len(allocableDevices), len(availableDevices), c.gpuCount, len(currentAllocations), c.resolvedVisibleDevices)
	}

	// Allocate the requested number of GPUs
	devicesToAllocate := allocableDevices[:requestedGpuCount]

	// Save the allocation in the SafeMap
	c.gpuAllocationMap.Set(containerId, devicesToAllocate)

	return devicesToAllocate, nil
}

func (c *ContainerNvidiaManager) CDIDevices(assignedDevices []int) []string {
	devices := make([]string, 0, len(assignedDevices))
	for _, device := range assignedDevices {
		devices = append(devices, fmt.Sprintf("%s=%s", nvidiaDeviceKindPrefix, c.deviceSelector(device)))
	}
	return devices
}

func (c *ContainerNvidiaManager) InjectAssignedEnvVars(env []string, assignedDevices []int) []string {
	visibleDevices := c.assignedVisibleDevices(assignedDevices)
	if visibleDevices == "" {
		return env
	}

	return upsertEnvVars(env, []string{
		types.NvidiaVisibleDevicesEnv + "=" + visibleDevices,
		types.WorkerGPUDevicesEnv + "=" + visibleDevices,
	})
}

func (c *ContainerNvidiaManager) assignedVisibleDevices(assignedDevices []int) string {
	devices := make([]string, 0, len(assignedDevices))
	for _, device := range assignedDevices {
		devices = append(devices, c.deviceSelector(device))
	}
	return strings.Join(devices, ",")
}

func (c *ContainerNvidiaManager) deviceSelector(device int) string {
	index := strconv.Itoa(device)
	uuid, ok := c.infoClient.DeviceUUID(device)
	if !ok {
		return index
	}
	if visibleDeviceMatchesUUID(c.resolvedVisibleDevices, uuid) {
		return uuid
	}
	return index
}

func visibleDeviceMatchesUUID(visibleDevices, uuid string) bool {
	for _, token := range strings.Split(visibleDevices, ",") {
		if strings.TrimSpace(token) == uuid {
			return true
		}
	}
	return false
}

func upsertEnvVars(env []string, updates []string) []string {
	for _, update := range updates {
		name, _, ok := strings.Cut(update, "=")
		if !ok {
			continue
		}
		replaced := false
		for i, existing := range env {
			existingName, _, ok := strings.Cut(existing, "=")
			if ok && existingName == name {
				env[i] = update
				replaced = true
			}
		}
		if !replaced {
			env = append(env, update)
		}
	}
	return env
}

func (c *ContainerNvidiaManager) InjectEnvVars(env []string) []string {
	cudaEnvVarDefaults := map[string]string{
		"NVIDIA_DRIVER_CAPABILITIES": "compute,utility,graphics,ngx,video",
		"NVIDIA_REQUIRE_CUDA":        "",
		"NVARCH":                     "",
		"NV_CUDA_COMPAT_PACKAGE":     "",
		"NV_CUDA_CUDART_VERSION":     "",
		"CUDA_VERSION":               "",
		types.WorkerGPUEnv:           "",
		"CUDA_HOME":                  fmt.Sprintf("/usr/local/cuda-%s", defaultContainerCudaVersion),
	}

	imageEnvVars := make(map[string]string)
	for _, m := range env {

		// Only split on the first "=" in the env var
		// incase the value has any "=" in it
		splitVar := strings.SplitN(m, "=", 2)
		if len(splitVar) < 2 {
			continue
		}

		name := splitVar[0]
		value := splitVar[1]
		imageEnvVars[name] = value
	}

	cudaVersion := defaultContainerCudaVersion
	existingCudaVersion, existingCudaFound := imageEnvVars["CUDA_VERSION"]
	if existingCudaFound {
		splitVersion := strings.Split(existingCudaVersion, ".")
		if len(splitVersion) >= 2 {
			major := splitVersion[0]
			minor := splitVersion[1]
			cudaVersion = fmt.Sprintf("%s.%s", major, minor)
			log.Info().Str("cuda_version", existingCudaVersion).Str("formatted_version", cudaVersion).Msg("found existing cuda version in container image")
		}
		cudaEnvVarDefaults["CUDA_HOME"] = fmt.Sprintf("/usr/local/cuda-%s", cudaVersion)
	}

	// Keep existing image values, otherwise use host values, fall back to defaults if neither exists
	for key, defaultValue := range cudaEnvVarDefaults {
		hostValue := os.Getenv(key)
		switch {
		case imageEnvVars[key] != "":
			continue
		case hostValue != "":
			imageEnvVars[key] = hostValue
		case defaultValue != "":
			imageEnvVars[key] = defaultValue
		}
	}

	mergePaths("PATH", imageEnvVars, append(defaultContainerPath, fmt.Sprintf("/usr/local/cuda-%s/bin", cudaVersion)))
	mergePaths("LD_LIBRARY_PATH", imageEnvVars, append(defaultContainerLibrary, fmt.Sprintf("/usr/local/cuda-%s/targets/x86_64-linux/lib", cudaVersion)))

	modifiedEnv := make([]string, 0, len(imageEnvVars))
	for key, value := range imageEnvVars {
		modifiedEnv = append(modifiedEnv, fmt.Sprintf("%s=%s", key, value))
	}

	return modifiedEnv
}

func mergePaths(pathName string, initEnv map[string]string, mergeIn []string) {
	if initEnv[pathName] == "" {
		initEnv[pathName] = strings.Join(mergeIn, ":")
		return
	}

	existingPath := initEnv[pathName]
	pathMembers := strings.Split(existingPath, ":")

	// Add paths to be merged in AFTER the existing paths so that the existing paths take precedence
	for _, path := range mergeIn {
		if !slices.Contains(pathMembers, path) {
			pathMembers = append(pathMembers, path)
		}
	}

	initEnv[pathName] = strings.Join(pathMembers, ":")
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
