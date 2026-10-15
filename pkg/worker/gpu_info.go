package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type GPUInfoClient interface {
	AvailableGPUDevices() ([]int, error)
	DeviceUUIDs() (map[int]string, error)
	GetGPUMemoryUsage(deviceIndex int) (GPUMemoryUsageStats, error)
}

type GPUMemoryUsageStats struct {
	UsedCapacity  int64
	TotalCapacity int64
}

type NvidiaInfoClient struct {
	visibleDevices string
	devicesMu      sync.Mutex
	devicesLoaded  bool
	devices        []nvidiaSMIDevice
}

const defaultDeviceCheckpointPath = types.HostKubeletDeviceCheckpointPath

var (
	nvidiaProcGPUInfoRoot = "/proc/driver/nvidia/gpus"
	errGPUUnhealthy       = errors.New("GPU requires recovery")
)

type kubeletCheckpoint struct {
	Data struct {
		PodDeviceEntries []podDeviceEntry `json:"PodDeviceEntries"`
	} `json:"Data"`
}

type podDeviceEntry struct {
	PodUID       string              `json:"PodUID"`
	ResourceName string              `json:"ResourceName"`
	DeviceIDs    map[string][]string `json:"DeviceIDs"`
}

type nvidiaSMIDevice struct {
	Index       int
	UUID        string
	SystemBusID string
}

// resolveVisibleDevices returns this worker's GPU assignment: "all" or a
// comma-separated list of UUIDs/indices. NVIDIA_VISIBLE_DEVICES is a request
// mechanism, not introspection (base images and the container toolkit both
// rewrite it to "void"), so the orchestrator's allocation record wins.
var resolveVisibleDevices = func() string {
	// k8s/k3s: the device-plugin checkpoint, keyed by pod UID.
	if devices := kubeletAssignedDevices(); devices != "" {
		return devices
	}
	// docker: the assignment passed explicitly by the agent.
	if devices := strings.TrimSpace(os.Getenv(types.WorkerGPUDevicesEnv)); devices != "" {
		return devices
	}

	devices := os.Getenv(types.NvidiaVisibleDevicesEnv)
	if devices == "void" && agentManagedWorker() {
		// Older agents only set NVIDIA_VISIBLE_DEVICES; the machine is
		// dedicated, so every device injected into the container is ours.
		return "all"
	}
	return devices
}

func kubeletAssignedDevices() string {
	podUID := os.Getenv(types.WorkerPodUIDEnv)
	if podUID == "" {
		return ""
	}

	data, err := os.ReadFile(defaultDeviceCheckpointPath)
	if err != nil {
		return ""
	}

	var checkpoint kubeletCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return ""
	}

	var allUUIDs []string
	for _, entry := range checkpoint.Data.PodDeviceEntries {
		if entry.PodUID != podUID || entry.ResourceName != "nvidia.com/gpu" {
			continue
		}
		for _, uuids := range entry.DeviceIDs {
			allUUIDs = append(allUUIDs, uuids...)
		}
	}
	return strings.Join(allUUIDs, ",")
}

// agentManagedWorker reports whether a private-pool agent started this worker
// on a machine dedicated to it.
func agentManagedWorker() bool {
	persistent, _ := strconv.ParseBool(os.Getenv(types.WorkerPersistentEnv))
	return persistent && os.Getenv(types.WorkerMachineEnv) != ""
}

func hexToPaddedString(hexStr string) (string, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	value, err := strconv.ParseUint(hexStr, 16, 16)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%04x", value), nil
}

var queryDevices = func() ([]byte, error) {
	cmd := exec.Command("nvidia-smi", "--query-gpu=pci.domain,pci.bus_id,index,uuid", "--format=csv,noheader,nounits")
	return cmd.Output()
}

// Recovery state changes while the worker is running; do not cache it with inventory.
var queryGPUHealth = func(deviceUUIDs string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=uuid,gpu_recovery_action", "--format=csv,noheader,nounits", "--id="+deviceUUIDs).CombinedOutput()
	if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 2 &&
		strings.TrimSpace(string(output)) == `Field "gpu_recovery_action" is not a valid field to query.` {
		return nil, nil // Older drivers do not expose recovery state.
	}
	return output, err
}

var checkGPUExists = func(busId string) (bool, error) {
	path := filepath.Join(nvidiaProcGPUInfoRoot, busId)
	_, err := os.Stat(path)
	if err == nil {
		data, err := os.ReadFile(filepath.Join(path, "information"))
		if err != nil {
			if os.IsNotExist(err) {
				return true, nil
			}
			return false, err
		}
		return !nvidiaProcInfoLooksFailed(string(data)), nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// deviceVisible reports whether a GPU matches the assignment spec: "all", or a
// comma-separated list of UUIDs and/or indices.
func deviceVisible(visibleDevices, uuid, index string) bool {
	if visibleDevices == "all" {
		return true
	}
	for _, token := range strings.Split(visibleDevices, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if token == uuid || token == index {
			return true
		}
	}
	return false
}

func (c *NvidiaInfoClient) AvailableGPUDevices() ([]int, error) {
	visibleDevices := c.visibleDevices
	devices, err := c.deviceInventory()
	if err != nil {
		return nil, err
	}

	var visible []nvidiaSMIDevice
	var uuids []string
	for _, device := range devices {
		if deviceVisible(visibleDevices, device.UUID, strconv.Itoa(device.Index)) {
			visible = append(visible, device)
			uuids = append(uuids, device.UUID)
		}
	}
	if len(visible) == 0 {
		return []int{}, nil
	}

	health, err := queryGPUHealth(strings.Join(uuids, ","))
	if err != nil {
		return nil, fmt.Errorf("query GPU recovery state: %w", err)
	}
	recoveryActions := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(health)), "\n") {
		if line == "" {
			continue
		}
		uuid, action, ok := strings.Cut(line, ",")
		if !ok || strings.TrimSpace(uuid) == "" {
			return nil, fmt.Errorf("invalid GPU recovery state: %q", line)
		}
		recoveryActions[strings.TrimSpace(uuid)] = strings.TrimSpace(action)
	}

	result := []int{}
	for _, device := range visible {
		action, reported := recoveryActions[device.UUID]
		if len(health) > 0 && !reported {
			return nil, fmt.Errorf("missing GPU recovery state for %s", device.UUID)
		}
		switch action {
		case "", "None", "N/A", "[N/A]":
		case "Reset", "Reboot", "Drain P2P", "Drain and Reset":
			return nil, fmt.Errorf("%w: %s requires %s", errGPUUnhealthy, device.UUID, action)
		default:
			return nil, fmt.Errorf("unknown GPU recovery state %q for %s", action, device.UUID)
		}

		if exists, err := checkGPUExists(device.SystemBusID); err == nil && exists {
			result = append(result, device.Index)
		}
	}

	return result, nil
}

// DeviceUUIDs returns the index-to-UUID mapping for all devices from a single
// nvidia-smi query, so callers can resolve many devices without re-querying.
func (c *NvidiaInfoClient) DeviceUUIDs() (map[int]string, error) {
	devices, err := c.deviceInventory()
	if err != nil {
		return nil, err
	}

	uuids := make(map[int]string, len(devices))
	for _, device := range devices {
		uuids[device.Index] = device.UUID
	}
	return uuids, nil
}

func (c *NvidiaInfoClient) deviceInventory() ([]nvidiaSMIDevice, error) {
	c.devicesMu.Lock()
	defer c.devicesMu.Unlock()
	if c.devicesLoaded {
		return c.devices, nil
	}

	devices, err := nvidiaSMIDevices()
	if err != nil {
		return nil, err
	}
	c.devices = devices
	c.devicesLoaded = true
	return c.devices, nil
}

func nvidiaSMIDevices() ([]nvidiaSMIDevice, error) {
	devices, err := queryDevices()
	if err != nil {
		return nil, err
	}
	return parseNvidiaSMIDevices(devices)
}

func nvidiaProcInfoLooksFailed(info string) bool {
	for _, line := range strings.Split(info, "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(name) {
		case "GPU Excluded":
			if strings.EqualFold(value, "yes") {
				return true
			}
		}
	}
	return false
}

func parseNvidiaSMIDevices(devices []byte) ([]nvidiaSMIDevice, error) {
	result := []nvidiaSMIDevice{}
	for _, line := range strings.Split(string(devices), "\n") {
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("unexpected output from nvidia-smi: %s", line)
		}

		domain, err := hexToPaddedString(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}

		smiBusIDParts := strings.Split(parts[1], ":")
		if len(smiBusIDParts) != 3 {
			return nil, fmt.Errorf("unexpected bus id format from nvidia-smi: %s", line)
		}

		gpuIndex, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, err
		}

		// The bus id from nvidia-smi comes as xxxxxxxx:xx:xx.x; procfs uses
		// xxxx:xx:xx.x.
		systemBusID := strings.ToLower(strings.Join([]string{domain, smiBusIDParts[1], smiBusIDParts[2]}, ":"))
		result = append(result, nvidiaSMIDevice{
			Index:       gpuIndex,
			UUID:        strings.TrimSpace(parts[3]),
			SystemBusID: systemBusID,
		})
	}
	return result, nil
}

// GetGpuMemoryUsage retrieves the memory usage of a specific NVIDIA GPU.
// It returns the total and used memory in bytes.
func (c *NvidiaInfoClient) GetGPUMemoryUsage(deviceIndex int) (GPUMemoryUsageStats, error) {
	stats := GPUMemoryUsageStats{}

	command := "nvidia-smi"
	commandArgs := []string{"--query-gpu=memory.total,memory.used", "--format=csv,noheader,nounits", fmt.Sprintf("--id=%d", deviceIndex)}

	out, err := exec.Command(command, commandArgs...).Output()
	if err != nil {
		return stats, fmt.Errorf("unable to invoke nvidia-smi: %v", err)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	if scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		if len(fields) != 2 {
			return stats, errors.New("unable to parse gpu memory info")
		}

		total, err := strconv.ParseInt(strings.Trim(fields[0], " "), 10, 64)
		if err != nil {
			return stats, fmt.Errorf("unable to parse total gpu memory: %v", err)
		}

		used, err := strconv.ParseInt(strings.Trim(fields[1], " "), 10, 64)
		if err != nil {
			return stats, fmt.Errorf("unable to parse used gpu memory: %v", err)
		}

		stats.TotalCapacity = total * 1024 * 1024
		stats.UsedCapacity = used * 1024 * 1024
	}

	return stats, nil
}
