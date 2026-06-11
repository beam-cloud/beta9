package worker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type GPUInfoClient interface {
	AvailableGPUDevices() ([]int, error)
	GetGPUMemoryUsage(deviceIndex int) (GPUMemoryUsageStats, error)
}

type GPUMemoryUsageStats struct {
	UsedCapacity  int64
	TotalCapacity int64
}

type NvidiaInfoClient struct {
	visibleDevices string
}

const defaultDeviceCheckpointPath = types.HostKubeletDeviceCheckpointPath

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

func (c *NvidiaInfoClient) hexToPaddedString(hexStr string) (string, error) {
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

var checkGPUExists = func(busId string) (bool, error) {
	_, err := os.Stat(fmt.Sprintf("/proc/driver/nvidia/gpus/%s", busId))
	if err == nil {
		return true, nil
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
	devices, err := queryDevices()
	if err != nil {
		return nil, err
	}

	// Parse the output
	result := []int{}
	for _, line := range strings.Split(string(devices), "\n") {
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("unexpected output from nvidia-smi: %s", line)
		}

		domain, err := c.hexToPaddedString(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}

		uuid := strings.TrimSpace(parts[3])
		if !deviceVisible(visibleDevices, uuid, strings.TrimSpace(parts[2])) {
			continue
		}

		smiBusIdParts := strings.Split(parts[1], ":")
		if len(smiBusIdParts) != 3 {
			return nil, fmt.Errorf("unexpected bus id format from nvidia-smi: %s", line)
		}

		// The bus id from nvidia-smi comes as xxxxxxxx:xx:xx.x so convert it to the format xxxx:xx:xx.x
		systemBusId := strings.ToLower(strings.Join([]string{domain, smiBusIdParts[1], smiBusIdParts[2]}, ":"))
		gpuIndex := strings.TrimSpace(parts[2])

		if exists, err := checkGPUExists(systemBusId); err == nil && exists {
			index, err := strconv.Atoi(strings.TrimSpace(gpuIndex))
			if err != nil {
				return nil, err
			}

			result = append(result, index)
		}
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
