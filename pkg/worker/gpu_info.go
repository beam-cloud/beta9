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

const defaultDeviceCheckpointPath = "/var/lib/kubelet/device-plugins/kubelet_internal_checkpoint"

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

// resolveVisibleDevices determines which GPU is assigned to this worker pod.
//
// The nvidia/cuda base image sets ENV NVIDIA_VISIBLE_DEVICES=void which the
// container runtime processes AFTER PID 1 starts, so os.Getenv always returns
// "void". The authoritative GPU assignment lives in the kubelet device plugin
// checkpoint file, which maps pod UIDs to allocated GPU UUIDs.
var resolveVisibleDevices = func() string {
	podUID := os.Getenv("POD_UID")
	if podUID == "" {
		return os.Getenv("NVIDIA_VISIBLE_DEVICES")
	}

	data, err := os.ReadFile(defaultDeviceCheckpointPath)
	if err != nil {
		return os.Getenv("NVIDIA_VISIBLE_DEVICES")
	}

	var checkpoint kubeletCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return os.Getenv("NVIDIA_VISIBLE_DEVICES")
	}

	for _, entry := range checkpoint.Data.PodDeviceEntries {
		if entry.PodUID != podUID || entry.ResourceName != "nvidia.com/gpu" {
			continue
		}
		for _, uuids := range entry.DeviceIDs {
			if len(uuids) > 0 {
				return strings.Join(uuids, ",")
			}
		}
	}

	return os.Getenv("NVIDIA_VISIBLE_DEVICES")
}

func (c *NvidiaInfoClient) hexToPaddedString(hexStr string) (string, error) {
	// Remove the "0x" prefix if it exists
	hexStr = strings.TrimPrefix(hexStr, "0x")

	// Parse the hexadecimal string to an integer
	value, err := strconv.ParseUint(hexStr, 16, 16)
	if err != nil {
		return "", err
	}

	// Format the integer as a zero-padded string with 4 digits
	paddedStr := fmt.Sprintf("%04x", value)
	return paddedStr, nil
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
		if !strings.Contains(visibleDevices, uuid) && visibleDevices != "all" {
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
