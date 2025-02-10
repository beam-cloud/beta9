package worker

import (
	"bufio"
	"errors"
	"fmt"
	"log"
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

type NvidiaInfoClient struct{}

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
	visibleDevices := os.Getenv("NVIDIA_VISIBLE_DEVICES") // Find available GPU BUS IDs
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

		// PCI bus_id is shown to be "domain:bus:device.function", but the folder in /proc/driver/nvidia/gpus is just "bus:device.function"
		busId := strings.ToLower(
			strings.TrimPrefix(
				strings.TrimSpace(parts[1]), domain,
			),
		)
		gpuIndex := strings.TrimSpace(parts[2])

		if exists, err := checkGPUExists(busId); err == nil && exists {
			index, err := strconv.Atoi(strings.TrimSpace(gpuIndex))
			if err != nil {
				log.Printf("error converting gpuIndex to int: %v", err)
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
