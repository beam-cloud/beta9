//go:build linux
// +build linux

package worker

import (
	"errors"
	"strconv"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/okteto/okteto/pkg/log"
)

func GetGpuMemoryUsage(deviceId int) (GpuMemoryUsageStats, error) {
	stats := GpuMemoryUsageStats{}

	device, ret := nvml.DeviceGetHandleByIndex(deviceId)
	if ret != nvml.SUCCESS {
		log.Error("Unable to get device at index %d: %v", deviceId, nvml.ErrorString(ret))
		return stats, errors.New(
			"Unable to get device at index " + strconv.Itoa(deviceId) + ": " + nvml.ErrorString(ret),
		)
	}

	memory, ret := device.GetMemoryInfo()
	if ret != nvml.SUCCESS {
		log.Error("Unable to get memory info for device at index %d: %v", deviceId, nvml.ErrorString(ret))
		return stats, errors.New(
			"Unable to get memory info for device at index " + strconv.Itoa(deviceId) + ": " + nvml.ErrorString(ret),
		)
	}

	stats.UsedCapacity = int64(memory.Used)
	stats.TotalCapacity = int64(memory.Total)

	return stats, nil
}
