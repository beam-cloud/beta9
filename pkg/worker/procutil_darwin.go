//go:build darwin
// +build darwin

package worker

func GetGpuMemoryUsage(deviceId int) (GpuMemoryUsageStats, error) {
	return GpuMemoryUsageStats{0, 0}, nil
}
