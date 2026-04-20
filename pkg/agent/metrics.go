package agent

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

// MachineMetrics matches pkg/types/provider.go ProviderMachineMetrics
type MachineMetrics struct {
	TotalCpuAvailable    int     `json:"total_cpu_available"`
	TotalMemoryAvailable int     `json:"total_memory_available"`
	TotalDiskSpaceBytes  int     `json:"total_disk_space_bytes"`
	CpuUtilizationPct    float64 `json:"cpu_utilization_pct"`
	MemoryUtilizationPct float64 `json:"memory_utilization_pct"`
	TotalDiskFreeBytes   int     `json:"total_disk_free_bytes"`
	WorkerCount          int     `json:"worker_count"`
	ContainerCount       int     `json:"container_count"`
	FreeGpuCount         int     `json:"free_gpu_count"`
	CacheUsagePct        float64 `json:"cache_usage_pct"`
	CacheCapacity        int     `json:"cache_capacity"`
	CacheMemoryUsage     int     `json:"cache_memory_usage"`
	CacheCpuUsage        float64 `json:"cache_cpu_usage"`
}

// MetricsCollector handles system metrics collection
type MetricsCollector struct{}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// Collect gathers current system metrics
func (c *MetricsCollector) Collect() (*MachineMetrics, error) {
	// CPU count
	cpuCount, err := cpu.Counts(true)
	if err != nil {
		cpuCount = 1
	}

	// CPU utilization (sample over 100ms)
	cpuPercent, err := cpu.Percent(100*time.Millisecond, false)
	cpuPct := 0.0
	if err == nil && len(cpuPercent) > 0 {
		cpuPct = cpuPercent[0]
	}

	// Memory
	memInfo, err := mem.VirtualMemory()
	totalMem := uint64(0)
	memPct := 0.0
	if err == nil {
		totalMem = memInfo.Total
		memPct = memInfo.UsedPercent
	}

	// Disk
	diskInfo, err := disk.Usage("/")
	diskTotal := uint64(0)
	diskFree := uint64(0)
	if err == nil {
		diskTotal = diskInfo.Total
		diskFree = diskInfo.Free
	}

	// GPU
	gpuCount := DetectGPUCount()

	return &MachineMetrics{
		TotalCpuAvailable:    cpuCount * 1000, // millicores
		TotalMemoryAvailable: int(totalMem),
		TotalDiskSpaceBytes:  int(diskTotal),
		CpuUtilizationPct:    cpuPct,
		MemoryUtilizationPct: memPct,
		TotalDiskFreeBytes:   int(diskFree),
		FreeGpuCount:         gpuCount,
		// Cache and worker/container metrics default to 0
	}, nil
}

// DetectGPUCount returns the number of NVIDIA GPUs. Uses the cached
// absolute path for nvidia-smi (P1-C: PATH hijack) and returns 0 if the
// binary isn't installed.
func DetectGPUCount() int {
	nvPath := NvidiaSMIPath()
	if nvPath == "" {
		return 0
	}
	cmd := exec.Command(nvPath, "--query-gpu=name", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		return 0
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// GetCPUString returns CPU in Beta9 format (e.g., "8000m")
func GetCPUString() string {
	count, err := cpu.Counts(true)
	if err != nil {
		count = 4
	}
	return fmt.Sprintf("%dm", count*1000)
}

// GetMemoryString returns memory in Beta9 format (e.g., "16Gi")
func GetMemoryString() string {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return "8Gi"
	}
	gi := memInfo.Total / (1024 * 1024 * 1024)
	return fmt.Sprintf("%dGi", gi)
}

// GetPrivateIP returns the machine's private IP address
func GetPrivateIP() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			// Skip non-IPv4 and special addresses
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			// Only return IPv4
			if ip4 := ip.To4(); ip4 != nil {
				return ip4.String()
			}
		}
	}

	return "127.0.0.1"
}
