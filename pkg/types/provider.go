package types

import (
	"time"
)

const (
	MachineStatusRegistered          MachineStatus = "registered"
	MachineStatusPending             MachineStatus = "pending"
	MachineStatusReady               MachineStatus = "ready"
	MachinePendingExpirationS        int           = 3600 // 1 hour
	MachineKeepaliveExpirationS      int           = 300  // 5 minutes
	MachineEmptyConsolidationPeriodM time.Duration = 10 * time.Minute
)

type MachineStatus string

type ProviderComputeRequest struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type ProviderMachine struct {
	State   *ProviderMachineState   `json:"state"`
	Metrics *ProviderMachineMetrics `json:"metrics"`
}

type ProviderMachineMetrics struct {
	TotalCpuAvailable    int     `json:"total_cpu_available" redis:"total_cpu_available"`
	TotalMemoryAvailable int     `json:"total_memory_available" redis:"total_memory_available"`
	TotalDiskSpaceBytes  int     `json:"total_disk_space_bytes" redis:"total_disk_space_bytes"`
	CpuUtilizationPct    float64 `json:"cpu_utilization_pct" redis:"cpu_utilization_pct"`
	MemoryUtilizationPct float64 `json:"memory_utilization_pct" redis:"memory_utilization_pct"`
	TotalDiskFreeBytes   int     `json:"total_disk_free_bytes" redis:"total_disk_free_bytes"`
	WorkerCount          int     `json:"worker_count" redis:"worker_count"`
	ContainerCount       int     `json:"container_count" redis:"container_count"`
	FreeGpuCount         int     `json:"free_gpu_count" redis:"free_gpu_count"`
	CacheUsagePct        float64 `json:"cache_usage_pct" redis:"cache_usage_pct"`
	CacheCapacity        int     `json:"cache_capacity" redis:"cache_capacity"`
	CacheMemoryUsage     int     `json:"cache_memory_usage" redis:"cache_memory_usage"`
	CacheCpuUsage        float64 `json:"cache_cpu_usage" redis:"cache_cpu_usage"`
}

type ProviderMachineState struct {
	MachineId         string        `json:"machine_id" redis:"machine_id"`
	PoolName          string        `json:"pool_name" redis:"pool_name"`
	Status            MachineStatus `json:"status" redis:"status"`
	HostName          string        `json:"hostname" redis:"hostname"`
	Token             string        `json:"token" redis:"token"`
	Cpu               int64         `json:"cpu" redis:"cpu"`
	Memory            int64         `json:"memory" redis:"memory"`
	Gpu               string        `json:"gpu" redis:"gpu"`
	GpuCount          uint32        `json:"gpu_count" redis:"gpu_count"`
	RegistrationToken string        `json:"registration_token" redis:"registration_token"`
	PrivateIP         string        `json:"private_ip" redis:"private_ip"`
	Created           string        `json:"created" redis:"created"`
	LastWorkerSeen    string        `json:"last_worker_seen" redis:"last_worker_seen"`
	LastKeepalive     string        `json:"last_keepalive" redis:"last_keepalive"`
	AutoConsolidate   bool          `json:"auto_consolidate" redis:"auto_consolidate"`
	AgentVersion      string        `json:"agent_version" redis:"agent_version"`
}

type ProviderNotImplemented struct {
	msg string
}

func (e *ProviderNotImplemented) Error() string {
	return e.msg
}

func NewProviderNotImplemented() error {
	return &ProviderNotImplemented{msg: "provider not implemented"}
}
