package types

import "time"

const (
	WorkerLifecycleStarted string = "STARTED"
	WorkerLifecycleStopped string = "STOPPED"

	WorkerLifecycleStatsKey        string        = "beta9.worker.usage.spawner.lifecycle"
	WorkerDurationStatsKey         string        = "beta9.worker.usage.spawner.duration"
	WorkerUserCodeVolume           string        = "/mnt/code"
	WorkerDurationEmissionInterval time.Duration = 30 * time.Second
	WorkerKeepAliveInterval        time.Duration = 15 * time.Second
)

type ContainerResourceUsage struct {
	ContainerID       string `json:"ContainerID"`
	CpuMillicoresUsed int64  `json:"CpuMillicoresUsed"`
	MemoryUsed        int    `json:"MemoryUsed"`
	GpuMemoryUsed     int64  `json:"GpuMemoryUsed"`
	GpuMemoryTotal    int64  `json:"GpuMemoryTotal"`
	GpuType           string `json:"GpuType"`
}

type Mount struct {
	LocalPath string `json:"local_path"`
	MountPath string `json:"mount_path"`
	LinkPath  string `json:"link_path"`
	ReadOnly  bool   `json:"read_only"`
}
