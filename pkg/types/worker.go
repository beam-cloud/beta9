package types

import "time"

const (
	WorkerLifecycleStarted string = "STARTED"
	WorkerLifecycleStopped string = "STOPPED"

	WorkerLifecycleStatsKey        string        = "beam.worker.usage.spawner.lifecycle"
	WorkerDurationStatsKey         string        = "beam.worker.usage.spawner.duration"
	WorkerDurationEmissionInterval time.Duration = 30 * time.Second
)

type ContainerResourceUsage struct {
	ContainerID       string `json:"ContainerID"`
	CpuMillicoresUsed int64  `json:"CpuMillicoresUsed"`
	MemoryUsed        int    `json:"MemoryUsed"`
	GpuMemoryUsed     int64  `json:"GpuMemoryUsed"`
	GpuMemoryTotal    int64  `json:"GpuMemoryTotal"`
	GpuType           string `json:"GpuType"`
}
