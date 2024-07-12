package types

import cloudevents "github.com/cloudevents/sdk-go/v2/event"

type EventSink = func(event []cloudevents.Event)

type EventClient interface {
	PushEvent(event cloudevents.Event) error
}

var (
	EventContainerLifecycle = "container.lifecycle"
	EventContainerMetrics   = "container.metrics"
	EventWorkerLifecycle    = "worker.lifecycle"
	EventStubDeploy         = "stub.deploy"
	EventStubServe          = "stub.serve"
	EventStubRun            = "stub.run"
)

var (
	EventContainerLifecycleRequested = "requested"
	EventContainerLifecycleScheduled = "scheduled"
	EventContainerLifecycleStarted   = "started"
	EventContainerLifecycleStopped   = "stopped"
	EventContainerLifecycleFailed    = "failed"
)

var (
	EventWorkerLifecycleStarted = "started"
	EventWorkerLifecycleStopped = "stopped"
)

// Schema versions should be in ISO 8601 format

var EventContainerLifecycleSchemaVersion = "1.0"

type EventContainerLifecycleSchema struct {
	ContainerID string           `json:"container_id"`
	WorkerID    string           `json:"worker_id"`
	Status      string           `json:"status"`
	Request     ContainerRequest `json:"request"`
}

var EventContainerMetricsSchemaVersion = "1.0"

type EventContainerMetricsSchema struct {
	WorkerID         string                    `json:"worker_id"`
	ContainerID      string                    `json:"container_id"`
	WorkspaceID      string                    `json:"workspace_id"`
	StubID           string                    `json:"stub_id"`
	ContainerMetrics EventContainerMetricsData `json:"metrics"`
}

type EventContainerMetricsData struct {
	CPUUsed            uint64  `json:"cpu_used"`
	CPUTotal           uint64  `json:"cpu_total"`
	CPUPercent         float32 `json:"cpu_pct"`
	MemoryRSS          uint64  `json:"memory_rss_bytes"`
	MemoryVMS          uint64  `json:"memory_vms_bytes"`
	MemorySwap         uint64  `json:"memory_swap_bytes"`
	MemoryTotal        uint64  `json:"memory_total_bytes"`
	DiskReadBytes      uint64  `json:"disk_read_bytes"`
	DiskWriteBytes     uint64  `json:"disk_write_bytes"`
	NetworkBytesRecv   uint64  `json:"network_recv_bytes"`
	NetworkBytesSent   uint64  `json:"network_sent_bytes"`
	NetworkPacketsRecv uint64  `json:"network_recv_packets"`
	NetworkPacketsSent uint64  `json:"network_sent_packets"`
	GPUMemoryUsed      uint64  `json:"gpu_memory_used_bytes"`
	GPUMemoryTotal     uint64  `json:"gpu_memory_total_bytes"`
	GPUType            string  `json:"gpu_type"`
}

var EventContainerStatusRequestedSchemaVersion = "1.0"

type EventContainerStatusRequestedSchema struct {
	ContainerID string           `json:"container_id"`
	Request     ContainerRequest `json:"request"`
	Status      string           `json:"status"`
}

var EventWorkerLifecycleSchemaVersion = "1.0"

type EventWorkerLifecycleSchema struct {
	WorkerID string `json:"worker_id"`
	Status   string `json:"status"`
}

var EventStubSchemaVersion = "1.0"

type EventStubSchema struct {
	ID          string   `json:"id"`
	StubType    StubType `json:"stub_type"`
	WorkspaceID string   `json:"workspace_id"`
	StubConfig  string   `json:"stub_config"`
}
