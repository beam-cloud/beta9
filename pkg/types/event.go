package types

import (
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

type EventSink = func(event []cloudevents.Event)

type EventClient interface {
	PushEvent(event cloudevents.Event) error
}

var (
	/*
		Stripe events utilize a format of <resource>.<action>
		1.	<resource>: This indicates the type of object or resource that the event pertains to, such as payment_intent, invoice, customer, subscription, etc.
		2.	<action>: This indicates the specific action or change that occurred with that resource, such as created, updated, deleted, succeeded, etc.
	*/
	EventTaskUpdated = "task.updated"
	EventTaskCreated = "task.created"
	EventStubState   = "stub.state.%s" // healthy, degraded, warning

	/*
		TODO: Requires updates
		stub.ran
		stub.deployed
		stub.served

		container.lifecycle.updated
		worker.lifecycle.updated

		Need to update logic the locations that use these events
	*/

	EventContainerLifecycle = "container.lifecycle"
	EventContainerMetrics   = "container.metrics"
	EventWorkerLifecycle    = "worker.lifecycle"
	EventStubDeploy         = "stub.deploy"
	EventStubServe          = "stub.serve"
	EventStubRun            = "stub.run"
	EventStubClone          = "stub.clone"

	EventWorkerPoolDegraded = "workerpool.degraded"
	EventWorkerPoolHealthy  = "workerpool.healthy"
)

var (
	EventContainerLifecycleRequested = "requested"
	EventContainerLifecycleScheduled = "scheduled"
	EventContainerLifecycleStarted   = "started"
	EventContainerLifecycleStopped   = "stopped"
	EventContainerLifecycleOOM       = "oom"
	EventContainerLifecycleFailed    = "failed"
)

var (
	EventWorkerLifecycleStarted = "started"
	EventWorkerLifecycleStopped = "stopped"
	EventWorkerLifecycleDeleted = "deleted"
)

// Schema versions should be in ISO 8601 format

var EventContainerLifecycleSchemaVersion = "1.1"

type EventContainerLifecycleSchema struct {
	ContainerID string           `json:"container_id"`
	WorkerID    string           `json:"worker_id"`
	StubID      string           `json:"stub_id"`
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

var EventContainerStatusRequestedSchemaVersion = "1.1"

type EventContainerStatusRequestedSchema struct {
	ContainerID string           `json:"container_id"`
	Request     ContainerRequest `json:"request"`
	StubID      string           `json:"stub_id"`
	Status      string           `json:"status"`
}

var EventWorkerLifecycleSchemaVersion = "1.0"

type EventWorkerLifecycleSchema struct {
	WorkerID  string              `json:"worker_id"`
	MachineID string              `json:"machine_id"`
	Status    string              `json:"status"`
	PoolName  string              `json:"pool_name"`
	Reason    DeletedWorkerReason `json:"reason"`
}

type DeletedWorkerReason string

func (d DeletedWorkerReason) String() string {
	return string(d)
}

const (
	DeletedWorkerReasonPodWithoutState            DeletedWorkerReason = "pod_without_state"
	DeletedWorkerReasonPodExceededPendingAgeLimit DeletedWorkerReason = "pod_exceeded_pending_age_limit"
)

var EventStubSchemaVersion = "1.0"

type EventStubSchema struct {
	ID          string   `json:"id"`
	StubType    StubType `json:"stub_type"`
	WorkspaceID string   `json:"workspace_id"`
	StubConfig  string   `json:"stub_config"`
}

var EventTaskSchemaVersion = "1.0"

type EventTaskSchema struct {
	ID          string     `json:"id"`
	Status      TaskStatus `json:"status"`
	ContainerID string     `json:"container_id"`
	StartedAt   *time.Time `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at"`
	WorkspaceID string     `json:"workspace_id"`
	StubID      string     `json:"stub_id"`
	CreatedAt   time.Time  `json:"created_at"`
}

var EventStubStateSchemaVersion = "1.0"

type EventStubStateSchema struct {
	ID               string   `json:"id"`
	WorkspaceID      string   `json:"workspace_id"`
	State            string   `json:"state"`
	PreviousState    string   `json:"previous_state"`
	Reason           string   `json:"reason"`
	FailedContainers []string `json:"failed_containers"`
}

var EventWorkerPoolStateSchemaVersion = "1.0"

type EventWorkerPoolStateSchema struct {
	PoolName  string           `json:"pool_name"`
	Reasons   []string         `json:"reasons"`
	Status    string           `json:"status"`
	PoolState *WorkerPoolState `json:"pool_state"`
}
