package types

import (
	"encoding/json"
	"strings"
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

	EventContainerLifecycle = "container.lifecycle"
	EventContainerMetrics   = "container.metrics"
	EventContainerEvent     = "container.event"
	EventContainerLog       = "container.log"
	EventPlatformLog        = "platform.log"
	EventWorkerLifecycle    = "worker.lifecycle"
	EventStubDeploy         = "stub.deploy"
	EventStubServe          = "stub.serve"
	EventStubRun            = "stub.run"
	EventStubClone          = "stub.clone"

	EventWorkerPoolDegraded = "workerpool.degraded"
	EventWorkerPoolHealthy  = "workerpool.healthy"

	EventGatewayEndpointCalled = "gateway.endpoint.called"
	EventLLMRoute              = "llm.route"

	EventComputePool      = "compute.pool"
	EventComputeJoinToken = "compute.join_token"
	EventComputeMachine   = "compute.machine"
	EventComputeTransport = "compute.transport"
	EventComputeRoute     = "compute.route"

	EventStubCacheRequiredContent = "stub.cache.required_content"
	EventPlatformCache            = "platform.cache"
)

var (
	EventWorkerLifecycleStarted = "started"
	EventWorkerLifecycleStopped = "stopped"
	EventWorkerLifecycleDeleted = "deleted"
)

// CacheContentKind identifies the source of a required-content report.
type CacheContentKind string

const (
	CacheContentKindClipV1       CacheContentKind = "clip_v1"
	CacheContentKindClipV2       CacheContentKind = "clip_v2"
	CacheContentKindVolume       CacheContentKind = "volume"
	CacheContentKindCheckpoint   CacheContentKind = "checkpoint"
	CacheContentKindDiskSnapshot CacheContentKind = "disk_snapshot"
)

// Platform cache audit statuses for EventPlatformCacheSchema.Status.
const (
	CacheAuditStatusMaterialized    = "materialized"
	CacheAuditStatusMiss            = "miss"
	CacheAuditStatusHostUnavailable = "host_unavailable"
	CacheAuditStatusOriginFailure   = "origin_failure"
	CacheAuditStatusReplicaFailure  = "replica_failure"
	CacheAuditStatusSkipped         = "skipped"
)

var EventStubCacheRequiredContentSchemaVersion = "1.0"
var EventPlatformCacheSchemaVersion = "1.0"

// CacheRequiredContentItem describes a single piece of content a stub needs.
// Locality-scoped recent-stub indexes decide where it is kept warm. Source is a
// non-secret descriptor (OCI layer identity or workspace object path) used to
// resolve an origin fetch; it never carries credentials.
type CacheRequiredContentItem struct {
	Hash               string           `json:"hash"`
	RoutingKey         string           `json:"routing_key"`
	SizeBytes          int64            `json:"size_bytes,omitempty"`
	ExpectedHash       string           `json:"expected_hash,omitempty"`
	ImageID            string           `json:"image_id,omitempty"`
	Source             string           `json:"source,omitempty"`
	SourceBucket       string           `json:"source_bucket,omitempty"`
	Kind               CacheContentKind `json:"kind,omitempty"`
	CheckpointID       string           `json:"checkpoint_id,omitempty"`
	Accelerator        string           `json:"accelerator,omitempty"`
	DiskName           string           `json:"disk_name,omitempty"`
	SnapshotGeneration int64            `json:"snapshot_generation,omitempty"`
}

// EventStubCacheRequiredContentSchema is the coalesced required-content report
// for a stub, persisted to the stub cache stream in S2. Locality records where
// the content was observed; the reconciler uses the locality-scoped recent-stub
// index to decide where to materialize it.
type EventStubCacheRequiredContentSchema struct {
	WorkspaceID string                     `json:"workspace_id"`
	StubID      string                     `json:"stub_id"`
	Locality    string                     `json:"locality"`
	Kind        CacheContentKind           `json:"kind"`
	ItemCount   int                        `json:"item_count"`
	TotalBytes  int64                      `json:"total_bytes"`
	Items       []CacheRequiredContentItem `json:"items"`
	Timestamp   time.Time                  `json:"timestamp"`
}

// EventPlatformCacheSchema is an operational/audit event for cache
// materialization status, persisted to the platform cache stream in S2.
type EventPlatformCacheSchema struct {
	Locality            string           `json:"locality"`
	LogicalHost         string           `json:"logical_host,omitempty"`
	WorkspaceID         string           `json:"workspace_id,omitempty"`
	StubID              string           `json:"stub_id,omitempty"`
	WorkerID            string           `json:"worker_id,omitempty"`
	MachineID           string           `json:"machine_id,omitempty"`
	PoolName            string           `json:"pool_name,omitempty"`
	NodeID              string           `json:"node_id,omitempty"`
	Hash                string           `json:"hash,omitempty"`
	RoutingKey          string           `json:"routing_key,omitempty"`
	Kind                CacheContentKind `json:"kind,omitempty"`
	Status              string           `json:"status"`
	Operation           string           `json:"operation,omitempty"`
	Source              string           `json:"source,omitempty"`
	Message             string           `json:"message,omitempty"`
	CachePath           string           `json:"cache_path,omitempty"`
	SizeBytes           int64            `json:"size_bytes,omitempty"`
	FreedBytes          int64            `json:"freed_bytes,omitempty"`
	ProtectedFreedBytes int64            `json:"protected_freed_bytes,omitempty"`
	EvictedObjects      int              `json:"evicted_objects,omitempty"`
	ProtectedObjects    int              `json:"protected_objects,omitempty"`
	UsagePct            float64          `json:"usage_pct,omitempty"`
	WatermarkPct        float64          `json:"watermark_pct,omitempty"`
	AvailableBytes      uint64           `json:"available_bytes,omitempty"`
	ReserveBytes        int64            `json:"reserve_bytes,omitempty"`
	TargetFreeBytes     int64            `json:"target_free_bytes,omitempty"`
	TotalCandidates     int              `json:"total_candidates,omitempty"`
	ProtectedCandidates int              `json:"protected_candidates,omitempty"`
	RecentCandidates    int              `json:"recent_candidates,omitempty"`
	EligibleCandidates  int              `json:"eligible_candidates,omitempty"`
	Timestamp           time.Time        `json:"timestamp"`
}

// Schema versions should be in ISO 8601 format

var EventContainerMetricsSchemaVersion = "1.0"

type EventContainerMetricsSchema struct {
	WorkerID         string                    `json:"worker_id"`
	MachineID        string                    `json:"machine_id,omitempty"`
	ContainerID      string                    `json:"container_id"`
	WorkspaceID      string                    `json:"workspace_id"`
	StubID           string                    `json:"stub_id"`
	StubType         string                    `json:"stub_type,omitempty"`
	AppID            string                    `json:"app_id,omitempty"`
	CPU              int64                     `json:"cpu,omitempty"`
	GPUCount         uint32                    `json:"gpu_count,omitempty"`
	ContainerMetrics EventContainerMetricsData `json:"metrics"`
}

type EventContainerMetricsData struct {
	SampleIntervalMs   int64   `json:"sample_interval_ms,omitempty"`
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

var EventWorkerLifecycleSchemaVersion = "1.0"

type EventWorkerLifecycleSchema struct {
	WorkerID  string              `json:"worker_id"`
	MachineID string              `json:"machine_id"`
	Status    string              `json:"status"`
	PoolName  string              `json:"pool_name"`
	Reason    DeletedWorkerReason `json:"reason"`
}

var EventComputeSchemaVersion = "1.0"

const (
	EventComputeActionPoolReserved               = "pool.reserved"
	EventComputeActionPoolCreated                = "pool.created"
	EventComputeActionPoolDeleted                = "pool.deleted"
	EventComputeActionPoolExtended               = "pool.extended"
	EventComputeActionPoolCreditExhausted        = "pool.credit_exhausted"
	EventComputeActionPoolBillingDegraded        = "pool.billing_degraded"
	EventComputeActionReservationUsageRecorded   = "reservation.usage_recorded"
	EventComputeActionReservationRenewed         = "reservation.renewed"
	EventComputeActionReservationTerminating     = "reservation.terminating"
	EventComputeActionReservationStatusUpdated   = "reservation.status_updated"
	EventComputeActionPoolHeartbeat              = "pool.heartbeat"
	EventComputeActionJoinTokenCreated           = "join_token.created"
	EventComputeActionJoinTokenRevoked           = "join_token.revoked"
	EventComputeActionMachineJoined              = "machine.joined"
	EventComputeActionMachineHeartbeat           = "machine.heartbeat"
	EventComputeActionMachineDisconnected        = "machine.disconnected"
	EventComputeActionMachineReleased            = "machine.released"
	EventComputeActionMachineAvailabilityUpdated = "machine.availability_updated"
	EventComputeActionWorkerSlotCreated          = "worker_slot.created"
	EventComputeActionWorkerSlotPruned           = "worker_slot.pruned"
	EventComputeActionWorkerDisabled             = "worker.disabled"
	EventComputeActionTransportCredentialVended  = "transport.credential_vended"
	EventComputeActionTransportPrewarm           = "transport.prewarm"
	EventComputeActionTransportSnapshot          = "transport.snapshot"
	EventComputeActionRouteStatusUpdated         = "route.status_updated"
)

const (
	EventComputeAttrAvailableWorkerCount     = "available_worker_count"
	EventComputeAttrContainerCount           = "container_count"
	EventComputeAttrCPUUtilizationPct        = "cpu_utilization_pct"
	EventComputeAttrDiskPath                 = "disk_path"
	EventComputeAttrDiskTotalMB              = "disk_total_mb"
	EventComputeAttrDiskUsagePct             = "disk_usage_pct"
	EventComputeAttrDiskUsedMB               = "disk_used_mb"
	EventComputeAttrFreeGPUCount             = "free_gpu_count"
	EventComputeAttrHostCPUUtilizationPct    = "host_cpu_utilization_pct"
	EventComputeAttrHostMemoryUsedMB         = "host_memory_used_mb"
	EventComputeAttrHostMemoryUtilizationPct = "host_memory_utilization_pct"
	EventComputeAttrHourlyCostMicros         = "hourly_cost_micros"
	EventComputeAttrMemoryUsedMB             = "memory_used_mb"
	EventComputeAttrMemoryUtilizationPct     = "memory_utilization_pct"
	EventComputeAttrPendingContainerCount    = "pending_container_count"
	EventComputeAttrPendingWorkerCount       = "pending_worker_count"
	EventComputeAttrPoolMode                 = "pool_mode"
	EventComputeAttrSchedulingLatencyMs      = "scheduling_latency_ms"
	EventComputeAttrTransport                = "transport"
	EventComputeAttrWorkerCount              = "worker_count"
)

type EventComputeSchema struct {
	Timestamp    time.Time         `json:"timestamp"`
	WorkspaceID  string            `json:"workspace_id,omitempty"`
	PoolName     string            `json:"pool_name,omitempty"`
	MachineID    string            `json:"machine_id,omitempty"`
	WorkerID     string            `json:"worker_id,omitempty"`
	ContainerID  string            `json:"container_id,omitempty"`
	RouteID      string            `json:"route_id,omitempty"`
	Action       string            `json:"action"`
	Status       string            `json:"status,omitempty"`
	Transport    string            `json:"transport,omitempty"`
	Executor     string            `json:"executor,omitempty"`
	Fallback     string            `json:"fallback,omitempty"`
	Source       string            `json:"source,omitempty"`
	Hostname     string            `json:"hostname,omitempty"`
	OS           string            `json:"os,omitempty"`
	Arch         string            `json:"arch,omitempty"`
	CPUCount     uint32            `json:"cpu_count,omitempty"`
	MemoryMB     uint64            `json:"memory_mb,omitempty"`
	GPUCount     uint32            `json:"gpu_count,omitempty"`
	NodeCount    uint32            `json:"node_count,omitempty"`
	MachineCount uint32            `json:"machine_count,omitempty"`
	GPUs         []string          `json:"gpus,omitempty"`
	Schedulable  *bool             `json:"schedulable,omitempty"`
	Message      string            `json:"message,omitempty"`
	Attrs        map[string]string `json:"attrs,omitempty"`
}

type DeletedWorkerReason string

func (d DeletedWorkerReason) String() string {
	return string(d)
}

const (
	DeletedWorkerReasonPodWithoutState            DeletedWorkerReason = "pod_without_state"
	DeletedWorkerReasonPodExceededPendingAgeLimit DeletedWorkerReason = "pod_exceeded_pending_age_limit"
	DeletedWorkerReasonPodCompleted               DeletedWorkerReason = "pod_completed"
	DeletedWorkerReasonWorkerStateWithoutJob      DeletedWorkerReason = "worker_state_without_job"
)

var EventStubSchemaVersion = "1.0"

type EventStubSchema struct {
	ID           string   `json:"id"`
	StubType     StubType `json:"stub_type"`
	WorkspaceID  string   `json:"workspace_id"`
	StubConfig   string   `json:"stub_config"`
	ParentStubID string   `json:"parent_stub_id"`
}

var EventTaskSchemaVersion = "1.0"

type EventTaskSchema struct {
	ID                  string     `json:"id"`
	Status              TaskStatus `json:"status"`
	FailureReason       string     `json:"failure_reason,omitempty"`
	ContainerID         string     `json:"container_id"`
	StartedAt           *time.Time `json:"started_at"`
	EndedAt             *time.Time `json:"ended_at"`
	WorkspaceID         string     `json:"workspace_id"`
	ExternalWorkspaceID string     `json:"external_workspace_id"`
	StubID              string     `json:"stub_id"`
	StubType            StubType   `json:"stub_type,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	AppID               string     `json:"app_id"`
	DeploymentID        string     `json:"deployment_id,omitempty"`
	DeploymentName      string     `json:"deployment_name,omitempty"`
	DeploymentVersion   string     `json:"deployment_version,omitempty"`
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

var EventGatewayEndpointSchemaVersion = "1.0"

type EventGatewayEndpointSchema struct {
	Method       string `json:"method"`
	Path         string `json:"path"`
	WorkspaceID  string `json:"workspace_id,omitempty"`
	StatusCode   int    `json:"status_code"`
	UserAgent    string `json:"user_agent,omitempty"`
	RemoteIP     string `json:"remote_ip,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	Accept       string `json:"accept,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type EventDomain string

const (
	EventDomainGateway    EventDomain = "gateway"
	EventDomainScheduler  EventDomain = "scheduler"
	EventDomainWorker     EventDomain = "worker"
	EventDomainImage      EventDomain = "image"
	EventDomainClip       EventDomain = "clip"
	EventDomainMount      EventDomain = "mount"
	EventDomainNetwork    EventDomain = "network"
	EventDomainRuntime    EventDomain = "runtime"
	EventDomainRunner     EventDomain = "runner"
	EventDomainLogs       EventDomain = "logs"
	EventDomainResult     EventDomain = "result"
	EventDomainServe      EventDomain = "serve"
	EventDomainShutdown   EventDomain = "shutdown"
	EventDomainTask       EventDomain = "task"
	EventDomainAutoscaler EventDomain = "autoscaler"
)

type EventSource string

const (
	EventSourceScheduler                EventSource = "scheduler"
	EventSourceSchedulerStop            EventSource = "scheduler.stop"
	EventSourceGatewayAttach            EventSource = "gateway.attach"
	EventSourceGatewayStartTask         EventSource = "gateway.start_task"
	EventSourceGatewayEndTask           EventSource = "gateway.end_task"
	EventSourceGatewayFunctionStream    EventSource = "gateway.function_stream"
	EventSourceGatewayFunctionGetArgs   EventSource = "gateway.function_get_args"
	EventSourceGatewayFunctionSetResult EventSource = "gateway.function_set_result"
	EventSourceGatewayStopTask          EventSource = "gateway.stop_task"
	EventSourceAPITaskStop              EventSource = "api.task.stop"
	EventSourceEndpointAutoscaler       EventSource = "endpoint.autoscaler"
	EventSourceWorkerEventBus           EventSource = "worker.event_bus"
	EventSourceWorkerEventStream        EventSource = "worker.event_stream"
	EventSourceWorkerLogger             EventSource = "worker.logger"
	EventSourceWorkerNetwork            EventSource = "worker.network"
	EventSourceWorkerRuntime            EventSource = "worker.runtime"
	EventSourceWorkerStatusHeartbeat    EventSource = "worker.status_heartbeat"
	EventSourceRunnerStdout             EventSource = "runner.stdout"
	EventSourceClipFUSE                 EventSource = "clip.fuse"
)

func (s EventSource) String() string {
	return string(s)
}

type EventMessage string

const (
	EventMessageSchedulerStopRequested         EventMessage = "scheduler received stop request"
	EventMessageAttachStreamEnded              EventMessage = "container stream ended during attach"
	EventMessageAttachClientDisconnected       EventMessage = "client attach stream disconnected"
	EventMessageServeLockPreserved             EventMessage = "attach stream ended without deleting serve lock"
	EventMessageAutoscalerScaleDecision        EventMessage = "endpoint autoscaler selected desired container count"
	EventMessageRunnerCalledStartTask          EventMessage = "runner called start_task"
	EventMessageRunnerLoadedFunctionArgs       EventMessage = "runner loaded function args"
	EventMessageFunctionResultStored           EventMessage = "function result stored in redis"
	EventMessageFunctionResultLoadedByGateway  EventMessage = "function result loaded by gateway"
	EventMessageFunctionResultSentToClient     EventMessage = "function result sent to client"
	EventMessageFunctionStreamCancelRequested  EventMessage = "function stream client disconnected before completion"
	EventMessageFunctionStreamCancelApplied    EventMessage = "function stream cancellation applied"
	EventMessageTaskEndStatePersisted          EventMessage = "task end state persisted"
	EventMessageGatewayTaskStopRequested       EventMessage = "gateway task stop requested"
	EventMessageGatewayTaskCancellationApplied EventMessage = "gateway task cancellation applied"
	EventMessageHTTPTaskStopRequested          EventMessage = "HTTP task cancellation requested"
	EventMessageHTTPTaskCancellationApplied    EventMessage = "HTTP task cancellation applied"
	EventMessageLogCaptureFlushed              EventMessage = "container log capture flushed"
	EventMessageLogBufferDroppedRateLimit      EventMessage = "container log buffer dropped a rate limit message"
	EventMessageLogBufferDroppedMessage        EventMessage = "container log buffer dropped a message"
	EventMessageLogBufferDroppedRawMessage     EventMessage = "container log buffer dropped a raw message"
	EventMessageLogCaptureReceivedFirstByte    EventMessage = "container log capture received first byte"
	EventMessageLogCaptureReceivedFinalByte    EventMessage = "container log capture received final byte"
	EventMessageWorkerStopEventReceived        EventMessage = "worker received stop event"
	EventMessageWorkerOrphanStateMissing       EventMessage = "container state was missing during worker heartbeat"
	EventMessagePendingReconciledRunning       EventMessage = "pending state reconciled to running after runtime start"
	EventMessageStoppingGraceKill              EventMessage = "container exceeded stopping grace period and will be force killed"
	EventMessageRuntimeExited                  EventMessage = "runtime process exited"
	EventMessageRuntimeOOMKilled               EventMessage = "runtime process was oom killed"
)

func (m EventMessage) String() string {
	return string(m)
}

const (
	EventAttrBytes              = "bytes"
	EventAttrCause              = "cause"
	EventAttrContainerID        = "container_id"
	EventAttrContainerStatus    = "container_status"
	EventAttrCurrentContainers  = "current_containers"
	EventAttrDesiredContainers  = "desired_containers"
	EventAttrAttempts           = "attempts"
	EventAttrExitCode           = "exit_code"
	EventAttrError              = "error"
	EventAttrFailureClass       = "failure_class"
	EventAttrFailureClasses     = "failure_classes"
	EventAttrFailureCount       = "failure_count"
	EventAttrFirstTCPReadyMs    = "first_tcp_ready_ms"
	EventAttrForce              = "force"
	EventAttrGracePeriodSeconds = "grace_period_seconds"
	EventAttrLifecycle          = "lifecycle"
	EventAttrLastError          = "last_error"
	EventAttrLockKey            = "lock_key"
	EventAttrMode               = "mode"
	EventAttrPreviousStatus     = "previous_status"
	EventAttrMappedExitCode     = "mapped_exit_code"
	EventAttrOOMKilled          = "oom_killed"
	EventAttrRawExitCode        = "raw_exit_code"
	EventAttrReason             = "reason"
	EventAttrRuntime            = "runtime"
	EventAttrRuntimePID         = "runtime_pid"
	EventAttrExitReason         = "exit_reason"
	EventAttrSource             = "source"
	EventAttrStatus             = "status"
	EventAttrTaskID             = "task_id"
	EventAttrPoolSelector       = "pool_selector"
	EventAttrTimeoutSeconds     = "timeout_seconds"
	EventAttrTotalRequests      = "total_requests"
	EventAttrDurationUs         = "duration_us"
	EventAttrDurationNs         = "duration_ns"
)

const (
	EventCauseClientContextDone = "client_context_done"
	EventLogStreamStdout        = "stdout"
	EventLogStreamStderr        = "stderr"
	EventStreamSourceContainer  = "container_stream"
	EventStreamSourceClient     = "client_stream"
	RunnerEventTypeLifecycle    = "lifecycle"
	RunnerEventTypeEvent        = "event"
)

const (
	FunctionLifecycleCheckpointContainerRequestReady = "container_request_ready"
	FunctionLifecycleCheckpointStartTask             = "start_task"
	FunctionLifecycleCheckpointGetArgs               = "get_args"
	FunctionLifecycleCheckpointSetResult             = "set_result"
)

type ContainerEventOptions struct {
	Source  EventSource
	Message EventMessage
	Reason  string
	TaskID  string
	Attrs   map[string]string
}

type ContainerLifecycleOptions struct {
	Source EventSource
	TaskID string
	Attrs  map[string]string
}

type ContainerRunnerEvent struct {
	Type        string            `json:"type"`
	ID          string            `json:"id"`
	Timestamp   string            `json:"timestamp"`
	StartTime   string            `json:"start_time"`
	EndTime     string            `json:"end_time"`
	DurationMs  int64             `json:"duration_ms"`
	Success     *bool             `json:"success"`
	ContainerID string            `json:"container_id"`
	StubID      string            `json:"stub_id"`
	StubType    string            `json:"stub_type"`
	TaskID      string            `json:"task_id"`
	Message     string            `json:"message"`
	Attrs       map[string]string `json:"attrs"`
}

type ContainerLifecycleID string

const (
	ContainerLifecycleStartup                     ContainerLifecycleID = "container.startup"
	ContainerLifecycleSchedulerQueuePush          ContainerLifecycleID = "scheduler.queue_push"
	ContainerLifecycleSchedulerBacklogWait        ContainerLifecycleID = "scheduler.backlog_wait"
	ContainerLifecycleSchedulerWorkerList         ContainerLifecycleID = "scheduler.worker_list"
	ContainerLifecycleSchedulerBatchPlan          ContainerLifecycleID = "scheduler.batch_plan"
	ContainerLifecycleSchedulerWorkerSelection    ContainerLifecycleID = "scheduler.worker_selection"
	ContainerLifecycleSchedulerReserveCapacity    ContainerLifecycleID = "scheduler.reserve_capacity"
	ContainerLifecycleSchedulerReservation        ContainerLifecycleID = "scheduler.reservation"
	ContainerLifecycleSchedulerProvisionWorker    ContainerLifecycleID = "scheduler.provision_worker"
	ContainerLifecycleSchedulerImageCredentials   ContainerLifecycleID = "scheduler.attach_image_credentials"
	ContainerLifecycleSchedulerBuildCredentials   ContainerLifecycleID = "scheduler.attach_build_registry_credentials"
	ContainerLifecycleSchedulerWorkerQueuePush    ContainerLifecycleID = "scheduler.worker_queue_push"
	ContainerLifecycleWorkerQueueReceive          ContainerLifecycleID = "worker.queue_receive"
	ContainerLifecycleImageLoad                   ContainerLifecycleID = "image.load"
	ContainerLifecycleImageEmbeddedCacheMetadata  ContainerLifecycleID = "image.embedded_cache_metadata_copy"
	ContainerLifecycleImageEmbeddedCacheStore     ContainerLifecycleID = "image.embedded_cache_store"
	ContainerLifecycleImageEmbeddedCacheWait      ContainerLifecycleID = "image.embedded_cache_wait"
	ContainerLifecycleImageEmbeddedCacheRestore   ContainerLifecycleID = "image.embedded_cache_restore"
	ContainerLifecycleImageV1DataCacheDeferred    ContainerLifecycleID = "image.v1_data_cache_deferred"
	ContainerLifecycleImageV1DataCacheRestore     ContainerLifecycleID = "image.v1_data_cache_restore"
	ContainerLifecycleSetWorkerAddress            ContainerLifecycleID = "worker.set_worker_address"
	ContainerLifecyclePortAllocation              ContainerLifecycleID = "worker.port_allocation"
	ContainerLifecycleReadBundleConfig            ContainerLifecycleID = "worker.read_bundle_config"
	ContainerLifecycleSetupMounts                 ContainerLifecycleID = "mount.setup"
	ContainerLifecycleSpecFromRequest             ContainerLifecycleID = "worker.spec_from_request"
	ContainerLifecycleSetContainerAddr            ContainerLifecycleID = "worker.set_container_address"
	ContainerLifecycleSetAddressMap               ContainerLifecycleID = "worker.set_address_map"
	ContainerLifecycleOverlaySetup                ContainerLifecycleID = "mount.overlay_setup"
	ContainerLifecycleNetworkSetup                ContainerLifecycleID = "network.setup"
	ContainerLifecycleNetworkCreateVeth           ContainerLifecycleID = "network.create_veth"
	ContainerLifecycleNetworkSetupBridge          ContainerLifecycleID = "network.setup_bridge"
	ContainerLifecycleNetworkCreateNamespace      ContainerLifecycleID = "network.create_namespace"
	ContainerLifecycleNetworkConfigureNamespace   ContainerLifecycleID = "network.configure_namespace"
	ContainerLifecycleNetworkIPLock               ContainerLifecycleID = "network.ip_lock"
	ContainerLifecycleNetworkIPLoad               ContainerLifecycleID = "network.ip_load"
	ContainerLifecycleNetworkIPAssign             ContainerLifecycleID = "network.ip_assign"
	ContainerLifecycleNetworkSetContainerIP       ContainerLifecycleID = "network.set_container_ip"
	ContainerLifecycleNetworkRestrictions         ContainerLifecycleID = "network.restrictions"
	ContainerLifecycleGPUAssignment               ContainerLifecycleID = "worker.gpu_assignment"
	ContainerLifecycleNetworkExpose               ContainerLifecycleID = "network.expose_ports"
	ContainerLifecycleRuntimePrepare              ContainerLifecycleID = "runtime.prepare"
	ContainerLifecycleConfigWrite                 ContainerLifecycleID = "runtime.config_write"
	ContainerLifecycleStartQueueWait              ContainerLifecycleID = "worker.start_queue_wait"
	ContainerLifecycleRuntimeStartToPID           ContainerLifecycleID = "runtime.start_to_pid"
	ContainerLifecycleSandboxApplyCPUQuota        ContainerLifecycleID = "sandbox.apply_cpu_quota"
	ContainerLifecycleRunnerApplyCPUQuota         ContainerLifecycleID = "runner.apply_cpu_quota"
	ContainerLifecycleSandboxProcessManagerTCP    ContainerLifecycleID = "sandbox.process_manager_tcp_ready"
	ContainerLifecycleSandboxProcessManagerReady  ContainerLifecycleID = "sandbox.process_manager_ready"
	ContainerLifecycleServeReady                  ContainerLifecycleID = "serve.ready"
	ContainerLifecycleResultDelivery              ContainerLifecycleID = "result.delivery"
	ContainerLifecycleContainerRequestToStartTask ContainerLifecycleID = "function.container_request_to_start_task"
	ContainerLifecycleContainerRunningToStartTask ContainerLifecycleID = "container.running_to_start_task"
	ContainerLifecycleRunnerStartToGetArgs        ContainerLifecycleID = "runner.start_task_to_get_args"
	ContainerLifecycleRunnerGetArgsToSetResult    ContainerLifecycleID = "runner.get_args_to_set_result"
	ContainerLifecycleRunnerStartToSetResult      ContainerLifecycleID = "runner.start_task_to_set_result"
	ContainerLifecycleResultSetToEndTask          ContainerLifecycleID = "result.set_result_to_end_task"
	ContainerLifecycleRunnerStartToEndTask        ContainerLifecycleID = "runner.start_task_to_end_task"
	ContainerLifecycleRunnerGatewayChannelOpen    ContainerLifecycleID = "runner.gateway_channel_open"
	ContainerLifecycleRunnerStartTaskRPC          ContainerLifecycleID = "runner.start_task_rpc"
	ContainerLifecycleRunnerGetArgsRPC            ContainerLifecycleID = "runner.get_args_rpc"
	ContainerLifecycleRunnerUserCodeImport        ContainerLifecycleID = "runner.user_code_import"
	ContainerLifecycleRunnerHandlerExecution      ContainerLifecycleID = "runner.handler_execution"
	ContainerLifecycleRunnerSetResultRPC          ContainerLifecycleID = "runner.set_result_rpc"
	ContainerLifecycleRunnerEndTaskRPC            ContainerLifecycleID = "runner.end_task_rpc"
	ContainerLifecycleClipRead                    ContainerLifecycleID = "clip.read"
	ContainerLifecycleClipOCIRead                 ContainerLifecycleID = "clip.oci_read"
	ContainerLifecycleClipArchiveRead             ContainerLifecycleID = "clip.archive_read"
	ContainerLifecycleClipDiskCacheRead           ContainerLifecycleID = "clip.disk_cache_read"
	ContainerLifecycleClipContentCacheRead        ContainerLifecycleID = "clip.content_cache_read"
	ContainerLifecycleClipCheckpointRead          ContainerLifecycleID = "clip.checkpoint_read"
	ContainerLifecycleClipLayerDecompress         ContainerLifecycleID = "clip.layer_decompress"
	ContainerLifecycleClipLayerDecompressWait     ContainerLifecycleID = "clip.layer_decompress_wait"
)

type ContainerLifecycleDefinition struct {
	ID       ContainerLifecycleID `json:"id"`
	Domain   EventDomain          `json:"domain"`
	ParentID ContainerLifecycleID `json:"parent_id,omitempty"`
	Label    string               `json:"label"`
	Required bool                 `json:"required"`
}

var ContainerLifecycleDefinitions = map[ContainerLifecycleID]ContainerLifecycleDefinition{
	ContainerLifecycleStartup:                     {ID: ContainerLifecycleStartup, Domain: EventDomainRuntime, Label: "Container startup", Required: true},
	ContainerLifecycleSchedulerQueuePush:          {ID: ContainerLifecycleSchedulerQueuePush, Domain: EventDomainScheduler, ParentID: ContainerLifecycleStartup, Label: "Queue request"},
	ContainerLifecycleSchedulerBacklogWait:        {ID: ContainerLifecycleSchedulerBacklogWait, Domain: EventDomainScheduler, ParentID: ContainerLifecycleStartup, Label: "Scheduler backlog wait"},
	ContainerLifecycleSchedulerWorkerList:         {ID: ContainerLifecycleSchedulerWorkerList, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Load workers"},
	ContainerLifecycleSchedulerBatchPlan:          {ID: ContainerLifecycleSchedulerBatchPlan, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Plan batch"},
	ContainerLifecycleSchedulerWorkerSelection:    {ID: ContainerLifecycleSchedulerWorkerSelection, Domain: EventDomainScheduler, ParentID: ContainerLifecycleStartup, Label: "Worker selection"},
	ContainerLifecycleSchedulerReserveCapacity:    {ID: ContainerLifecycleSchedulerReserveCapacity, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Reserve capacity"},
	ContainerLifecycleSchedulerReservation:        {ID: ContainerLifecycleSchedulerReservation, Domain: EventDomainScheduler, ParentID: ContainerLifecycleStartup, Label: "Capacity reservation"},
	ContainerLifecycleSchedulerProvisionWorker:    {ID: ContainerLifecycleSchedulerProvisionWorker, Domain: EventDomainScheduler, ParentID: ContainerLifecycleStartup, Label: "Worker provisioning"},
	ContainerLifecycleSchedulerImageCredentials:   {ID: ContainerLifecycleSchedulerImageCredentials, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Attach image credentials"},
	ContainerLifecycleSchedulerBuildCredentials:   {ID: ContainerLifecycleSchedulerBuildCredentials, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Attach build registry credentials"},
	ContainerLifecycleSchedulerWorkerQueuePush:    {ID: ContainerLifecycleSchedulerWorkerQueuePush, Domain: EventDomainScheduler, ParentID: ContainerLifecycleSchedulerBacklogWait, Label: "Push to worker queue"},
	ContainerLifecycleWorkerQueueReceive:          {ID: ContainerLifecycleWorkerQueueReceive, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Worker queue receive"},
	ContainerLifecycleImageLoad:                   {ID: ContainerLifecycleImageLoad, Domain: EventDomainImage, ParentID: ContainerLifecycleStartup, Label: "Image load", Required: true},
	ContainerLifecycleImageEmbeddedCacheMetadata:  {ID: ContainerLifecycleImageEmbeddedCacheMetadata, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "Embedded image cache metadata copy"},
	ContainerLifecycleImageEmbeddedCacheStore:     {ID: ContainerLifecycleImageEmbeddedCacheStore, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "Embedded image cache store"},
	ContainerLifecycleImageEmbeddedCacheWait:      {ID: ContainerLifecycleImageEmbeddedCacheWait, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "Wait for embedded image cache"},
	ContainerLifecycleImageEmbeddedCacheRestore:   {ID: ContainerLifecycleImageEmbeddedCacheRestore, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "Embedded image cache restore"},
	ContainerLifecycleImageV1DataCacheDeferred:    {ID: ContainerLifecycleImageV1DataCacheDeferred, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "Defer v1 data archive restore"},
	ContainerLifecycleImageV1DataCacheRestore:     {ID: ContainerLifecycleImageV1DataCacheRestore, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: "V1 data archive restore"},
	ContainerLifecycleSetWorkerAddress:            {ID: ContainerLifecycleSetWorkerAddress, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Set worker address"},
	ContainerLifecyclePortAllocation:              {ID: ContainerLifecyclePortAllocation, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Port allocation"},
	ContainerLifecycleReadBundleConfig:            {ID: ContainerLifecycleReadBundleConfig, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Read bundle config"},
	ContainerLifecycleSetupMounts:                 {ID: ContainerLifecycleSetupMounts, Domain: EventDomainMount, ParentID: ContainerLifecycleStartup, Label: "Setup mounts"},
	ContainerLifecycleSpecFromRequest:             {ID: ContainerLifecycleSpecFromRequest, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Spec from request"},
	ContainerLifecycleSetContainerAddr:            {ID: ContainerLifecycleSetContainerAddr, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Set container address"},
	ContainerLifecycleSetAddressMap:               {ID: ContainerLifecycleSetAddressMap, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Set address map"},
	ContainerLifecycleOverlaySetup:                {ID: ContainerLifecycleOverlaySetup, Domain: EventDomainMount, ParentID: ContainerLifecycleStartup, Label: "Overlay setup"},
	ContainerLifecycleNetworkSetup:                {ID: ContainerLifecycleNetworkSetup, Domain: EventDomainNetwork, ParentID: ContainerLifecycleStartup, Label: "Network setup"},
	ContainerLifecycleNetworkCreateVeth:           {ID: ContainerLifecycleNetworkCreateVeth, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkSetup, Label: "Create veth pair"},
	ContainerLifecycleNetworkSetupBridge:          {ID: ContainerLifecycleNetworkSetupBridge, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkSetup, Label: "Setup bridge"},
	ContainerLifecycleNetworkCreateNamespace:      {ID: ContainerLifecycleNetworkCreateNamespace, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkSetup, Label: "Create namespace"},
	ContainerLifecycleNetworkConfigureNamespace:   {ID: ContainerLifecycleNetworkConfigureNamespace, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkSetup, Label: "Configure namespace"},
	ContainerLifecycleNetworkIPLock:               {ID: ContainerLifecycleNetworkIPLock, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkConfigureNamespace, Label: "Acquire IP lock"},
	ContainerLifecycleNetworkIPLoad:               {ID: ContainerLifecycleNetworkIPLoad, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkConfigureNamespace, Label: "Load allocated IPs"},
	ContainerLifecycleNetworkIPAssign:             {ID: ContainerLifecycleNetworkIPAssign, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkConfigureNamespace, Label: "Assign container IP"},
	ContainerLifecycleNetworkSetContainerIP:       {ID: ContainerLifecycleNetworkSetContainerIP, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkConfigureNamespace, Label: "Persist container IP"},
	ContainerLifecycleNetworkRestrictions:         {ID: ContainerLifecycleNetworkRestrictions, Domain: EventDomainNetwork, ParentID: ContainerLifecycleNetworkSetup, Label: "Network restrictions"},
	ContainerLifecycleGPUAssignment:               {ID: ContainerLifecycleGPUAssignment, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "GPU assignment"},
	ContainerLifecycleNetworkExpose:               {ID: ContainerLifecycleNetworkExpose, Domain: EventDomainNetwork, ParentID: ContainerLifecycleStartup, Label: "Expose ports"},
	ContainerLifecycleRuntimePrepare:              {ID: ContainerLifecycleRuntimePrepare, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Runtime prepare"},
	ContainerLifecycleConfigWrite:                 {ID: ContainerLifecycleConfigWrite, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Config write"},
	ContainerLifecycleStartQueueWait:              {ID: ContainerLifecycleStartQueueWait, Domain: EventDomainWorker, ParentID: ContainerLifecycleStartup, Label: "Worker start queue wait"},
	ContainerLifecycleRuntimeStartToPID:           {ID: ContainerLifecycleRuntimeStartToPID, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Runtime start to PID", Required: true},
	ContainerLifecycleSandboxApplyCPUQuota:        {ID: ContainerLifecycleSandboxApplyCPUQuota, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Apply sandbox CPU quota"},
	ContainerLifecycleRunnerApplyCPUQuota:         {ID: ContainerLifecycleRunnerApplyCPUQuota, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Apply function CPU quota"},
	ContainerLifecycleSandboxProcessManagerTCP:    {ID: ContainerLifecycleSandboxProcessManagerTCP, Domain: EventDomainNetwork, ParentID: ContainerLifecycleSandboxProcessManagerReady, Label: "Sandbox process manager TCP ready"},
	ContainerLifecycleSandboxProcessManagerReady:  {ID: ContainerLifecycleSandboxProcessManagerReady, Domain: EventDomainRuntime, ParentID: ContainerLifecycleStartup, Label: "Sandbox process manager ready"},
	ContainerLifecycleServeReady:                  {ID: ContainerLifecycleServeReady, Domain: EventDomainServe, ParentID: ContainerLifecycleStartup, Label: "Serve ready"},
	ContainerLifecycleResultDelivery:              {ID: ContainerLifecycleResultDelivery, Domain: EventDomainResult, ParentID: ContainerLifecycleStartup, Label: "Result delivery"},
	ContainerLifecycleContainerRequestToStartTask: {ID: ContainerLifecycleContainerRequestToStartTask, Domain: EventDomainTask, Label: "Container request to runner start"},
	ContainerLifecycleContainerRunningToStartTask: {ID: ContainerLifecycleContainerRunningToStartTask, Domain: EventDomainRunner, Label: "Container running to runner start"},
	ContainerLifecycleRunnerStartToGetArgs:        {ID: ContainerLifecycleRunnerStartToGetArgs, Domain: EventDomainRunner, Label: "Runner start to get args"},
	ContainerLifecycleRunnerGetArgsToSetResult:    {ID: ContainerLifecycleRunnerGetArgsToSetResult, Domain: EventDomainRunner, Label: "Get args to set result"},
	ContainerLifecycleRunnerStartToSetResult:      {ID: ContainerLifecycleRunnerStartToSetResult, Domain: EventDomainRunner, Label: "Runner start to set result"},
	ContainerLifecycleResultSetToEndTask:          {ID: ContainerLifecycleResultSetToEndTask, Domain: EventDomainResult, Label: "Set result to end task"},
	ContainerLifecycleRunnerStartToEndTask:        {ID: ContainerLifecycleRunnerStartToEndTask, Domain: EventDomainRunner, Label: "Runner start to end task"},
	ContainerLifecycleRunnerGatewayChannelOpen:    {ID: ContainerLifecycleRunnerGatewayChannelOpen, Domain: EventDomainRunner, Label: "Open gateway channel"},
	ContainerLifecycleRunnerStartTaskRPC:          {ID: ContainerLifecycleRunnerStartTaskRPC, Domain: EventDomainRunner, Label: "StartTask RPC"},
	ContainerLifecycleRunnerGetArgsRPC:            {ID: ContainerLifecycleRunnerGetArgsRPC, Domain: EventDomainRunner, Label: "GetArgs RPC"},
	ContainerLifecycleRunnerUserCodeImport:        {ID: ContainerLifecycleRunnerUserCodeImport, Domain: EventDomainRunner, Label: "Import user code"},
	ContainerLifecycleRunnerHandlerExecution:      {ID: ContainerLifecycleRunnerHandlerExecution, Domain: EventDomainRunner, Label: "Run handler"},
	ContainerLifecycleRunnerSetResultRPC:          {ID: ContainerLifecycleRunnerSetResultRPC, Domain: EventDomainRunner, Label: "SetResult RPC"},
	ContainerLifecycleRunnerEndTaskRPC:            {ID: ContainerLifecycleRunnerEndTaskRPC, Domain: EventDomainRunner, Label: "EndTask RPC"},
	ContainerLifecycleClipRead:                    {ID: ContainerLifecycleClipRead, Domain: EventDomainClip, Label: "CLIP lazy read"},
	ContainerLifecycleClipOCIRead:                 {ID: ContainerLifecycleClipOCIRead, Domain: EventDomainClip, Label: "CLIP OCI lazy read"},
	ContainerLifecycleClipArchiveRead:             {ID: ContainerLifecycleClipArchiveRead, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP archive read"},
	ContainerLifecycleClipDiskCacheRead:           {ID: ContainerLifecycleClipDiskCacheRead, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP disk cache read"},
	ContainerLifecycleClipContentCacheRead:        {ID: ContainerLifecycleClipContentCacheRead, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP content cache read"},
	ContainerLifecycleClipCheckpointRead:          {ID: ContainerLifecycleClipCheckpointRead, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP checkpoint read"},
	ContainerLifecycleClipLayerDecompress:         {ID: ContainerLifecycleClipLayerDecompress, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP layer decompress"},
	ContainerLifecycleClipLayerDecompressWait:     {ID: ContainerLifecycleClipLayerDecompressWait, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: "CLIP layer decompress wait"},
}

type ContainerEventID string

const (
	ContainerEventSchedulerStopRequested    ContainerEventID = "scheduler.stop_requested"
	ContainerEventWorkerStopEventReceived   ContainerEventID = "worker.stop_event_received"
	ContainerEventWorkerOrphanStateMissing  ContainerEventID = "worker.orphan_state_missing"
	ContainerEventWorkerPendingReconciled   ContainerEventID = "worker.pending_reconciled_running"
	ContainerEventWorkerStoppingGraceKill   ContainerEventID = "worker.stopping_grace_kill"
	ContainerEventRuntimeExited             ContainerEventID = "runtime.exited"
	ContainerEventRuntimeOOMKilled          ContainerEventID = "runtime.oom_killed"
	ContainerEventGatewayAttachDisconnected ContainerEventID = "gateway.attach_disconnected"
	ContainerEventGatewayServeLockDeleted   ContainerEventID = "gateway.serve_lock_deleted"
	ContainerEventGatewayServeLockPreserved ContainerEventID = "gateway.serve_lock_preserved"
	ContainerEventAutoscalerScaleDecision   ContainerEventID = "autoscaler.scale_decision"
	ContainerEventTaskCancelRequested       ContainerEventID = "task.cancel_requested"
	ContainerEventTaskCancelApplied         ContainerEventID = "task.cancel_applied"
	ContainerEventLogsFirstByte             ContainerEventID = "logs.first_byte"
	ContainerEventLogsLastByte              ContainerEventID = "logs.last_byte"
	ContainerEventLogsFlushCompleted        ContainerEventID = "logs.flush_completed"
	ContainerEventLogsDropped               ContainerEventID = "logs.dropped"
	ContainerEventRunnerStartTask           ContainerEventID = "runner.start_task"
	ContainerEventRunnerGetArgs             ContainerEventID = "runner.get_args"
	ContainerEventRunnerProcessStarted      ContainerEventID = "runner.process_started"
	ContainerEventRunnerModuleLoaded        ContainerEventID = "runner.module_loaded"
	ContainerEventRunnerMainEntered         ContainerEventID = "runner.main_entered"
	ContainerEventResultSetResult           ContainerEventID = "result.set_result"
	ContainerEventResultEndTask             ContainerEventID = "result.end_task"
	ContainerEventResultLoadedByGateway     ContainerEventID = "result.loaded_by_gateway"
	ContainerEventResultSentToClient        ContainerEventID = "result.sent_to_client"
)

type ContainerEventDefinition struct {
	ID       ContainerEventID `json:"id"`
	Domain   EventDomain      `json:"domain"`
	Label    string           `json:"label"`
	Required bool             `json:"required"`
}

var ContainerEventDefinitions = map[ContainerEventID]ContainerEventDefinition{
	ContainerEventSchedulerStopRequested:    {ID: ContainerEventSchedulerStopRequested, Domain: EventDomainScheduler, Label: "Scheduler stop requested"},
	ContainerEventWorkerStopEventReceived:   {ID: ContainerEventWorkerStopEventReceived, Domain: EventDomainWorker, Label: "Worker stop event received"},
	ContainerEventWorkerOrphanStateMissing:  {ID: ContainerEventWorkerOrphanStateMissing, Domain: EventDomainWorker, Label: "Worker orphan state missing"},
	ContainerEventWorkerPendingReconciled:   {ID: ContainerEventWorkerPendingReconciled, Domain: EventDomainWorker, Label: "Pending reconciled to running"},
	ContainerEventWorkerStoppingGraceKill:   {ID: ContainerEventWorkerStoppingGraceKill, Domain: EventDomainWorker, Label: "Stopping grace kill"},
	ContainerEventRuntimeExited:             {ID: ContainerEventRuntimeExited, Domain: EventDomainRuntime, Label: "Runtime exited"},
	ContainerEventRuntimeOOMKilled:          {ID: ContainerEventRuntimeOOMKilled, Domain: EventDomainRuntime, Label: "Runtime OOM killed"},
	ContainerEventGatewayAttachDisconnected: {ID: ContainerEventGatewayAttachDisconnected, Domain: EventDomainGateway, Label: "Attach disconnected"},
	ContainerEventGatewayServeLockDeleted:   {ID: ContainerEventGatewayServeLockDeleted, Domain: EventDomainGateway, Label: "Serve lock deleted"},
	ContainerEventGatewayServeLockPreserved: {ID: ContainerEventGatewayServeLockPreserved, Domain: EventDomainGateway, Label: "Serve lock preserved"},
	ContainerEventAutoscalerScaleDecision:   {ID: ContainerEventAutoscalerScaleDecision, Domain: EventDomainAutoscaler, Label: "Autoscaler scale decision"},
	ContainerEventTaskCancelRequested:       {ID: ContainerEventTaskCancelRequested, Domain: EventDomainTask, Label: "Task cancel requested"},
	ContainerEventTaskCancelApplied:         {ID: ContainerEventTaskCancelApplied, Domain: EventDomainTask, Label: "Task cancel applied"},
	ContainerEventLogsFirstByte:             {ID: ContainerEventLogsFirstByte, Domain: EventDomainLogs, Label: "First log byte"},
	ContainerEventLogsLastByte:              {ID: ContainerEventLogsLastByte, Domain: EventDomainLogs, Label: "Last log byte"},
	ContainerEventLogsFlushCompleted:        {ID: ContainerEventLogsFlushCompleted, Domain: EventDomainLogs, Label: "Log flush completed"},
	ContainerEventLogsDropped:               {ID: ContainerEventLogsDropped, Domain: EventDomainLogs, Label: "Log dropped"},
	ContainerEventRunnerStartTask:           {ID: ContainerEventRunnerStartTask, Domain: EventDomainRunner, Label: "Runner start task"},
	ContainerEventRunnerGetArgs:             {ID: ContainerEventRunnerGetArgs, Domain: EventDomainRunner, Label: "Runner get args"},
	ContainerEventRunnerProcessStarted:      {ID: ContainerEventRunnerProcessStarted, Domain: EventDomainRunner, Label: "Runner process started"},
	ContainerEventRunnerModuleLoaded:        {ID: ContainerEventRunnerModuleLoaded, Domain: EventDomainRunner, Label: "Runner module loaded"},
	ContainerEventRunnerMainEntered:         {ID: ContainerEventRunnerMainEntered, Domain: EventDomainRunner, Label: "Runner main entered"},
	ContainerEventResultSetResult:           {ID: ContainerEventResultSetResult, Domain: EventDomainResult, Label: "Result set"},
	ContainerEventResultEndTask:             {ID: ContainerEventResultEndTask, Domain: EventDomainResult, Label: "End task"},
	ContainerEventResultLoadedByGateway:     {ID: ContainerEventResultLoadedByGateway, Domain: EventDomainResult, Label: "Result loaded by gateway"},
	ContainerEventResultSentToClient:        {ID: ContainerEventResultSentToClient, Domain: EventDomainResult, Label: "Result sent to client"},
}

var EventContainerLifecycleSchemaVersion = "1.0"

type EventContainerLifecycleSchema struct {
	ID          ContainerLifecycleID `json:"id"`
	Domain      EventDomain          `json:"domain"`
	ParentID    ContainerLifecycleID `json:"parent_id,omitempty"`
	StartTime   time.Time            `json:"start_time"`
	EndTime     time.Time            `json:"end_time,omitempty"`
	DurationMs  int64                `json:"duration_ms,omitempty"`
	ContainerID string               `json:"container_id,omitempty"`
	StubID      string               `json:"stub_id,omitempty"`
	StubType    string               `json:"stub_type,omitempty"`
	TaskID      string               `json:"task_id,omitempty"`
	WorkspaceID string               `json:"workspace_id,omitempty"`
	AppID       string               `json:"app_id,omitempty"`
	WorkerID    string               `json:"worker_id,omitempty"`
	MachineID   string               `json:"machine_id,omitempty"`
	Success     *bool                `json:"success,omitempty"`
	Source      string               `json:"source,omitempty"`
	Attrs       map[string]string    `json:"attrs,omitempty"`
}

var EventContainerEventSchemaVersion = "1.0"

type EventContainerEventSchema struct {
	ID          ContainerEventID  `json:"id"`
	Domain      EventDomain       `json:"domain"`
	Timestamp   time.Time         `json:"timestamp"`
	ContainerID string            `json:"container_id,omitempty"`
	StubID      string            `json:"stub_id,omitempty"`
	StubType    string            `json:"stub_type,omitempty"`
	TaskID      string            `json:"task_id,omitempty"`
	WorkspaceID string            `json:"workspace_id,omitempty"`
	AppID       string            `json:"app_id,omitempty"`
	WorkerID    string            `json:"worker_id,omitempty"`
	MachineID   string            `json:"machine_id,omitempty"`
	CPU         int64             `json:"cpu,omitempty"`
	GPUCount    uint32            `json:"gpu_count,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	Source      string            `json:"source,omitempty"`
	Message     string            `json:"message,omitempty"`
	Attrs       map[string]string `json:"attrs,omitempty"`
}

var EventContainerLogSchemaVersion = "1.0"

type EventContainerLogSchema struct {
	Timestamp   time.Time `json:"timestamp"`
	ContainerID string    `json:"container_id,omitempty"`
	StubID      string    `json:"stub_id,omitempty"`
	StubType    string    `json:"stub_type,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	AppID       string    `json:"app_id,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
	MachineID   string    `json:"machine_id,omitempty"`
	Stream      string    `json:"stream,omitempty"`
	Line        string    `json:"line"`
	PID         int32     `json:"pid,omitempty"`
	ProcessArgs []string  `json:"process_args,omitempty"`
	ProcessCwd  string    `json:"process_cwd,omitempty"`
	ProcessSeq  uint64    `json:"process_seq,omitempty"`
}

var EventPlatformLogSchemaVersion = "1.0"

type EventPlatformLogSchema struct {
	Timestamp   time.Time `json:"timestamp"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	PoolName    string    `json:"pool_name,omitempty"`
	MachineID   string    `json:"machine_id,omitempty"`
	Service     string    `json:"service,omitempty"`
	InstanceID  string    `json:"instance_id,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
	Level       string    `json:"level,omitempty"`
	Stream      string    `json:"stream,omitempty"`
	Line        string    `json:"line"`
}

var EventLLMRouteSchemaVersion = "1.0"

type EventLLMRouteSchema struct {
	Timestamp                time.Time `json:"timestamp"`
	WorkspaceID              string    `json:"workspace_id,omitempty"`
	AppID                    string    `json:"app_id,omitempty"`
	StubID                   string    `json:"stub_id,omitempty"`
	StubType                 string    `json:"stub_type,omitempty"`
	ContainerID              string    `json:"container_id,omitempty"`
	Method                   string    `json:"method,omitempty"`
	Path                     string    `json:"path,omitempty"`
	RequestID                string    `json:"request_id,omitempty"`
	Model                    string    `json:"model,omitempty"`
	Engine                   string    `json:"engine,omitempty"`
	RouteReason              string    `json:"route_reason,omitempty"`
	RouteScore               int64     `json:"route_score,omitempty"`
	CandidateCount           int       `json:"candidate_count,omitempty"`
	ReadyContainerCount      int       `json:"ready_container_count,omitempty"`
	SessionHash              string    `json:"session_hash,omitempty"`
	PrefixHash               string    `json:"prefix_hash,omitempty"`
	PrefixBlockCount         int       `json:"prefix_block_count,omitempty"`
	PrefixCacheMatches       int       `json:"prefix_cache_matches,omitempty"`
	PromptTokens             int64     `json:"prompt_tokens,omitempty"`
	OutputTokens             int64     `json:"output_tokens,omitempty"`
	TokenPressure            int64     `json:"token_pressure,omitempty"`
	Stream                   bool      `json:"stream,omitempty"`
	TotalActiveStreams       int64     `json:"total_active_streams,omitempty"`
	TotalTokenPressure       int64     `json:"total_token_pressure,omitempty"`
	ContainerActiveStreams   int64     `json:"container_active_streams,omitempty"`
	ContainerTokenPressure   int64     `json:"container_token_pressure,omitempty"`
	EngineRunningRequests    int64     `json:"engine_running_requests,omitempty"`
	EngineWaitingRequests    int64     `json:"engine_waiting_requests,omitempty"`
	EngineTTFTMs             int64     `json:"engine_ttft_ms,omitempty"`
	EngineTPOTMs             int64     `json:"engine_tpot_ms,omitempty"`
	EngineDecodeTokensPerS   int64     `json:"engine_decode_tokens_per_s,omitempty"`
	EngineGPUCacheUsageMilli int64     `json:"engine_gpu_cache_usage_milli,omitempty"`
	EnginePrefixCacheMilli   int64     `json:"engine_prefix_cache_hit_milli,omitempty"`
	QueueWaitMs              int64     `json:"queue_wait_ms,omitempty"`
	FirstResponseMs          int64     `json:"first_response_ms,omitempty"`
	DurationMs               int64     `json:"duration_ms,omitempty"`
	StatusCode               int       `json:"status_code,omitempty"`
	BackendError             bool      `json:"backend_error,omitempty"`
	ErrorMessage             string    `json:"error_message,omitempty"`
}

type EventQuery struct {
	Limit       uint64   `json:"limit,omitempty"`
	WorkspaceID string   `json:"workspace_id,omitempty"`
	StubID      string   `json:"stub_id,omitempty"`
	StubType    string   `json:"stub_type,omitempty"`
	AppID       string   `json:"app_id,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	ContainerID string   `json:"container_id,omitempty"`
	EventTypes  []string `json:"event_types,omitempty"`
	// ExcludeEventTypes drops records of the given event types (wildcards
	// supported); used so task reads from the multiplexed stub task stream do
	// not return log records unless explicitly requested.
	ExcludeEventTypes []string `json:"exclude_event_types,omitempty"`
	// ExcludeActions drops records whose payload "action" field matches; used
	// to keep dense actions (e.g. machine.heartbeat) from consuming the limit.
	ExcludeActions []string   `json:"exclude_actions,omitempty"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	SeqNum         *uint64    `json:"seq_num,omitempty"`
	Timestamp      *uint64    `json:"timestamp,omitempty"`
	TailOffset     *int64     `json:"tail_offset,omitempty"`
	Until          *uint64    `json:"until,omitempty"`
	WaitSeconds    *int32     `json:"wait,omitempty"`
	Clamp          *bool      `json:"clamp,omitempty"`
}

type LogQuery struct {
	Limit       uint64     `json:"limit,omitempty"`
	Page        uint64     `json:"page,omitempty"`
	ObjectID    string     `json:"object_id,omitempty"`
	ObjectType  string     `json:"object_type,omitempty"`
	WorkspaceID string     `json:"workspace_id,omitempty"`
	StubID      string     `json:"stub_id,omitempty"`
	AppID       string     `json:"app_id,omitempty"`
	TaskID      string     `json:"task_id,omitempty"`
	ContainerID string     `json:"container_id,omitempty"`
	MachineID   string     `json:"machine_id,omitempty"`
	WorkerID    string     `json:"worker_id,omitempty"`
	Query       string     `json:"query,omitempty"`
	StartTime   *time.Time `json:"start_time,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	SeqNum      *uint64    `json:"seq_num,omitempty"`
	TailOffset  *int64     `json:"tail_offset,omitempty"`
	WaitSeconds *int32     `json:"wait,omitempty"`
	Clamp       *bool      `json:"clamp,omitempty"`
}

const (
	GatewayObjectTypeDeployment = "BETA9_DEPLOYMENT"
	GatewayObjectTypeTask       = "BETA9_TASK"
	GatewayObjectTypeStub       = "BETA9_STUB"
	GatewayObjectTypeContainer  = "BETA9_CONTAINER"
	GatewayObjectTypeWorkspace  = "BETA9_WORKSPACE"
	GatewayObjectTypeApp        = "BETA9_APP"
	GatewayObjectTypeMachine    = "BETA9_MACHINE"
)

type LogRecord struct {
	SeqNum      uint64    `json:"seq_num"`
	StoredAtNs  uint64    `json:"stored_at_ns,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Message     string    `json:"message"`
	Stream      string    `json:"stream,omitempty"`
	ContainerID string    `json:"container_id,omitempty"`
	StubID      string    `json:"stub_id,omitempty"`
	StubType    string    `json:"stub_type,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	AppID       string    `json:"app_id,omitempty"`
	MachineID   string    `json:"machine_id,omitempty"`
	WorkerID    string    `json:"worker_id,omitempty"`
	PID         int32     `json:"pid,omitempty"`
	ProcessArgs []string  `json:"process_args,omitempty"`
	ProcessCwd  string    `json:"process_cwd,omitempty"`
	ProcessSeq  uint64    `json:"process_seq,omitempty"`
}

type LogsResponse struct {
	ObjectID      string      `json:"object_id,omitempty"`
	ObjectType    string      `json:"object_type,omitempty"`
	Logs          []LogRecord `json:"logs"`
	TotalExpected int         `json:"total_expected"`
	NextCursor    string      `json:"next_cursor,omitempty"`
	Streams       []string    `json:"streams,omitempty"`
}

type MetricAverage struct {
	Value float64 `json:"value"`
}

type MetricsAggregationBucket struct {
	Key                     int64         `json:"key"`
	KeyAsString             string        `json:"key_as_string"`
	DocCount                int           `json:"doc_count"`
	ContainerCount          MetricAverage `json:"container_count"`
	CPUConcurrency          MetricAverage `json:"cpu_concurrency"`
	GPUConcurrency          MetricAverage `json:"gpu_concurrency"`
	DiskReadBytesRateAvg    MetricAverage `json:"disk_read_bytes_per_second_avg"`
	DiskWriteBytesRateAvg   MetricAverage `json:"disk_write_bytes_per_second_avg"`
	NetworkRecvBytesRateAvg MetricAverage `json:"network_recv_bytes_per_second_avg"`
	NetworkSentBytesRateAvg MetricAverage `json:"network_sent_bytes_per_second_avg"`
	CPUPercentAvg           MetricAverage `json:"cpu_pct_avg"`
	CPUTotalAvg             MetricAverage `json:"cpu_total_avg"`
	CPUUsedAvg              MetricAverage `json:"cpu_used_avg"`
	DiskReadBytesAvg        MetricAverage `json:"disk_read_bytes_avg"`
	DiskWriteBytesAvg       MetricAverage `json:"disk_write_bytes_avg"`
	GPUMemoryTotalBytesAvg  MetricAverage `json:"gpu_memory_total_bytes_avg"`
	GPUMemoryUsedBytesAvg   MetricAverage `json:"gpu_memory_used_bytes_avg"`
	MemoryRSSBytesAvg       MetricAverage `json:"memory_rss_bytes_avg"`
	MemoryTotalBytesAvg     MetricAverage `json:"memory_total_bytes_avg"`
	MemoryVMSBytesAvg       MetricAverage `json:"memory_vms_bytes_avg"`
	MemorySwapBytesAvg      MetricAverage `json:"memory_swap_bytes_avg"`
	NetworkRecvBytesAvg     MetricAverage `json:"network_recv_bytes_avg"`
	NetworkSentBytesAvg     MetricAverage `json:"network_sent_bytes_avg"`
	NetworkRecvPacketsAvg   MetricAverage `json:"network_recv_packets_avg"`
	NetworkSentPacketsAvg   MetricAverage `json:"network_sent_packets_avg"`
}

type MetricsTimeseriesResponse struct {
	ScannedRecords uint64 `json:"scanned_records"`
	Truncated      bool   `json:"truncated"`
	Timeseries     struct {
		AggregationBuckets []MetricsAggregationBucket `json:"aggregation_buckets"`
	} `json:"timeseries"`
}

type PoolMetrics struct {
	WorkspaceID          string  `json:"workspace_id"`
	PoolName             string  `json:"pool_name"`
	MachineCount         int     `json:"machine_count"`
	ContainerCount       uint32  `json:"container_count"`
	GPUCount             uint32  `json:"gpu_count"`
	FreeGPUCount         uint32  `json:"free_gpu_count"`
	CPUUtilizationPct    float64 `json:"cpu_utilization_pct"`
	MemoryUtilizationPct float64 `json:"memory_utilization_pct"`
	GPUUtilizationPct    float64 `json:"gpu_utilization_pct"`
	DiskUsagePct         float64 `json:"disk_usage_pct"`
	HourlyCost           float64 `json:"hourly_cost"`
	EstimatedCost        float64 `json:"estimated_cost"`
}

type PoolMetricsPoint struct {
	Timestamp int64         `json:"timestamp"`
	Pools     []PoolMetrics `json:"pools"`
}

type PoolMetricsTimeseriesResponse struct {
	Workspaces     []string           `json:"workspaces"`
	Points         []PoolMetricsPoint `json:"points"`
	ScannedRecords uint64             `json:"scanned_records"`
	Truncated      bool               `json:"truncated"`
}

type ContainerEventRecord struct {
	SeqNum      uint64            `json:"seq_num"`
	StoredAtNs  uint64            `json:"stored_at_ns,omitempty"`
	CloudEvent  json.RawMessage   `json:"cloud_event"`
	Type        string            `json:"type"`
	EventID     string            `json:"event_id,omitempty"`
	Domain      string            `json:"domain,omitempty"`
	ParentID    string            `json:"parent_id,omitempty"`
	Timestamp   time.Time         `json:"timestamp,omitempty"`
	StartTime   time.Time         `json:"start_time,omitempty"`
	EndTime     time.Time         `json:"end_time,omitempty"`
	DurationMs  int64             `json:"duration_ms,omitempty"`
	Success     *bool             `json:"success,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	Source      string            `json:"source,omitempty"`
	Message     string            `json:"message,omitempty"`
	Attrs       map[string]string `json:"attrs,omitempty"`
	Stream      string            `json:"stream,omitempty"`
	Line        string            `json:"line,omitempty"`
	Data        json.RawMessage   `json:"data,omitempty"`
	ContainerID string            `json:"container_id,omitempty"`
	StubID      string            `json:"stub_id,omitempty"`
	StubType    string            `json:"stub_type,omitempty"`
	TaskID      string            `json:"task_id,omitempty"`
	WorkspaceID string            `json:"workspace_id,omitempty"`
	AppID       string            `json:"app_id,omitempty"`
	MachineID   string            `json:"machine_id,omitempty"`
	WorkerID    string            `json:"worker_id,omitempty"`
	PID         int32             `json:"pid,omitempty"`
	ProcessArgs []string          `json:"process_args,omitempty"`
	ProcessCwd  string            `json:"process_cwd,omitempty"`
	ProcessSeq  uint64            `json:"process_seq,omitempty"`
}

type ContainerEventsResponse struct {
	ContainerID    string                 `json:"container_id"`
	WorkspaceID    string                 `json:"workspace_id,omitempty"`
	StubID         string                 `json:"stub_id,omitempty"`
	Status         string                 `json:"status,omitempty"`
	StopReason     string                 `json:"stop_reason,omitempty"`
	RootCauseEvent string                 `json:"root_cause_event,omitempty"`
	Summary        map[string]int64       `json:"summary"`
	Events         []ContainerEventRecord `json:"events"`
	Missing        []string               `json:"missing"`
	Streams        []string               `json:"streams,omitempty"`
}

type EventHistoryResponse struct {
	Events  []ContainerEventRecord `json:"events"`
	Streams []string               `json:"streams,omitempty"`
}

func ContainerEventDomain(id ContainerEventID) EventDomain {
	if def, ok := ContainerEventDefinitions[id]; ok {
		return def.Domain
	}
	return ""
}

func ContainerLifecycleDefinitionFor(id ContainerLifecycleID) ContainerLifecycleDefinition {
	if def, ok := ContainerLifecycleDefinitions[id]; ok {
		return def
	}
	if strings.HasPrefix(string(id), "image.") {
		return ContainerLifecycleDefinition{ID: id, Domain: EventDomainImage, ParentID: ContainerLifecycleImageLoad, Label: string(id)}
	}
	if strings.HasPrefix(string(id), "clip.") {
		return ContainerLifecycleDefinition{ID: id, Domain: EventDomainClip, ParentID: ContainerLifecycleClipRead, Label: string(id)}
	}
	return ContainerLifecycleDefinition{ID: id}
}

func IsContainerRootCauseCandidate(id ContainerEventID) bool {
	switch id {
	case ContainerEventSchedulerStopRequested,
		ContainerEventWorkerStopEventReceived,
		ContainerEventWorkerOrphanStateMissing,
		ContainerEventWorkerStoppingGraceKill,
		ContainerEventRuntimeExited,
		ContainerEventRuntimeOOMKilled,
		ContainerEventGatewayServeLockDeleted,
		ContainerEventTaskCancelRequested,
		ContainerEventTaskCancelApplied:
		return true
	default:
		return false
	}
}

func NormalizeEventReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "UNKNOWN"
	}
	return reason
}

func EventSummaryKeyForLifecycle(id ContainerLifecycleID) string {
	switch id {
	case ContainerLifecycleSchedulerQueuePush:
		return "scheduler_queue_push_ms"
	case ContainerLifecycleSchedulerBacklogWait:
		return "scheduler_backlog_ms"
	case ContainerLifecycleSchedulerWorkerList:
		return "scheduler_worker_list_ms"
	case ContainerLifecycleSchedulerBatchPlan:
		return "scheduler_batch_plan_ms"
	case ContainerLifecycleSchedulerWorkerSelection:
		return "scheduler_worker_selection_ms"
	case ContainerLifecycleSchedulerReserveCapacity:
		return "scheduler_reserve_capacity_ms"
	case ContainerLifecycleSchedulerReservation:
		return "scheduler_reservation_ms"
	case ContainerLifecycleSchedulerProvisionWorker:
		return "scheduler_provision_worker_ms"
	case ContainerLifecycleSchedulerImageCredentials:
		return "scheduler_attach_image_credentials_ms"
	case ContainerLifecycleSchedulerBuildCredentials:
		return "scheduler_attach_build_registry_credentials_ms"
	case ContainerLifecycleSchedulerWorkerQueuePush:
		return "scheduler_worker_queue_push_ms"
	case ContainerLifecycleWorkerQueueReceive:
		return "worker_queue_ms"
	case ContainerLifecycleImageLoad:
		return "image_ms"
	case ContainerLifecycleRuntimeStartToPID:
		return "runtime_start_to_pid_ms"
	case ContainerLifecycleServeReady:
		return "serve_ready_ms"
	case ContainerLifecycleResultDelivery:
		return "result_ms"
	case ContainerLifecycleContainerRequestToStartTask:
		return "container_request_to_start_task_ms"
	case ContainerLifecycleContainerRunningToStartTask:
		return "container_running_to_start_task_ms"
	case ContainerLifecycleRunnerStartToGetArgs:
		return "runner_start_to_get_args_ms"
	case ContainerLifecycleRunnerGetArgsToSetResult:
		return "runner_get_args_to_set_result_ms"
	case ContainerLifecycleRunnerStartToSetResult:
		return "runner_start_to_set_result_ms"
	case ContainerLifecycleResultSetToEndTask:
		return "result_set_to_end_task_ms"
	case ContainerLifecycleRunnerStartToEndTask:
		return "runner_start_to_end_task_ms"
	case ContainerLifecycleRunnerGatewayChannelOpen:
		return "runner_gateway_channel_open_ms"
	case ContainerLifecycleRunnerStartTaskRPC:
		return "runner_start_task_rpc_ms"
	case ContainerLifecycleRunnerGetArgsRPC:
		return "runner_get_args_rpc_ms"
	case ContainerLifecycleRunnerUserCodeImport:
		return "runner_user_code_import_ms"
	case ContainerLifecycleRunnerHandlerExecution:
		return "runner_handler_execution_ms"
	case ContainerLifecycleRunnerSetResultRPC:
		return "runner_set_result_rpc_ms"
	case ContainerLifecycleRunnerEndTaskRPC:
		return "runner_end_task_rpc_ms"
	case ContainerLifecycleClipRead:
		return "clip_read_ms"
	case ContainerLifecycleClipOCIRead:
		return "clip_oci_read_ms"
	case ContainerLifecycleClipArchiveRead:
		return "clip_archive_read_ms"
	case ContainerLifecycleClipDiskCacheRead:
		return "clip_disk_cache_read_ms"
	case ContainerLifecycleClipContentCacheRead:
		return "clip_content_cache_read_ms"
	case ContainerLifecycleClipCheckpointRead:
		return "clip_checkpoint_read_ms"
	case ContainerLifecycleClipLayerDecompress:
		return "clip_layer_decompress_ms"
	case ContainerLifecycleClipLayerDecompressWait:
		return "clip_layer_decompress_wait_ms"
	default:
		return strings.ReplaceAll(string(id), ".", "_") + "_ms"
	}
}
