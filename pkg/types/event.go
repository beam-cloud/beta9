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
	EventContainerPhase     = "container.phase"
	EventContainerEvent     = "container.event"
	EventContainerLog       = "container.log"
	EventWorkerLifecycle    = "worker.lifecycle"
	EventStubDeploy         = "stub.deploy"
	EventStubServe          = "stub.serve"
	EventStubRun            = "stub.run"
	EventStubClone          = "stub.clone"

	EventWorkerPoolDegraded = "workerpool.degraded"
	EventWorkerPoolHealthy  = "workerpool.healthy"

	EventGatewayEndpointCalled = "gateway.endpoint.called"
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
	WorkspaceID string           `json:"workspace_id,omitempty"`
	StubID      string           `json:"stub_id"`
	StubType    string           `json:"stub_type,omitempty"`
	TaskID      string           `json:"task_id,omitempty"`
	Status      string           `json:"status"`
	Request     ContainerRequest `json:"request"`
}

var EventContainerStoppedSchemaVersion = "1.0"

type EventContainerStoppedSchema struct {
	EventContainerLifecycleSchema
	ExitCode int `json:"exit_code"`
}

var EventContainerMetricsSchemaVersion = "1.0"

type EventContainerMetricsSchema struct {
	WorkerID         string                    `json:"worker_id"`
	ContainerID      string                    `json:"container_id"`
	WorkspaceID      string                    `json:"workspace_id"`
	StubID           string                    `json:"stub_id"`
	StubType         string                    `json:"stub_type,omitempty"`
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
	WorkspaceID string           `json:"workspace_id,omitempty"`
	Request     ContainerRequest `json:"request"`
	StubID      string           `json:"stub_id"`
	StubType    string           `json:"stub_type,omitempty"`
	TaskID      string           `json:"task_id,omitempty"`
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
	ContainerID         string     `json:"container_id"`
	StartedAt           *time.Time `json:"started_at"`
	EndedAt             *time.Time `json:"ended_at"`
	WorkspaceID         string     `json:"workspace_id"`
	ExternalWorkspaceID string     `json:"external_workspace_id"`
	StubID              string     `json:"stub_id"`
	CreatedAt           time.Time  `json:"created_at"`
	AppID               string     `json:"app_id"`
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

type ContainerPhaseID string

const (
	ContainerPhaseStartup                     ContainerPhaseID = "container.startup"
	ContainerPhaseSchedulerQueuePush          ContainerPhaseID = "scheduler.queue_push"
	ContainerPhaseSchedulerBacklogWait        ContainerPhaseID = "scheduler.backlog_wait"
	ContainerPhaseSchedulerWorkerSelection    ContainerPhaseID = "scheduler.worker_selection"
	ContainerPhaseSchedulerReservation        ContainerPhaseID = "scheduler.reservation"
	ContainerPhaseSchedulerProvisionWorker    ContainerPhaseID = "scheduler.provision_worker"
	ContainerPhaseWorkerQueueReceive          ContainerPhaseID = "worker.queue_receive"
	ContainerPhaseImageLoad                   ContainerPhaseID = "image.load"
	ContainerPhaseSetWorkerAddress            ContainerPhaseID = "worker.set_worker_address"
	ContainerPhasePortAllocation              ContainerPhaseID = "worker.port_allocation"
	ContainerPhaseReadBundleConfig            ContainerPhaseID = "worker.read_bundle_config"
	ContainerPhaseSetupMounts                 ContainerPhaseID = "mount.setup"
	ContainerPhaseSpecFromRequest             ContainerPhaseID = "worker.spec_from_request"
	ContainerPhaseSetContainerAddr            ContainerPhaseID = "worker.set_container_address"
	ContainerPhaseSetAddressMap               ContainerPhaseID = "worker.set_address_map"
	ContainerPhaseOverlaySetup                ContainerPhaseID = "mount.overlay_setup"
	ContainerPhaseNetworkSetup                ContainerPhaseID = "network.setup"
	ContainerPhaseGPUAssignment               ContainerPhaseID = "worker.gpu_assignment"
	ContainerPhaseNetworkExpose               ContainerPhaseID = "network.expose_ports"
	ContainerPhaseRuntimePrepare              ContainerPhaseID = "runtime.prepare"
	ContainerPhaseConfigWrite                 ContainerPhaseID = "runtime.config_write"
	ContainerPhaseStartQueueWait              ContainerPhaseID = "worker.start_queue_wait"
	ContainerPhaseRuntimeStartToPID           ContainerPhaseID = "runtime.start_to_pid"
	ContainerPhaseServeReady                  ContainerPhaseID = "serve.ready"
	ContainerPhaseResultDelivery              ContainerPhaseID = "result.delivery"
	ContainerPhaseContainerRequestToStartTask ContainerPhaseID = "function.container_request_to_start_task"
	ContainerPhaseContainerRunningToStartTask ContainerPhaseID = "container.running_to_start_task"
	ContainerPhaseRunnerStartToGetArgs        ContainerPhaseID = "runner.start_task_to_get_args"
	ContainerPhaseRunnerGetArgsToSetResult    ContainerPhaseID = "runner.get_args_to_set_result"
	ContainerPhaseRunnerStartToSetResult      ContainerPhaseID = "runner.start_task_to_set_result"
	ContainerPhaseResultSetToEndTask          ContainerPhaseID = "result.set_result_to_end_task"
	ContainerPhaseRunnerStartToEndTask        ContainerPhaseID = "runner.start_task_to_end_task"
	ContainerPhaseRunnerGatewayChannelOpen    ContainerPhaseID = "runner.gateway_channel_open"
	ContainerPhaseRunnerStartTaskRPC          ContainerPhaseID = "runner.start_task_rpc"
	ContainerPhaseRunnerGetArgsRPC            ContainerPhaseID = "runner.get_args_rpc"
	ContainerPhaseRunnerUserCodeImport        ContainerPhaseID = "runner.user_code_import"
	ContainerPhaseRunnerHandlerExecution      ContainerPhaseID = "runner.handler_execution"
	ContainerPhaseRunnerSetResultRPC          ContainerPhaseID = "runner.set_result_rpc"
	ContainerPhaseRunnerEndTaskRPC            ContainerPhaseID = "runner.end_task_rpc"
)

type ContainerPhaseDefinition struct {
	ID       ContainerPhaseID `json:"id"`
	Domain   EventDomain      `json:"domain"`
	ParentID ContainerPhaseID `json:"parent_id,omitempty"`
	Label    string           `json:"label"`
	Required bool             `json:"required"`
}

var ContainerPhaseDefinitions = map[ContainerPhaseID]ContainerPhaseDefinition{
	ContainerPhaseStartup:                     {ID: ContainerPhaseStartup, Domain: EventDomainRuntime, Label: "Container startup", Required: true},
	ContainerPhaseSchedulerQueuePush:          {ID: ContainerPhaseSchedulerQueuePush, Domain: EventDomainScheduler, ParentID: ContainerPhaseStartup, Label: "Queue request"},
	ContainerPhaseSchedulerBacklogWait:        {ID: ContainerPhaseSchedulerBacklogWait, Domain: EventDomainScheduler, ParentID: ContainerPhaseStartup, Label: "Scheduler backlog wait"},
	ContainerPhaseSchedulerWorkerSelection:    {ID: ContainerPhaseSchedulerWorkerSelection, Domain: EventDomainScheduler, ParentID: ContainerPhaseStartup, Label: "Worker selection"},
	ContainerPhaseSchedulerReservation:        {ID: ContainerPhaseSchedulerReservation, Domain: EventDomainScheduler, ParentID: ContainerPhaseStartup, Label: "Capacity reservation"},
	ContainerPhaseSchedulerProvisionWorker:    {ID: ContainerPhaseSchedulerProvisionWorker, Domain: EventDomainScheduler, ParentID: ContainerPhaseStartup, Label: "Worker provisioning"},
	ContainerPhaseWorkerQueueReceive:          {ID: ContainerPhaseWorkerQueueReceive, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Worker queue receive"},
	ContainerPhaseImageLoad:                   {ID: ContainerPhaseImageLoad, Domain: EventDomainImage, ParentID: ContainerPhaseStartup, Label: "Image load", Required: true},
	ContainerPhaseSetWorkerAddress:            {ID: ContainerPhaseSetWorkerAddress, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Set worker address"},
	ContainerPhasePortAllocation:              {ID: ContainerPhasePortAllocation, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Port allocation"},
	ContainerPhaseReadBundleConfig:            {ID: ContainerPhaseReadBundleConfig, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Read bundle config"},
	ContainerPhaseSetupMounts:                 {ID: ContainerPhaseSetupMounts, Domain: EventDomainMount, ParentID: ContainerPhaseStartup, Label: "Setup mounts"},
	ContainerPhaseSpecFromRequest:             {ID: ContainerPhaseSpecFromRequest, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Spec from request"},
	ContainerPhaseSetContainerAddr:            {ID: ContainerPhaseSetContainerAddr, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Set container address"},
	ContainerPhaseSetAddressMap:               {ID: ContainerPhaseSetAddressMap, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Set address map"},
	ContainerPhaseOverlaySetup:                {ID: ContainerPhaseOverlaySetup, Domain: EventDomainMount, ParentID: ContainerPhaseStartup, Label: "Overlay setup"},
	ContainerPhaseNetworkSetup:                {ID: ContainerPhaseNetworkSetup, Domain: EventDomainNetwork, ParentID: ContainerPhaseStartup, Label: "Network setup"},
	ContainerPhaseGPUAssignment:               {ID: ContainerPhaseGPUAssignment, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "GPU assignment"},
	ContainerPhaseNetworkExpose:               {ID: ContainerPhaseNetworkExpose, Domain: EventDomainNetwork, ParentID: ContainerPhaseStartup, Label: "Expose ports"},
	ContainerPhaseRuntimePrepare:              {ID: ContainerPhaseRuntimePrepare, Domain: EventDomainRuntime, ParentID: ContainerPhaseStartup, Label: "Runtime prepare"},
	ContainerPhaseConfigWrite:                 {ID: ContainerPhaseConfigWrite, Domain: EventDomainRuntime, ParentID: ContainerPhaseStartup, Label: "Config write"},
	ContainerPhaseStartQueueWait:              {ID: ContainerPhaseStartQueueWait, Domain: EventDomainWorker, ParentID: ContainerPhaseStartup, Label: "Worker start queue wait"},
	ContainerPhaseRuntimeStartToPID:           {ID: ContainerPhaseRuntimeStartToPID, Domain: EventDomainRuntime, ParentID: ContainerPhaseStartup, Label: "Runtime start to PID", Required: true},
	ContainerPhaseServeReady:                  {ID: ContainerPhaseServeReady, Domain: EventDomainServe, ParentID: ContainerPhaseStartup, Label: "Serve ready"},
	ContainerPhaseResultDelivery:              {ID: ContainerPhaseResultDelivery, Domain: EventDomainResult, ParentID: ContainerPhaseStartup, Label: "Result delivery"},
	ContainerPhaseContainerRequestToStartTask: {ID: ContainerPhaseContainerRequestToStartTask, Domain: EventDomainTask, Label: "Container request to runner start"},
	ContainerPhaseContainerRunningToStartTask: {ID: ContainerPhaseContainerRunningToStartTask, Domain: EventDomainRunner, Label: "Container running to runner start"},
	ContainerPhaseRunnerStartToGetArgs:        {ID: ContainerPhaseRunnerStartToGetArgs, Domain: EventDomainRunner, Label: "Runner start to get args"},
	ContainerPhaseRunnerGetArgsToSetResult:    {ID: ContainerPhaseRunnerGetArgsToSetResult, Domain: EventDomainRunner, Label: "Get args to set result"},
	ContainerPhaseRunnerStartToSetResult:      {ID: ContainerPhaseRunnerStartToSetResult, Domain: EventDomainRunner, Label: "Runner start to set result"},
	ContainerPhaseResultSetToEndTask:          {ID: ContainerPhaseResultSetToEndTask, Domain: EventDomainResult, Label: "Set result to end task"},
	ContainerPhaseRunnerStartToEndTask:        {ID: ContainerPhaseRunnerStartToEndTask, Domain: EventDomainRunner, Label: "Runner start to end task"},
	ContainerPhaseRunnerGatewayChannelOpen:    {ID: ContainerPhaseRunnerGatewayChannelOpen, Domain: EventDomainRunner, Label: "Open gateway channel"},
	ContainerPhaseRunnerStartTaskRPC:          {ID: ContainerPhaseRunnerStartTaskRPC, Domain: EventDomainRunner, Label: "StartTask RPC"},
	ContainerPhaseRunnerGetArgsRPC:            {ID: ContainerPhaseRunnerGetArgsRPC, Domain: EventDomainRunner, Label: "GetArgs RPC"},
	ContainerPhaseRunnerUserCodeImport:        {ID: ContainerPhaseRunnerUserCodeImport, Domain: EventDomainRunner, Label: "Import user code"},
	ContainerPhaseRunnerHandlerExecution:      {ID: ContainerPhaseRunnerHandlerExecution, Domain: EventDomainRunner, Label: "Run handler"},
	ContainerPhaseRunnerSetResultRPC:          {ID: ContainerPhaseRunnerSetResultRPC, Domain: EventDomainRunner, Label: "SetResult RPC"},
	ContainerPhaseRunnerEndTaskRPC:            {ID: ContainerPhaseRunnerEndTaskRPC, Domain: EventDomainRunner, Label: "EndTask RPC"},
}

type ContainerEventID string

const (
	ContainerEventSchedulerStopRequested    ContainerEventID = "scheduler.stop_requested"
	ContainerEventWorkerStopEventReceived   ContainerEventID = "worker.stop_event_received"
	ContainerEventWorkerOrphanStateMissing  ContainerEventID = "worker.orphan_state_missing"
	ContainerEventWorkerPendingReconciled   ContainerEventID = "worker.pending_reconciled_running"
	ContainerEventWorkerStoppingGraceKill   ContainerEventID = "worker.stopping_grace_kill"
	ContainerEventRuntimeExited             ContainerEventID = "runtime.exited"
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

var EventContainerPhaseSchemaVersion = "1.0"

type EventContainerPhaseSchema struct {
	ID          ContainerPhaseID  `json:"id"`
	Domain      EventDomain       `json:"domain"`
	ParentID    ContainerPhaseID  `json:"parent_id,omitempty"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time,omitempty"`
	DurationMs  int64             `json:"duration_ms,omitempty"`
	ContainerID string            `json:"container_id,omitempty"`
	StubID      string            `json:"stub_id,omitempty"`
	StubType    string            `json:"stub_type,omitempty"`
	TaskID      string            `json:"task_id,omitempty"`
	WorkspaceID string            `json:"workspace_id,omitempty"`
	WorkerID    string            `json:"worker_id,omitempty"`
	Success     *bool             `json:"success,omitempty"`
	Source      string            `json:"source,omitempty"`
	Attrs       map[string]string `json:"attrs,omitempty"`
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
	WorkerID    string            `json:"worker_id,omitempty"`
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
	WorkerID    string    `json:"worker_id,omitempty"`
	Stream      string    `json:"stream,omitempty"`
	Line        string    `json:"line"`
}

type EventQuery struct {
	Limit       uint64 `json:"limit,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	StubID      string `json:"stub_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
}

type ContainerEventRecord struct {
	SeqNum      uint64            `json:"seq_num"`
	StoredAtNs  uint64            `json:"stored_at_ns,omitempty"`
	CloudEvent  json.RawMessage   `json:"cloud_event"`
	Type        string            `json:"type"`
	EventID     string            `json:"event_id,omitempty"`
	Domain      string            `json:"domain,omitempty"`
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
	WorkerID    string            `json:"worker_id,omitempty"`
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

func ContainerEventDomain(id ContainerEventID) EventDomain {
	if def, ok := ContainerEventDefinitions[id]; ok {
		return def.Domain
	}
	return ""
}

func ContainerPhaseDefinitionFor(id ContainerPhaseID) ContainerPhaseDefinition {
	if def, ok := ContainerPhaseDefinitions[id]; ok {
		return def
	}
	if strings.HasPrefix(string(id), "image.") {
		return ContainerPhaseDefinition{ID: id, Domain: EventDomainImage, ParentID: ContainerPhaseImageLoad, Label: string(id)}
	}
	return ContainerPhaseDefinition{ID: id}
}

func IsContainerRootCauseCandidate(id ContainerEventID) bool {
	switch id {
	case ContainerEventSchedulerStopRequested,
		ContainerEventWorkerStopEventReceived,
		ContainerEventWorkerOrphanStateMissing,
		ContainerEventWorkerStoppingGraceKill,
		ContainerEventRuntimeExited,
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

func EventSummaryKeyForPhase(id ContainerPhaseID) string {
	switch id {
	case ContainerPhaseSchedulerQueuePush:
		return "scheduler_queue_push_ms"
	case ContainerPhaseSchedulerBacklogWait:
		return "scheduler_backlog_ms"
	case ContainerPhaseSchedulerWorkerSelection:
		return "scheduler_worker_selection_ms"
	case ContainerPhaseSchedulerReservation:
		return "scheduler_reservation_ms"
	case ContainerPhaseSchedulerProvisionWorker:
		return "scheduler_provision_worker_ms"
	case ContainerPhaseWorkerQueueReceive:
		return "worker_queue_ms"
	case ContainerPhaseImageLoad:
		return "image_ms"
	case ContainerPhaseRuntimeStartToPID:
		return "runtime_start_to_pid_ms"
	case ContainerPhaseServeReady:
		return "serve_ready_ms"
	case ContainerPhaseResultDelivery:
		return "result_ms"
	case ContainerPhaseContainerRequestToStartTask:
		return "container_request_to_start_task_ms"
	case ContainerPhaseContainerRunningToStartTask:
		return "container_running_to_start_task_ms"
	case ContainerPhaseRunnerStartToGetArgs:
		return "runner_start_to_get_args_ms"
	case ContainerPhaseRunnerGetArgsToSetResult:
		return "runner_get_args_to_set_result_ms"
	case ContainerPhaseRunnerStartToSetResult:
		return "runner_start_to_set_result_ms"
	case ContainerPhaseResultSetToEndTask:
		return "result_set_to_end_task_ms"
	case ContainerPhaseRunnerStartToEndTask:
		return "runner_start_to_end_task_ms"
	case ContainerPhaseRunnerGatewayChannelOpen:
		return "runner_gateway_channel_open_ms"
	case ContainerPhaseRunnerStartTaskRPC:
		return "runner_start_task_rpc_ms"
	case ContainerPhaseRunnerGetArgsRPC:
		return "runner_get_args_rpc_ms"
	case ContainerPhaseRunnerUserCodeImport:
		return "runner_user_code_import_ms"
	case ContainerPhaseRunnerHandlerExecution:
		return "runner_handler_execution_ms"
	case ContainerPhaseRunnerSetResultRPC:
		return "runner_set_result_rpc_ms"
	case ContainerPhaseRunnerEndTaskRPC:
		return "runner_end_task_rpc_ms"
	default:
		return strings.ReplaceAll(string(id), ".", "_") + "_ms"
	}
}
