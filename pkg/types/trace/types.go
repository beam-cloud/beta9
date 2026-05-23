package trace

import (
	"strings"
	"time"
)

const (
	Retention = 7 * 24 * time.Hour
)

type Domain string

const (
	DomainGateway    Domain = "gateway"
	DomainScheduler  Domain = "scheduler"
	DomainWorker     Domain = "worker"
	DomainImage      Domain = "image"
	DomainMount      Domain = "mount"
	DomainNetwork    Domain = "network"
	DomainRuntime    Domain = "runtime"
	DomainRunner     Domain = "runner"
	DomainLogs       Domain = "logs"
	DomainResult     Domain = "result"
	DomainServe      Domain = "serve"
	DomainShutdown   Domain = "shutdown"
	DomainTask       Domain = "task"
	DomainAutoscaler Domain = "autoscaler"
)

type SpanID string

const (
	SpanContainerStartup  SpanID = "container.startup"
	SpanImageLoad         SpanID = "image.load"
	SpanSetWorkerAddress  SpanID = "worker.set_worker_address"
	SpanPortAllocation    SpanID = "worker.port_allocation"
	SpanReadBundleConfig  SpanID = "worker.read_bundle_config"
	SpanSetupMounts       SpanID = "mount.setup"
	SpanSpecFromRequest   SpanID = "worker.spec_from_request"
	SpanSetContainerAddr  SpanID = "worker.set_container_address"
	SpanSetAddressMap     SpanID = "worker.set_address_map"
	SpanOverlaySetup      SpanID = "mount.overlay_setup"
	SpanNetworkSetup      SpanID = "network.setup"
	SpanGPUAssignment     SpanID = "worker.gpu_assignment"
	SpanNetworkExpose     SpanID = "network.expose_ports"
	SpanRuntimePrepare    SpanID = "runtime.prepare"
	SpanConfigWrite       SpanID = "runtime.config_write"
	SpanStartQueueWait    SpanID = "worker.start_queue_wait"
	SpanRuntimeStartToPID SpanID = "runtime.start_to_pid"
	SpanServeReady        SpanID = "serve.ready"
	SpanResultDelivery    SpanID = "result.delivery"
)

type SpanDefinition struct {
	ID       SpanID `json:"id"`
	Domain   Domain `json:"domain"`
	ParentID SpanID `json:"parent_id,omitempty"`
	Label    string `json:"label"`
	Required bool   `json:"required"`
}

var SpanDefinitions = map[SpanID]SpanDefinition{
	SpanContainerStartup:  {ID: SpanContainerStartup, Domain: DomainRuntime, Label: "Container startup", Required: true},
	SpanImageLoad:         {ID: SpanImageLoad, Domain: DomainImage, ParentID: SpanContainerStartup, Label: "Image load", Required: true},
	SpanSetWorkerAddress:  {ID: SpanSetWorkerAddress, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Set worker address"},
	SpanPortAllocation:    {ID: SpanPortAllocation, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Port allocation"},
	SpanReadBundleConfig:  {ID: SpanReadBundleConfig, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Read bundle config"},
	SpanSetupMounts:       {ID: SpanSetupMounts, Domain: DomainMount, ParentID: SpanContainerStartup, Label: "Setup mounts"},
	SpanSpecFromRequest:   {ID: SpanSpecFromRequest, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Spec from request"},
	SpanSetContainerAddr:  {ID: SpanSetContainerAddr, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Set container address"},
	SpanSetAddressMap:     {ID: SpanSetAddressMap, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Set address map"},
	SpanOverlaySetup:      {ID: SpanOverlaySetup, Domain: DomainMount, ParentID: SpanContainerStartup, Label: "Overlay setup"},
	SpanNetworkSetup:      {ID: SpanNetworkSetup, Domain: DomainNetwork, ParentID: SpanContainerStartup, Label: "Network setup"},
	SpanGPUAssignment:     {ID: SpanGPUAssignment, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "GPU assignment"},
	SpanNetworkExpose:     {ID: SpanNetworkExpose, Domain: DomainNetwork, ParentID: SpanContainerStartup, Label: "Expose ports"},
	SpanRuntimePrepare:    {ID: SpanRuntimePrepare, Domain: DomainRuntime, ParentID: SpanContainerStartup, Label: "Runtime prepare"},
	SpanConfigWrite:       {ID: SpanConfigWrite, Domain: DomainRuntime, ParentID: SpanContainerStartup, Label: "Config write"},
	SpanStartQueueWait:    {ID: SpanStartQueueWait, Domain: DomainWorker, ParentID: SpanContainerStartup, Label: "Worker start queue wait"},
	SpanRuntimeStartToPID: {ID: SpanRuntimeStartToPID, Domain: DomainRuntime, ParentID: SpanContainerStartup, Label: "Runtime start to PID", Required: true},
	SpanServeReady:        {ID: SpanServeReady, Domain: DomainServe, ParentID: SpanContainerStartup, Label: "Serve ready"},
	SpanResultDelivery:    {ID: SpanResultDelivery, Domain: DomainResult, ParentID: SpanContainerStartup, Label: "Result delivery"},
}

type EventID string

const (
	EventSchedulerStopRequested    EventID = "scheduler.stop_requested"
	EventWorkerStopEventReceived   EventID = "worker.stop_event_received"
	EventWorkerOrphanStateMissing  EventID = "worker.orphan_state_missing"
	EventWorkerPendingReconciled   EventID = "worker.pending_reconciled_running"
	EventWorkerStoppingGraceKill   EventID = "worker.stopping_grace_kill"
	EventRuntimeExited             EventID = "runtime.exited"
	EventGatewayAttachDisconnected EventID = "gateway.attach_disconnected"
	EventGatewayServeLockDeleted   EventID = "gateway.serve_lock_deleted"
	EventGatewayServeLockPreserved EventID = "gateway.serve_lock_preserved"
	EventAutoscalerScaleDecision   EventID = "autoscaler.scale_decision"
	EventTaskCancelRequested       EventID = "task.cancel_requested"
	EventTaskCancelApplied         EventID = "task.cancel_applied"
	EventLogsFirstByte             EventID = "logs.first_byte"
	EventLogsLastByte              EventID = "logs.last_byte"
	EventLogsFlushCompleted        EventID = "logs.flush_completed"
	EventLogsDropped               EventID = "logs.dropped"
	EventResultSetResult           EventID = "result.set_result"
	EventResultEndTask             EventID = "result.end_task"
	EventResultLoadedByGateway     EventID = "result.loaded_by_gateway"
	EventResultSentToClient        EventID = "result.sent_to_client"
)

type EventDefinition struct {
	ID       EventID `json:"id"`
	Domain   Domain  `json:"domain"`
	Label    string  `json:"label"`
	Required bool    `json:"required"`
}

var EventDefinitions = map[EventID]EventDefinition{
	EventSchedulerStopRequested:    {ID: EventSchedulerStopRequested, Domain: DomainScheduler, Label: "Scheduler stop requested"},
	EventWorkerStopEventReceived:   {ID: EventWorkerStopEventReceived, Domain: DomainWorker, Label: "Worker stop event received"},
	EventWorkerOrphanStateMissing:  {ID: EventWorkerOrphanStateMissing, Domain: DomainWorker, Label: "Worker orphan state missing"},
	EventWorkerPendingReconciled:   {ID: EventWorkerPendingReconciled, Domain: DomainWorker, Label: "Pending reconciled to running"},
	EventWorkerStoppingGraceKill:   {ID: EventWorkerStoppingGraceKill, Domain: DomainWorker, Label: "Stopping grace kill"},
	EventRuntimeExited:             {ID: EventRuntimeExited, Domain: DomainRuntime, Label: "Runtime exited"},
	EventGatewayAttachDisconnected: {ID: EventGatewayAttachDisconnected, Domain: DomainGateway, Label: "Attach disconnected"},
	EventGatewayServeLockDeleted:   {ID: EventGatewayServeLockDeleted, Domain: DomainGateway, Label: "Serve lock deleted"},
	EventGatewayServeLockPreserved: {ID: EventGatewayServeLockPreserved, Domain: DomainGateway, Label: "Serve lock preserved"},
	EventAutoscalerScaleDecision:   {ID: EventAutoscalerScaleDecision, Domain: DomainAutoscaler, Label: "Autoscaler scale decision"},
	EventTaskCancelRequested:       {ID: EventTaskCancelRequested, Domain: DomainTask, Label: "Task cancel requested"},
	EventTaskCancelApplied:         {ID: EventTaskCancelApplied, Domain: DomainTask, Label: "Task cancel applied"},
	EventLogsFirstByte:             {ID: EventLogsFirstByte, Domain: DomainLogs, Label: "First log byte"},
	EventLogsLastByte:              {ID: EventLogsLastByte, Domain: DomainLogs, Label: "Last log byte"},
	EventLogsFlushCompleted:        {ID: EventLogsFlushCompleted, Domain: DomainLogs, Label: "Log flush completed"},
	EventLogsDropped:               {ID: EventLogsDropped, Domain: DomainLogs, Label: "Log dropped"},
	EventResultSetResult:           {ID: EventResultSetResult, Domain: DomainResult, Label: "Result set"},
	EventResultEndTask:             {ID: EventResultEndTask, Domain: DomainResult, Label: "End task"},
	EventResultLoadedByGateway:     {ID: EventResultLoadedByGateway, Domain: DomainResult, Label: "Result loaded by gateway"},
	EventResultSentToClient:        {ID: EventResultSentToClient, Domain: DomainResult, Label: "Result sent to client"},
}

type Event struct {
	ID          EventID           `json:"id"`
	Domain      Domain            `json:"domain"`
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

type Span struct {
	ID          SpanID            `json:"id"`
	Domain      Domain            `json:"domain"`
	ParentID    SpanID            `json:"parent_id,omitempty"`
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
	Attrs       map[string]string `json:"attrs,omitempty"`
}

type LogEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	ContainerID string    `json:"container_id,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	Stream      string    `json:"stream,omitempty"`
	Line        string    `json:"line"`
}

type ContainerTrace struct {
	ContainerID    string           `json:"container_id"`
	TaskID         string           `json:"task_id,omitempty"`
	WorkspaceID    string           `json:"workspace_id,omitempty"`
	StubID         string           `json:"stub_id,omitempty"`
	StubType       string           `json:"stub_type,omitempty"`
	Status         string           `json:"status,omitempty"`
	StopReason     string           `json:"stop_reason,omitempty"`
	RootCauseEvent string           `json:"root_cause_event,omitempty"`
	Summary        map[string]int64 `json:"summary"`
	Spans          []Span           `json:"spans"`
	Events         []Event          `json:"events"`
	Logs           TraceLogs        `json:"logs"`
	Missing        []string         `json:"missing"`
}

type TraceLogs struct {
	Truncated bool       `json:"truncated"`
	Tail      []LogEntry `json:"tail"`
}

type Reason string

const (
	ReasonUnknown Reason = "UNKNOWN"
)

func DomainForEvent(id EventID) Domain {
	if def, ok := EventDefinitions[id]; ok {
		return def.Domain
	}
	return ""
}

func IsRootCauseCandidate(id EventID) bool {
	switch id {
	case EventSchedulerStopRequested,
		EventWorkerStopEventReceived,
		EventWorkerOrphanStateMissing,
		EventWorkerStoppingGraceKill,
		EventRuntimeExited,
		EventGatewayServeLockDeleted,
		EventTaskCancelRequested,
		EventTaskCancelApplied:
		return true
	default:
		return false
	}
}

func NormalizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return string(ReasonUnknown)
	}
	return reason
}

func SummaryKeyForSpan(id SpanID) string {
	switch id {
	case SpanImageLoad:
		return "image_ms"
	case SpanRuntimeStartToPID:
		return "runtime_ms"
	case SpanServeReady:
		return "serve_ready_ms"
	case SpanResultDelivery:
		return "result_ms"
	default:
		return strings.ReplaceAll(string(id), ".", "_") + "_ms"
	}
}
