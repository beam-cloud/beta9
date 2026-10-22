package repository

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

// s2StreamLayout names the streams events and logs are kept in, all under one
// prefix, and routes each event to the streams it is written to.
type s2StreamLayout struct {
	streamPrefix string
}

func newS2StreamLayout(prefix string) s2StreamLayout {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		prefix = defaultS2EventStreamPrefix
	}
	return s2StreamLayout{streamPrefix: prefix}
}

// name formats a stream name under the prefix, escaping each ID.
func (l s2StreamLayout) name(path string, ids ...string) s2.StreamName {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = eventStreamPart(id)
	}
	return s2.StreamName(l.streamPrefix + "/" + fmt.Sprintf(path, args...))
}

func eventStreamPart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	return strings.ReplaceAll(value, "/", "_")
}

// appendRecordForEvent encodes an event as an S2 record and lists the streams
// it is written to.
func (l s2StreamLayout) appendRecordForEvent(event cloudevents.Event) (s2.AppendRecord, []s2.StreamName, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return s2.AppendRecord{}, nil, fmt.Errorf("marshal cloud event: %w", err)
	}

	metadata := eventMetadataFromCloudEvent(event)
	streamNames := l.streamNamesForEvent(event.Type(), metadata)
	if len(streamNames) == 0 {
		return s2.AppendRecord{}, nil, nil
	}

	timestamp := uint64(event.Time().UnixMilli())
	return s2.AppendRecord{
		Timestamp: &timestamp,
		Headers:   s2HeadersForEvent(event, metadata),
		Body:      body,
	}, streamNames, nil
}

// streamNameForEvent returns the stream that holds an event first and foremost.
func (l s2StreamLayout) streamNameForEvent(eventType string, metadata eventMetadata) s2.StreamName {
	switch eventType {
	case types.EventContainerLog:
		return l.primaryLogStreamName(metadata)
	case types.EventPlatformLog:
		return l.platformLogStreamName(metadata)
	case types.EventStubCacheRequiredContent:
		if metadata.WorkspaceID == "" || metadata.StubID == "" {
			return ""
		}
		return l.stubCacheStreamName(metadata.WorkspaceID, metadata.StubID)
	case types.EventPlatformCache:
		return l.platformCacheStreamName()
	}

	switch {
	case isTaskEvent(eventType) && metadata.TaskID != "":
		return l.taskEventStreamName(metadata)
	case metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return l.containerStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID)
	case strings.HasPrefix(eventType, "container.") && metadata.ContainerID != "":
		return ""
	case metadata.TaskID != "":
		return l.taskEventStreamName(metadata)
	case isComputeEvent(eventType) && metadata.WorkspaceID != "":
		return l.workspaceStreamName(metadata.WorkspaceID)
	case metadata.WorkerID != "":
		return l.workerStreamName(metadata.WorkerID)
	case metadata.WorkspaceID != "" && metadata.StubID != "":
		return l.stubStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.WorkspaceID != "":
		return l.workspaceStreamName(metadata.WorkspaceID)
	case metadata.PoolName != "":
		return l.workerPoolStreamName(metadata.PoolName)
	default:
		return l.typeStreamName(eventType)
	}
}

// streamNamesForEvent lists every stream an event is written to: its primary
// stream, then the workspace streams that index it for narrower reads.
func (l s2StreamLayout) streamNamesForEvent(eventType string, metadata eventMetadata) []s2.StreamName {
	switch eventType {
	case types.EventContainerLog:
		return l.logStreamNamesForEvent(metadata)
	case types.EventPlatformLog:
		return l.platformLogStreamNames(metadata)
	case types.EventStubCacheRequiredContent:
		// Required-content reports are persisted only to the dedicated stub cache
		// stream; they must not fan out to the stub/workspace event streams.
		return uniqueStreams(l.streamNameForEvent(eventType, metadata))
	case types.EventPlatformCache:
		streams := []s2.StreamName{l.platformCacheStreamName()}
		if metadata.WorkspaceID != "" && metadata.MachineID != "" {
			streams = append(streams, l.workspaceMachineStreamName(metadata.WorkspaceID, metadata.MachineID))
		}
		return streams
	}

	streams := []s2.StreamName{l.streamNameForEvent(eventType, metadata)}
	workspaceID := metadata.WorkspaceID
	if workspaceID == "" {
		return uniqueStreams(streams...)
	}
	if shouldWriteContainerAlias(metadata) {
		streams = append(streams, l.containerAliasStreamName(workspaceID, metadata.ContainerID))
	}
	if metadata.StubID != "" {
		if isTaskEvent(eventType) && metadata.ContainerID != "" {
			streams = append(streams, l.containerStreamName(workspaceID, metadata.StubID, metadata.ContainerID))
		}
		streams = append(streams, l.stubStreamName(workspaceID, metadata.StubID))
	}

	switch {
	case isStubEvent(eventType):
		streams = append(streams, l.workspaceStreamName(workspaceID))
		if isStubStateEvent(eventType) {
			streams = append(streams, l.workspaceStubStateStreamName(workspaceID))
		}
	case eventType == types.EventEndpointRequestStats:
		// Request stats also feed the app-scoped history.
		if metadata.AppID != "" {
			streams = append(streams, l.appNamespaceStreamName(workspaceID, metadata.AppID))
		}
	case isWorkspaceEndpointEvent(eventType):
		streams = append(streams, l.workspaceStreamName(workspaceID))
	case isWorkspaceContainerRealtimeEvent(eventType):
		streams = append(streams, l.workspaceStreamName(workspaceID))
		if metadata.AppID != "" {
			streams = append(streams, l.appNamespaceStreamName(workspaceID, metadata.AppID))
		}
		if metadata.MachineID != "" {
			streams = append(streams, l.workspaceMachineStreamName(workspaceID, metadata.MachineID))
		}
	case isComputeEvent(eventType):
		if metadata.MachineID != "" {
			streams = append(streams, l.workspaceMachineStreamName(workspaceID, metadata.MachineID))
		}
		// Workspace stream powers the live workspace SSE; the compute stream keeps
		// a dense history for fast compute-only history queries. Heartbeats fire
		// every few seconds per machine, so they use a separate metrics stream
		// instead of drowning out lifecycle history.
		streams = append(streams, l.workspaceStreamName(workspaceID))
		if isComputeMetricsAction(metadata.Action) {
			streams = append(streams, l.workspacePoolMetricsStreamName(workspaceID))
		} else {
			streams = append(streams, l.workspaceComputeStreamName(workspaceID))
		}
	case isTaskEvent(eventType):
		streams = append(streams, l.workspaceStreamName(workspaceID))
		if metadata.AppID != "" {
			streams = append(streams, l.appNamespaceStreamName(workspaceID, metadata.AppID))
		}
	}
	return uniqueStreams(streams...)
}

func (l s2StreamLayout) primaryLogStreamName(metadata eventMetadata) s2.StreamName {
	switch {
	case metadata.ContainerID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return l.containerLogStreamName(metadata.WorkspaceID, metadata.StubID, metadata.ContainerID)
	case metadata.TaskID != "" && metadata.WorkspaceID != "" && metadata.StubID != "":
		return l.stubTaskStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.TaskID != "" && metadata.WorkspaceID != "":
		return l.workspaceLogStreamName(metadata.WorkspaceID)
	case metadata.StubID != "" && metadata.WorkspaceID != "":
		return l.stubLogStreamName(metadata.WorkspaceID, metadata.StubID)
	case metadata.AppID != "" && metadata.WorkspaceID != "":
		return l.appNamespaceLogStreamName(metadata.WorkspaceID, metadata.AppID)
	case metadata.WorkspaceID != "":
		return l.workspaceLogStreamName(metadata.WorkspaceID)
	default:
		return ""
	}
}

func (l s2StreamLayout) logStreamNamesForEvent(metadata eventMetadata) []s2.StreamName {
	workspaceID := metadata.WorkspaceID
	if workspaceID == "" {
		return nil
	}

	// Container/stub streams are the canonical lookup path for individual logs.
	// App namespace logs are a secondary dashboard index across those containers.
	var streams []s2.StreamName
	if metadata.ContainerID != "" && metadata.StubID != "" {
		streams = append(streams, l.containerLogStreamName(workspaceID, metadata.StubID, metadata.ContainerID))
	}
	if shouldWriteContainerAlias(metadata) {
		streams = append(streams, l.containerLogAliasStreamName(workspaceID, metadata.ContainerID))
	}
	if metadata.StubID != "" {
		streams = append(streams, l.stubLogStreamName(workspaceID, metadata.StubID))
		// Task-attributed logs are multiplexed into the per-stub task stream
		// (demultiplexed on read via the task_id record header) instead of a
		// per-task stream, so stream creation does not scale with task count.
		if metadata.TaskID != "" {
			streams = append(streams, l.stubTaskStreamName(workspaceID, metadata.StubID))
		}
	}
	if metadata.AppID != "" {
		streams = append(streams, l.appNamespaceLogStreamName(workspaceID, metadata.AppID))
	}
	if metadata.MachineID != "" {
		streams = append(streams, l.workspaceMachineLogStreamName(workspaceID, metadata.MachineID))
	}
	return append(streams, l.workspaceLogStreamName(workspaceID))
}

func (l s2StreamLayout) platformLogStreamNames(metadata eventMetadata) []s2.StreamName {
	stream := l.platformLogStreamName(metadata)
	if stream == "" {
		return nil
	}
	streams := []s2.StreamName{stream}
	if metadata.WorkspaceID != "" {
		if metadata.MachineID != "" {
			streams = append(streams, l.workspaceMachineLogStreamName(metadata.WorkspaceID, metadata.MachineID))
		}
		streams = append(streams, l.workspaceLogStreamName(metadata.WorkspaceID))
	}
	return streams
}

// containerEventStream returns the stream holding a container's events. A
// container ID scoped to a stub names that stub when the caller doesn't.
func (l s2StreamLayout) containerEventStream(workspaceID, stubID, containerID string) s2.StreamName {
	if workspaceID == "" {
		return ""
	}
	if stubID == "" {
		stubID, _ = common.ExtractStubIdFromStubScopedContainerId(containerID)
	}
	if stubID == "" {
		return l.containerAliasStreamName(workspaceID, containerID)
	}
	return l.containerStreamName(workspaceID, stubID, containerID)
}

// taskEventStreamName routes a task-scoped event to the multiplexed stub task
// stream, falling back to the workspace stream when stub metadata is missing.
func (l s2StreamLayout) taskEventStreamName(metadata eventMetadata) s2.StreamName {
	if metadata.WorkspaceID != "" && metadata.StubID != "" {
		return l.stubTaskStreamName(metadata.WorkspaceID, metadata.StubID)
	}
	if metadata.WorkspaceID != "" {
		return l.workspaceStreamName(metadata.WorkspaceID)
	}
	return ""
}

func shouldWriteContainerAlias(metadata eventMetadata) bool {
	if metadata.ContainerID == "" || metadata.WorkspaceID == "" {
		return false
	}

	stubID, ok := common.ExtractStubIdFromStubScopedContainerId(metadata.ContainerID)
	if !ok {
		return true
	}
	return metadata.StubID == "" || metadata.StubID != stubID
}

// uniqueStreams drops empty and repeated stream names, keeping their order.
func uniqueStreams(streams ...s2.StreamName) []s2.StreamName {
	unique := make([]s2.StreamName, 0, len(streams))
	for _, stream := range streams {
		if stream != "" && !slices.Contains(unique, stream) {
			unique = append(unique, stream)
		}
	}
	return unique
}

func isTaskEvent(eventType string) bool {
	return eventType == types.EventTaskCreated || eventType == types.EventTaskUpdated
}

func isStubEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "stub.")
}

func isStubStateEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "stub.state.")
}

func isComputeEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "compute.")
}

func isComputeMetricsAction(action string) bool {
	return action == types.EventComputeActionMachineHeartbeat || action == types.EventComputeActionPoolHeartbeat
}

func isWorkspaceContainerRealtimeEvent(eventType string) bool {
	return eventType == types.EventContainerMetrics ||
		eventType == types.EventContainerEvent ||
		eventType == types.EventContainerLifecycle
}

// isWorkspaceEndpointEvent reports whether eventType is an endpoint control
// audit event, which the workspace history serves.
func isWorkspaceEndpointEvent(eventType string) bool {
	return eventType == types.EventEndpointConfig ||
		eventType == types.EventEndpointHarness ||
		eventType == types.EventEndpointReplica ||
		eventType == types.EventEndpointGitOps
}

// workspaceStreamName and workerStreamName don't escape their IDs, unlike the
// other names, so they keep matching streams already written.
func (l s2StreamLayout) workspaceStreamName(workspaceID string) s2.StreamName {
	return s2.StreamName(l.streamPrefix + "/workspaces/" + workspaceID)
}

func (l s2StreamLayout) workerStreamName(workerID string) s2.StreamName {
	return s2.StreamName(l.streamPrefix + "/workers/" + workerID)
}

func (l s2StreamLayout) typeStreamName(eventType string) s2.StreamName {
	return s2.StreamName(l.streamPrefix + "/types/" + strings.ReplaceAll(eventType, ".", "-"))
}

func (l s2StreamLayout) workerPoolStreamName(poolName string) s2.StreamName {
	return l.name("worker-pools/%s", poolName)
}

func (l s2StreamLayout) stubStreamName(workspaceID, stubID string) s2.StreamName {
	return l.name("workspaces/%s/stubs/%s", workspaceID, stubID)
}

func (l s2StreamLayout) containerStreamName(workspaceID, stubID, containerID string) s2.StreamName {
	return l.name("workspaces/%s/stubs/%s/containers/%s", workspaceID, stubID, containerID)
}

func (l s2StreamLayout) containerAliasStreamName(workspaceID, containerID string) s2.StreamName {
	return l.name("workspaces/%s/containers/%s", workspaceID, containerID)
}

// stubTaskStreamName is the multiplexed per-stub stream carrying all task
// records (lifecycle events and task-attributed logs) for every task of a
// stub. Individual tasks are demultiplexed on read via the task_id and type
// record headers, so stream count scales with stubs instead of tasks.
func (l s2StreamLayout) stubTaskStreamName(workspaceID, stubID string) s2.StreamName {
	return l.name("workspaces/%s/stubs/%s/tasks", workspaceID, stubID)
}

// legacyTaskStreamName is the retired per-task event stream; it is only used
// as a read fallback for tasks that predate the multiplexed stub task stream.
func (l s2StreamLayout) legacyTaskStreamName(taskID string) s2.StreamName {
	return l.name("tasks/%s", taskID)
}

func (l s2StreamLayout) stubCacheStreamName(workspaceID, stubID string) s2.StreamName {
	return l.name("workspaces/%s/stubs/%s/cache", workspaceID, stubID)
}

func (l s2StreamLayout) appNamespaceStreamName(workspaceID, appID string) s2.StreamName {
	return l.name("workspaces/%s/apps/%s", workspaceID, appID)
}

func (l s2StreamLayout) workspaceMachineStreamName(workspaceID, machineID string) s2.StreamName {
	return l.name("workspaces/%s/machines/%s", workspaceID, machineID)
}

// workspaceComputeStreamName is a dedicated, compute-only stream so that compute
// event history reads stay dense and fast instead of paging through the busy
// shared workspace stream (which also carries container metrics/lifecycle).
func (l s2StreamLayout) workspaceComputeStreamName(workspaceID string) s2.StreamName {
	return l.name("workspaces/%s/compute", workspaceID)
}

func (l s2StreamLayout) workspacePoolMetricsStreamName(workspaceID string) s2.StreamName {
	return l.name("workspaces/%s/compute/metrics", workspaceID)
}

// workspaceStubStateStreamName holds only stub health transitions, so
// recent-failure reads don't page through the container metrics in the
// workspace stream.
func (l s2StreamLayout) workspaceStubStateStreamName(workspaceID string) s2.StreamName {
	return l.name("workspaces/%s/stub-states", workspaceID)
}

func (l s2StreamLayout) platformCacheStreamName() s2.StreamName {
	return l.name("platform/cache")
}

func (l s2StreamLayout) containerLogStreamName(workspaceID, stubID, containerID string) s2.StreamName {
	return l.name("logs/workspaces/%s/stubs/%s/containers/%s", workspaceID, stubID, containerID)
}

func (l s2StreamLayout) containerLogAliasStreamName(workspaceID, containerID string) s2.StreamName {
	return l.name("logs/workspaces/%s/containers/%s", workspaceID, containerID)
}

func (l s2StreamLayout) stubLogStreamName(workspaceID, stubID string) s2.StreamName {
	return l.name("logs/workspaces/%s/stubs/%s", workspaceID, stubID)
}

// legacyTaskLogStreamName is the retired per-task log stream; it is only used
// as a read fallback for tasks that predate the multiplexed stub task stream.
func (l s2StreamLayout) legacyTaskLogStreamName(workspaceID, taskID string) s2.StreamName {
	return l.name("logs/workspaces/%s/tasks/%s", workspaceID, taskID)
}

func (l s2StreamLayout) appNamespaceLogStreamName(workspaceID, appID string) s2.StreamName {
	return l.name("logs/workspaces/%s/apps/%s", workspaceID, appID)
}

func (l s2StreamLayout) workspaceLogStreamName(workspaceID string) s2.StreamName {
	return l.name("logs/workspaces/%s", workspaceID)
}

func (l s2StreamLayout) workspaceMachineLogStreamName(workspaceID, machineID string) s2.StreamName {
	return l.name("logs/workspaces/%s/machines/%s", workspaceID, machineID)
}

func (l s2StreamLayout) platformLogStreamName(metadata eventMetadata) s2.StreamName {
	switch {
	case metadata.WorkerID != "":
		return l.name("logs/platform/workers/%s", metadata.WorkerID)
	case metadata.ServiceName != "" && metadata.InstanceID != "":
		return l.name("logs/platform/services/%s/%s", metadata.ServiceName, metadata.InstanceID)
	case metadata.ServiceName != "":
		return l.name("logs/platform/services/%s", metadata.ServiceName)
	default:
		return ""
	}
}

func eventMetadataFromCloudEvent(event cloudevents.Event) eventMetadata {
	extensions := event.Extensions()
	return eventMetadata{
		ContainerID: extensionString(extensions, "containerid"),
		WorkspaceID: extensionString(extensions, "workspaceid"),
		TaskID:      extensionString(extensions, "taskid"),
		StubID:      extensionString(extensions, "stubid"),
		WorkerID:    extensionString(extensions, "workerid"),
		MachineID:   extensionString(extensions, "machineid"),
		RouteID:     extensionString(extensions, "routeid"),
		AppID:       extensionString(extensions, "appid"),
		ServiceName: extensionString(extensions, "servicename"),
		InstanceID:  extensionString(extensions, "instanceid"),
		PoolName:    extensionString(extensions, "poolname"),
	}
}

func extensionString(extensions map[string]interface{}, key string) string {
	if value, ok := extensions[key]; ok {
		return fmt.Sprintf("%v", value)
	}
	return ""
}

// s2HeadersForEvent lets readers filter records without decoding their body.
func s2HeadersForEvent(event cloudevents.Event, metadata eventMetadata) []s2.Header {
	headers := []s2.Header{
		s2.NewHeader("type", event.Type()),
		s2.NewHeader("id", event.ID()),
	}
	for _, header := range []struct{ name, value string }{
		{"container_id", metadata.ContainerID},
		{"workspace_id", metadata.WorkspaceID},
		{"task_id", metadata.TaskID},
		{"stub_id", metadata.StubID},
		{"worker_id", metadata.WorkerID},
		{"machine_id", metadata.MachineID},
		{"route_id", metadata.RouteID},
		{"service", metadata.ServiceName},
		{"instance_id", metadata.InstanceID},
		{"app_id", metadata.AppID},
		{"pool_name", metadata.PoolName},
	} {
		if header.value != "" {
			headers = append(headers, s2.NewHeader(header.name, header.value))
		}
	}
	return headers
}
