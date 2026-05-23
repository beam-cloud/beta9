package worker

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/trace"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Worker) recordTraceEvent(ctx context.Context, request *types.ContainerRequest, event trace.Event) {
	if request != nil {
		populateTraceEventFromRequest(&event, request)
	}
	if event.WorkerID == "" {
		event.WorkerID = s.workerId
	}
	if err := recordTraceEvent(ctx, s.traceRepoClient, event); err != nil {
		log.Debug().Err(err).Str("container_id", event.ContainerID).Str("event_id", string(event.ID)).Msg("failed to record startup trace event")
	}
}

func (s *Worker) recordTraceSpan(ctx context.Context, request *types.ContainerRequest, span trace.Span) {
	if request != nil {
		populateTraceSpanFromRequest(&span, request)
	}
	if span.WorkerID == "" {
		span.WorkerID = s.workerId
	}
	if err := recordTraceSpan(ctx, s.traceRepoClient, span); err != nil {
		log.Debug().Err(err).Str("container_id", span.ContainerID).Str("span_id", string(span.ID)).Msg("failed to record startup trace span")
	}
}

func populateTraceEventFromRequest(event *trace.Event, request *types.ContainerRequest) {
	if event.ContainerID == "" {
		event.ContainerID = request.ContainerId
	}
	if event.StubID == "" {
		event.StubID = request.StubId
	}
	if event.StubType == "" {
		event.StubType = string(request.Stub.Type.Kind())
	}
	if event.WorkspaceID == "" {
		event.WorkspaceID = request.WorkspaceId
	}
	if event.TaskID == "" {
		event.TaskID = taskIDFromEnv(request.Env)
	}
}

func populateTraceSpanFromRequest(span *trace.Span, request *types.ContainerRequest) {
	if span.ContainerID == "" {
		span.ContainerID = request.ContainerId
	}
	if span.StubID == "" {
		span.StubID = request.StubId
	}
	if span.StubType == "" {
		span.StubType = string(request.Stub.Type.Kind())
	}
	if span.WorkspaceID == "" {
		span.WorkspaceID = request.WorkspaceId
	}
	if span.TaskID == "" {
		span.TaskID = taskIDFromEnv(request.Env)
	}
}

func taskIDFromEnv(env []string) string {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "TASK_ID="); ok {
			return value
		}
	}
	return ""
}

func traceSpanFromDuration(id trace.SpanID, request *types.ContainerRequest, startedAt time.Time, duration time.Duration, success bool, attrs map[string]string) trace.Span {
	endTime := startedAt.Add(duration)
	span := trace.Span{
		ID:         id,
		StartTime:  startedAt.UTC(),
		EndTime:    endTime.UTC(),
		DurationMs: duration.Milliseconds(),
		Success:    &success,
		Attrs:      attrs,
	}
	populateTraceSpanFromRequest(&span, request)
	return span
}

func (s *Worker) recordStartupPhaseSpan(ctx context.Context, request *types.ContainerRequest, id trace.SpanID, startedAt time.Time, success bool, attrs map[string]string) {
	s.recordTraceSpan(ctx, request, traceSpanFromDuration(id, request, startedAt, time.Since(startedAt), success, attrs))
}

func recordTraceEvent(ctx context.Context, client pb.TraceRepositoryServiceClient, event trace.Event) error {
	if client == nil {
		return nil
	}
	_, err := handleGRPCResponse(client.RecordTraceEvent(ctx, &pb.RecordTraceEventRequest{Event: traceEventToProto(event)}))
	return err
}

func recordTraceSpan(ctx context.Context, client pb.TraceRepositoryServiceClient, span trace.Span) error {
	if client == nil {
		return nil
	}
	_, err := handleGRPCResponse(client.RecordTraceSpan(ctx, &pb.RecordTraceSpanRequest{Span: traceSpanToProto(span)}))
	return err
}

func recordTraceLog(ctx context.Context, client pb.TraceRepositoryServiceClient, entry trace.LogEntry) error {
	if client == nil {
		return nil
	}
	_, err := handleGRPCResponse(client.RecordTraceLog(ctx, &pb.RecordTraceLogRequest{Entry: traceLogEntryToProto(entry)}))
	return err
}

func traceEventToProto(event trace.Event) *pb.TraceEvent {
	return &pb.TraceEvent{
		Id:          string(event.ID),
		Domain:      string(event.Domain),
		Timestamp:   timestamppb.New(event.Timestamp),
		ContainerId: event.ContainerID,
		StubId:      event.StubID,
		StubType:    event.StubType,
		TaskId:      event.TaskID,
		WorkspaceId: event.WorkspaceID,
		WorkerId:    event.WorkerID,
		Reason:      event.Reason,
		Source:      event.Source,
		Message:     event.Message,
		Attrs:       event.Attrs,
	}
}

func traceSpanToProto(span trace.Span) *pb.TraceSpan {
	return &pb.TraceSpan{
		Id:          string(span.ID),
		Domain:      string(span.Domain),
		ParentId:    string(span.ParentID),
		StartTime:   timestamppb.New(span.StartTime),
		EndTime:     timestamppb.New(span.EndTime),
		DurationMs:  span.DurationMs,
		ContainerId: span.ContainerID,
		StubId:      span.StubID,
		StubType:    span.StubType,
		TaskId:      span.TaskID,
		WorkspaceId: span.WorkspaceID,
		WorkerId:    span.WorkerID,
		Success:     span.Success,
		Attrs:       span.Attrs,
	}
}

func traceLogEntryToProto(entry trace.LogEntry) *pb.TraceLogEntry {
	return &pb.TraceLogEntry{
		Timestamp:   timestamppb.New(entry.Timestamp),
		ContainerId: entry.ContainerID,
		TaskId:      entry.TaskID,
		Stream:      entry.Stream,
		Line:        entry.Line,
	}
}
