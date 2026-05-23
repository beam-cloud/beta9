package repository_services

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types/trace"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TraceRepositoryService struct {
	ctx       context.Context
	traceRepo repository.TraceRepository
	pb.UnimplementedTraceRepositoryServiceServer
}

func NewTraceRepositoryService(ctx context.Context, traceRepo repository.TraceRepository) *TraceRepositoryService {
	return &TraceRepositoryService{ctx: ctx, traceRepo: traceRepo}
}

func (s *TraceRepositoryService) RecordTraceEvent(ctx context.Context, req *pb.RecordTraceEventRequest) (*pb.RecordTraceEventResponse, error) {
	if req.Event == nil {
		return &pb.RecordTraceEventResponse{Ok: false, ErrorMsg: "missing trace event"}, nil
	}
	if err := s.traceRepo.RecordEvent(ctx, traceEventFromProto(req.Event)); err != nil {
		return &pb.RecordTraceEventResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RecordTraceEventResponse{Ok: true}, nil
}

func (s *TraceRepositoryService) RecordTraceSpan(ctx context.Context, req *pb.RecordTraceSpanRequest) (*pb.RecordTraceSpanResponse, error) {
	if req.Span == nil {
		return &pb.RecordTraceSpanResponse{Ok: false, ErrorMsg: "missing trace span"}, nil
	}
	if err := s.traceRepo.RecordSpan(ctx, traceSpanFromProto(req.Span)); err != nil {
		return &pb.RecordTraceSpanResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RecordTraceSpanResponse{Ok: true}, nil
}

func (s *TraceRepositoryService) RecordTraceLog(ctx context.Context, req *pb.RecordTraceLogRequest) (*pb.RecordTraceLogResponse, error) {
	if req.Entry == nil {
		return &pb.RecordTraceLogResponse{Ok: false, ErrorMsg: "missing trace log entry"}, nil
	}
	if err := s.traceRepo.RecordLog(ctx, traceLogEntryFromProto(req.Entry)); err != nil {
		return &pb.RecordTraceLogResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RecordTraceLogResponse{Ok: true}, nil
}

func traceEventFromProto(in *pb.TraceEvent) trace.Event {
	event := trace.Event{
		ID:          trace.EventID(in.Id),
		Domain:      trace.Domain(in.Domain),
		ContainerID: in.ContainerId,
		StubID:      in.StubId,
		StubType:    in.StubType,
		TaskID:      in.TaskId,
		WorkspaceID: in.WorkspaceId,
		WorkerID:    in.WorkerId,
		Reason:      in.Reason,
		Source:      in.Source,
		Message:     in.Message,
		Attrs:       in.Attrs,
	}
	if in.Timestamp != nil {
		event.Timestamp = in.Timestamp.AsTime()
	}
	return event
}

func traceSpanFromProto(in *pb.TraceSpan) trace.Span {
	span := trace.Span{
		ID:          trace.SpanID(in.Id),
		Domain:      trace.Domain(in.Domain),
		ParentID:    trace.SpanID(in.ParentId),
		DurationMs:  in.DurationMs,
		ContainerID: in.ContainerId,
		StubID:      in.StubId,
		StubType:    in.StubType,
		TaskID:      in.TaskId,
		WorkspaceID: in.WorkspaceId,
		WorkerID:    in.WorkerId,
		Attrs:       in.Attrs,
	}
	if in.StartTime != nil {
		span.StartTime = in.StartTime.AsTime()
	}
	if in.EndTime != nil {
		span.EndTime = in.EndTime.AsTime()
	}
	if in.Success != nil {
		span.Success = in.Success
	}
	return span
}

func traceLogEntryFromProto(in *pb.TraceLogEntry) trace.LogEntry {
	entry := trace.LogEntry{
		ContainerID: in.ContainerId,
		TaskID:      in.TaskId,
		Stream:      in.Stream,
		Line:        in.Line,
	}
	if in.Timestamp != nil {
		entry.Timestamp = in.Timestamp.AsTime()
	}
	return entry
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
	out := &pb.TraceSpan{
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
	return out
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
