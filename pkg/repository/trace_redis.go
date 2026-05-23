package repository

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types/trace"
	"github.com/redis/go-redis/v9"
)

const (
	maxTraceEventEntries = 2000
	maxTraceLogEntries   = 1000
)

type TraceRedisRepository struct {
	rdb *common.RedisClient
}

func NewTraceRedisRepository(rdb *common.RedisClient) TraceRepository {
	return &TraceRedisRepository{rdb: rdb}
}

func (r *TraceRedisRepository) RecordEvent(ctx context.Context, event trace.Event) error {
	rdb := r.rdb
	if rdb == nil || event.ContainerID == "" || event.ID == "" {
		return nil
	}

	event.ContainerID = strings.TrimSpace(event.ContainerID)
	if event.ContainerID == "" {
		return nil
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Domain == "" {
		event.Domain = trace.DomainForEvent(event.ID)
	}
	event.Reason = trace.NormalizeReason(event.Reason)
	event.Attrs = sanitizeAttrs(event.Attrs)

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	metaKey := common.RedisKeys.TraceContainerMeta(event.ContainerID)
	eventsKey := common.RedisKeys.TraceContainerEvents(event.ContainerID)
	pipe := rdb.TxPipeline()
	pipe.RPush(ctx, eventsKey, payload)
	pipe.LTrim(ctx, eventsKey, -maxTraceEventEntries, -1)
	pipe.Expire(ctx, eventsKey, trace.Retention)

	meta := map[string]any{
		"container_id": event.ContainerID,
		"updated_at":   event.Timestamp.Format(time.RFC3339Nano),
	}
	if event.TaskID != "" {
		meta["task_id"] = event.TaskID
	}
	if event.WorkspaceID != "" {
		meta["workspace_id"] = event.WorkspaceID
	}
	if event.StubID != "" {
		meta["stub_id"] = event.StubID
	}
	if event.StubType != "" {
		meta["stub_type"] = event.StubType
	}
	if event.Reason != "" && event.Reason != string(trace.ReasonUnknown) {
		meta["stop_reason"] = event.Reason
	}
	if event.ID == trace.EventRuntimeExited && event.Reason != "" {
		meta["stop_reason"] = event.Reason
	}
	pipe.HSet(ctx, metaKey, meta)
	if trace.IsRootCauseCandidate(event.ID) {
		pipe.HSetNX(ctx, metaKey, "root_cause_event", string(event.ID))
	}
	pipe.Expire(ctx, metaKey, trace.Retention)
	if event.TaskID != "" {
		taskKey := common.RedisKeys.TraceTaskContainerIndex(event.TaskID)
		pipe.Set(ctx, taskKey, event.ContainerID, trace.Retention)
	}

	_, err = pipe.Exec(ctx)
	return err
}

func (r *TraceRedisRepository) RecordSpan(ctx context.Context, span trace.Span) error {
	rdb := r.rdb
	if rdb == nil || span.ContainerID == "" || span.ID == "" {
		return nil
	}

	span.ContainerID = strings.TrimSpace(span.ContainerID)
	if span.ContainerID == "" {
		return nil
	}
	def, ok := trace.SpanDefinitions[span.ID]
	if ok {
		if span.Domain == "" {
			span.Domain = def.Domain
		}
		if span.ParentID == "" {
			span.ParentID = def.ParentID
		}
	}
	if span.StartTime.IsZero() {
		span.StartTime = time.Now().UTC()
	}
	if !span.EndTime.IsZero() && span.DurationMs == 0 {
		span.DurationMs = span.EndTime.Sub(span.StartTime).Milliseconds()
	}
	span.Attrs = sanitizeAttrs(span.Attrs)

	payload, err := json.Marshal(span)
	if err != nil {
		return err
	}

	metaKey := common.RedisKeys.TraceContainerMeta(span.ContainerID)
	spansKey := common.RedisKeys.TraceContainerSpans(span.ContainerID)
	pipe := rdb.TxPipeline()
	pipe.RPush(ctx, spansKey, payload)
	pipe.Expire(ctx, spansKey, trace.Retention)
	pipe.HSet(ctx, metaKey, map[string]any{
		"container_id": span.ContainerID,
		"updated_at":   time.Now().UTC().Format(time.RFC3339Nano),
	})
	if span.TaskID != "" {
		pipe.HSet(ctx, metaKey, "task_id", span.TaskID)
		pipe.Set(ctx, common.RedisKeys.TraceTaskContainerIndex(span.TaskID), span.ContainerID, trace.Retention)
	}
	if span.WorkspaceID != "" {
		pipe.HSet(ctx, metaKey, "workspace_id", span.WorkspaceID)
	}
	if span.StubID != "" {
		pipe.HSet(ctx, metaKey, "stub_id", span.StubID)
	}
	if span.StubType != "" {
		pipe.HSet(ctx, metaKey, "stub_type", span.StubType)
	}
	pipe.Expire(ctx, metaKey, trace.Retention)

	_, err = pipe.Exec(ctx)
	return err
}

func (r *TraceRedisRepository) RecordLog(ctx context.Context, entry trace.LogEntry) error {
	rdb := r.rdb
	if rdb == nil || entry.ContainerID == "" || entry.Line == "" {
		return nil
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	logsKey := common.RedisKeys.TraceContainerLogs(entry.ContainerID)
	metaKey := common.RedisKeys.TraceContainerMeta(entry.ContainerID)
	pipe := rdb.TxPipeline()
	pipe.RPush(ctx, logsKey, payload)
	pipe.LTrim(ctx, logsKey, -maxTraceLogEntries, -1)
	pipe.Expire(ctx, logsKey, trace.Retention)
	pipe.HSet(ctx, metaKey, map[string]any{
		"container_id": entry.ContainerID,
		"updated_at":   entry.Timestamp.Format(time.RFC3339Nano),
	})
	pipe.Expire(ctx, metaKey, trace.Retention)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *TraceRedisRepository) GetContainerTrace(ctx context.Context, containerID string) (*trace.ContainerTrace, error) {
	rdb := r.rdb
	containerTrace := &trace.ContainerTrace{
		ContainerID: strings.TrimSpace(containerID),
		Summary:     map[string]int64{},
		Spans:       []trace.Span{},
		Events:      []trace.Event{},
		Logs:        trace.TraceLogs{Tail: []trace.LogEntry{}},
		Missing:     []string{},
	}
	if rdb == nil || containerTrace.ContainerID == "" {
		return containerTrace, nil
	}

	meta, err := rdb.HGetAll(ctx, common.RedisKeys.TraceContainerMeta(containerTrace.ContainerID)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	containerTrace.TaskID = meta["task_id"]
	containerTrace.WorkspaceID = meta["workspace_id"]
	containerTrace.StubID = meta["stub_id"]
	containerTrace.StubType = meta["stub_type"]
	containerTrace.Status = meta["status"]
	containerTrace.StopReason = meta["stop_reason"]
	containerTrace.RootCauseEvent = meta["root_cause_event"]

	eventRows, err := rdb.LRange(ctx, common.RedisKeys.TraceContainerEvents(containerTrace.ContainerID), 0, -1)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, row := range eventRows {
		var event trace.Event
		if json.Unmarshal([]byte(row), &event) == nil {
			containerTrace.Events = append(containerTrace.Events, event)
		}
	}
	if containerTrace.RootCauseEvent == "" {
		for _, event := range containerTrace.Events {
			if trace.IsRootCauseCandidate(event.ID) {
				containerTrace.RootCauseEvent = string(event.ID)
				break
			}
		}
	}

	spanRows, err := rdb.LRange(ctx, common.RedisKeys.TraceContainerSpans(containerTrace.ContainerID), 0, -1)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, row := range spanRows {
		var span trace.Span
		if json.Unmarshal([]byte(row), &span) == nil {
			containerTrace.Spans = append(containerTrace.Spans, span)
			if span.DurationMs > 0 {
				containerTrace.Summary[trace.SummaryKeyForSpan(span.ID)] += span.DurationMs
			}
		}
	}

	logRows, err := rdb.LRange(ctx, common.RedisKeys.TraceContainerLogs(containerTrace.ContainerID), 0, -1)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, row := range logRows {
		var entry trace.LogEntry
		if json.Unmarshal([]byte(row), &entry) == nil {
			containerTrace.Logs.Tail = append(containerTrace.Logs.Tail, entry)
		}
	}
	containerTrace.Logs.Truncated = len(logRows) >= maxTraceLogEntries

	return containerTrace, nil
}

func sanitizeAttrs(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]string, len(attrs))
	for key, value := range attrs {
		normalizedKey := strings.ToLower(key)
		if strings.Contains(normalizedKey, "token") ||
			strings.Contains(normalizedKey, "secret") ||
			strings.Contains(normalizedKey, "password") ||
			strings.Contains(normalizedKey, "authorization") {
			out[key] = "[redacted]"
			continue
		}
		out[key] = value
	}
	return out
}
