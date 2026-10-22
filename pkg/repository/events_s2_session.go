package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"k8s.io/utils/ptr"
)

func (r *S2EventRepository) StreamContainerEvents(ctx context.Context, containerID string, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream container events")
	}

	query.ContainerID = containerID
	return r.streamEvents(ctx, r.containerStreamName(query.WorkspaceID, query.StubID, containerID), containerID, query)
}

func (r *S2EventRepository) StreamStubEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.StubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to stream stub events")
	}
	return r.streamEvents(ctx, r.stubStreamName(query.WorkspaceID, query.StubID), "", query)
}

func (r *S2EventRepository) StreamTaskEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.TaskID == "" {
		return nil, fmt.Errorf("task id is required to stream task events")
	}

	// Task records are multiplexed into the per-stub task stream and
	// demultiplexed by the task_id record header. Log records share the
	// stream, so exclude them unless the caller explicitly asked for them.
	if len(query.EventTypes) == 0 {
		query.ExcludeEventTypes = append(query.ExcludeEventTypes, types.EventContainerLog)
	}

	streamName := r.stubTaskStreamName(query.WorkspaceID, query.StubID)
	if query.WorkspaceID == "" || query.StubID == "" {
		streamName = r.legacyTaskStreamName(query.TaskID)
	} else if r.streamHasRecords(ctx, r.legacyTaskStreamName(query.TaskID)) {
		// Tasks that predate the multiplexed stub task stream have their
		// records in the legacy per-task stream.
		streamName = r.legacyTaskStreamName(query.TaskID)
	}
	return r.streamEvents(ctx, streamName, "", query)
}

func (r *S2EventRepository) StreamWorkspaceEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" {
		return nil, fmt.Errorf("workspace id is required to stream workspace events")
	}
	return r.streamEvents(ctx, r.workspaceStreamName(query.WorkspaceID), "", query)
}

func (r *S2EventRepository) StreamAppNamespaceEvents(ctx context.Context, query types.EventQuery) (EventStream, error) {
	if query.WorkspaceID == "" || query.AppID == "" {
		return nil, fmt.Errorf("workspace id and app id are required to stream app namespace events")
	}
	return r.streamEvents(ctx, r.appNamespaceStreamName(query.WorkspaceID, query.AppID), "", query)
}

func (r *S2EventRepository) streamEvents(ctx context.Context, streamName s2.StreamName, containerID string, query types.EventQuery) (EventStream, error) {
	response := &types.ContainerEventsResponse{ContainerID: containerID, WorkspaceID: query.WorkspaceID, StubID: query.StubID}
	resume := query.Limit == 0 && query.Until == nil && query.WaitSeconds == nil
	stream, err := r.openSessionStream(ctx, streamName, s2EventReadOptions(query), resume, func(record s2.SequencedRecord) (types.ContainerEventRecord, bool) {
		return matchEventRecord(record, query, response)
	})
	if err != nil {
		return nil, fmt.Errorf("stream events from s2 stream %q: %w", streamName, err)
	}
	return stream, nil
}

// streamHasRecords reports whether the stream exists and holds at least one
// record. It is used to detect tasks whose records still live in legacy
// per-task streams.
func (r *S2EventRepository) streamHasRecords(ctx context.Context, streamName s2.StreamName) bool {
	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		return false
	}
	return tail.Tail.SeqNum > 0
}

// s2SessionStream is an EventStream over an S2 read session, yielding the
// records match accepts. If resume is set, a session that ends cleanly is
// reopened from the next record until the context is done.
type s2SessionStream struct {
	ctx     context.Context
	stream  *s2.StreamClient
	session *s2.ReadSession
	resume  bool
	match   func(s2.SequencedRecord) (types.ContainerEventRecord, bool)

	nextSeqNum *uint64
	current    types.ContainerEventRecord
	err        error
}

func (r *S2EventRepository) openSessionStream(ctx context.Context, streamName s2.StreamName, opts *s2.ReadOptions, resume bool, match func(s2.SequencedRecord) (types.ContainerEventRecord, bool)) (*s2SessionStream, error) {
	stream := r.basin.Stream(streamName)
	session, err := stream.ReadSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &s2SessionStream{ctx: ctx, stream: stream, session: session, resume: resume, match: match}, nil
}

func (s *s2SessionStream) Next() bool {
	for {
		for s.session.Next() {
			record := s.session.Record()
			s.nextSeqNum = ptr.To(record.SeqNum + 1)
			if current, ok := s.match(record); ok {
				s.current = current
				return true
			}
		}

		if err := s.session.Err(); err != nil {
			s.err = err
			return false
		}
		if !s.reopen() {
			return false
		}
	}
}

func (s *s2SessionStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2SessionStream) Err() error {
	return s.err
}

func (s *s2SessionStream) Close() error {
	return s.session.Close()
}

func (s *s2SessionStream) reopen() bool {
	if !s.resume || s.ctx.Err() != nil {
		return false
	}
	nextSeqNum := s.resumeSeqNum()
	if nextSeqNum == nil || !sleepWithContext(s.ctx, s2StreamResumeDelay) {
		return false
	}

	session, err := s.stream.ReadSession(s.ctx, &s2.ReadOptions{SeqNum: nextSeqNum, Clamp: ptr.To(true)})
	if err != nil {
		s.err = err
		return false
	}
	_ = s.session.Close()
	s.session = session
	return true
}

func (s *s2SessionStream) resumeSeqNum() *uint64 {
	if s.nextSeqNum != nil {
		return ptr.To(*s.nextSeqNum)
	}
	if position := s.session.NextReadPosition(); position != nil {
		return ptr.To(position.SeqNum)
	}
	if tail := s.session.LastObservedTail(); tail != nil {
		return ptr.To(tail.SeqNum)
	}
	return nil
}

func s2EventReadOptions(query types.EventQuery) *s2.ReadOptions {
	return &s2.ReadOptions{
		SeqNum:     query.SeqNum,
		Timestamp:  query.Timestamp,
		TailOffset: query.TailOffset,
		Count:      countOption(query.Limit),
		Until:      query.Until,
		Wait:       query.WaitSeconds,
		Clamp:      query.Clamp,
	}
}

func countOption(limit uint64) *uint64 {
	if limit == 0 {
		return nil
	}
	return &limit
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
