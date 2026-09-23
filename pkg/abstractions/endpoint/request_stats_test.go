package endpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type statsSink struct {
	repository.EventRepository
	events []types.EventEndpointRequestStatsSchema
}

func (s *statsSink) PushEndpointRequestStatsEvent(e types.EventEndpointRequestStatsSchema) {
	s.events = append(s.events, e)
}

func TestRequestStatsFoldsRequestsIntoOneEventPerStub(t *testing.T) {
	sink := &statsSink{}
	stats := &requestStats{windows: map[string]*types.EventEndpointRequestStatsSchema{}, sink: sink}

	stats.record("stub-a", "ws", "app", 200, 3*time.Millisecond)
	stats.record("stub-a", "ws", "app", 503, 40*time.Millisecond)
	stats.record("stub-a", "ws", "app", 404, 45*time.Second)
	stats.record("stub-b", "ws", "app", 200, 120*time.Millisecond)
	stats.flush()

	if len(sink.events) != 2 {
		t.Fatalf("events = %d, want one per stub", len(sink.events))
	}
	byStub := map[string]types.EventEndpointRequestStatsSchema{}
	for _, e := range sink.events {
		byStub[e.StubID] = e
	}
	a := byStub["stub-a"]
	if a.Requests != 3 || a.Status4xx != 1 || a.Status5xx != 1 || a.DurationMaxMs != 45000 {
		t.Fatalf("stub-a = %+v", a)
	}
	// 3ms -> bucket 0 (<=5), 40ms -> bucket 3 (<=50), 45s -> open-ended bucket.
	last := len(types.RequestLatencyBoundsMs)
	if a.LatencyBuckets[0] != 1 || a.LatencyBuckets[3] != 1 || a.LatencyBuckets[last] != 1 {
		t.Fatalf("stub-a buckets = %v", a.LatencyBuckets)
	}
	if b := byStub["stub-b"]; b.Requests != 1 || b.LatencyBuckets[5] != 1 {
		t.Fatalf("stub-b = %+v", b)
	}

	stats.flush()
	if len(sink.events) != 2 {
		t.Fatalf("empty window emitted events: %d", len(sink.events))
	}
}

func TestRequestStatsWithoutSinkIsNoop(t *testing.T) {
	stats := newRequestStats(context.Background(), nil)
	stats.record("stub", "ws", "", 200, time.Millisecond)
	if len(stats.windows) != 0 {
		t.Fatal("recorded without a sink")
	}
}
