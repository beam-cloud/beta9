package endpoint

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

const requestStatsWindow = 10 * time.Second

// requestStats folds every endpoint request into a per-stub window and emits
// one endpoint.request_stats event per stub per window, so event volume is
// bounded by active stubs, not request rate.
type requestStats struct {
	mu      sync.Mutex
	windows map[string]*types.EventEndpointRequestStatsSchema
	sink    repository.EventRepository
}

func newRequestStats(ctx context.Context, sink repository.EventRepository) *requestStats {
	s := &requestStats{windows: map[string]*types.EventEndpointRequestStatsSchema{}, sink: sink}
	if sink != nil {
		go s.run(ctx)
	}
	return s
}

func (s *requestStats) record(stubId, workspaceId, appId string, status int, duration time.Duration) {
	if s.sink == nil {
		return
	}
	ms := duration.Milliseconds()
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.windows[stubId]
	if w == nil {
		w = &types.EventEndpointRequestStatsSchema{
			StubID:          stubId,
			WorkspaceID:     workspaceId,
			AppID:           appId,
			WindowStart:     time.Now(),
			WindowSeconds:   int(requestStatsWindow / time.Second),
			LatencyBuckets:  make([]int64, len(types.RequestLatencyBoundsMs)+1),
			LatencyBoundsMs: types.RequestLatencyBoundsMs,
		}
		s.windows[stubId] = w
	}
	w.Requests++
	switch {
	case status >= 500:
		w.Status5xx++
	case status >= 400:
		w.Status4xx++
	}
	w.DurationSumMs += ms
	w.DurationMaxMs = max(w.DurationMaxMs, ms)
	i := 0
	for i < len(types.RequestLatencyBoundsMs) && ms > types.RequestLatencyBoundsMs[i] {
		i++
	}
	w.LatencyBuckets[i]++
}

func (s *requestStats) run(ctx context.Context) {
	ticker := time.NewTicker(requestStatsWindow)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flush()
			return
		case <-ticker.C:
			s.flush()
		}
	}
}

func (s *requestStats) flush() {
	s.mu.Lock()
	windows := s.windows
	s.windows = map[string]*types.EventEndpointRequestStatsSchema{}
	s.mu.Unlock()
	now := time.Now()
	for _, w := range windows {
		w.Timestamp = now
		s.sink.PushEndpointRequestStatsEvent(*w)
	}
}
