package common

import (
	"context"
	"runtime/trace"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// StartupTrace is opt-in, per-launch instrumentation. Absolute boundaries let
// a benchmark correlate processes; monotonic durations remain valid within
// each process. Parallel preparation records overlapping spans, not a sum.
type StartupTrace struct {
	mu          sync.Mutex
	started     time.Time
	vmID        string
	containerID string
	spans       []startupTraceSpan
	attributes  map[string]any
}

type startupTraceSpan struct {
	Name       string            `json:"name"`
	StartNS    int64             `json:"start_ns"`
	EndNS      int64             `json:"end_ns"`
	DurationUS int64             `json:"duration_us"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type startupTraceKey struct{}

func NewStartupTrace(enabled bool) *StartupTrace {
	if !enabled {
		return nil
	}
	return &StartupTrace{started: time.Now()}
}

func ContextWithStartupTrace(ctx context.Context, trace *StartupTrace) context.Context {
	if trace == nil {
		return ctx
	}
	return context.WithValue(ctx, startupTraceKey{}, trace)
}

func StartupTraceFromContext(ctx context.Context) *StartupTrace {
	trace, _ := ctx.Value(startupTraceKey{}).(*StartupTrace)
	return trace
}

func (t *StartupTrace) Identity(vmID, containerID string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.vmID, t.containerID = vmID, containerID
	t.mu.Unlock()
}

func (t *StartupTrace) Attribute(name string, value any) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.attributes == nil {
		t.attributes = make(map[string]any)
	}
	t.attributes[name] = value
	t.mu.Unlock()
}

func RecordStartupSpan(ctx context.Context, name string, start, end time.Time) {
	RecordStartupSpanWithAttributes(ctx, name, start, end, nil)
}

func RecordStartupSpanWithAttributes(ctx context.Context, name string, start, end time.Time, attributes map[string]string) {
	t := StartupTraceFromContext(ctx)
	if t == nil {
		return
	}
	var copied map[string]string
	if len(attributes) > 0 {
		copied = make(map[string]string, len(attributes))
		for key, value := range attributes {
			copied[key] = value
		}
	}
	t.mu.Lock()
	t.spans = append(t.spans, startupTraceSpan{Name: name, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationUS: end.Sub(start).Microseconds(), Attributes: copied})
	t.mu.Unlock()
}

func StartupSpan(ctx context.Context, name string) func() {
	if StartupTraceFromContext(ctx) == nil {
		return startupTraceNoop
	}
	started := time.Now()
	region := trace.StartRegion(ctx, name)
	return sync.OnceFunc(func() {
		RecordStartupSpan(ctx, name, started, time.Now())
		region.End()
	})
}

func startupTraceNoop() {}

func (t *StartupTrace) Log(event *zerolog.Event) {
	t.LogEvent(event, "vm startup trace")
}

func (t *StartupTrace) LogEvent(event *zerolog.Event, message string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	spans := append([]startupTraceSpan(nil), t.spans...)
	vmID, containerID := t.vmID, t.containerID
	attributes := make(map[string]any, len(t.attributes))
	for key, value := range t.attributes {
		attributes[key] = value
	}
	t.mu.Unlock()
	event.Str("vm_id", vmID).Str("container_id", containerID).
		Int64("trace_start_ns", t.started.UnixNano()).Interface("startup_spans", spans).Interface("startup_attributes", attributes).
		Msg(message)
}
