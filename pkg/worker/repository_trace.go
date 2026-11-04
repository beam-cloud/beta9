package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/stats"
)

// These measurements separate transport admission from server response time.
// They are installed only on the opt-in staging profiler, and never log RPC
// payloads, credentials, or metadata.
type repositoryRPCTraceStats struct{}
type repositoryRPCTraceKey struct{}
type repositoryRPCTrace struct {
	mu            sync.Mutex
	name          string
	started, sent time.Time
}

func (repositoryRPCTraceStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if common.StartupTraceFromContext(ctx) == nil {
		return ctx
	}
	name := info.FullMethodName[strings.LastIndex(info.FullMethodName, "/")+1:]
	return context.WithValue(ctx, repositoryRPCTraceKey{}, &repositoryRPCTrace{name: "repository." + name, started: time.Now()})
}

func (repositoryRPCTraceStats) HandleRPC(ctx context.Context, stat stats.RPCStats) {
	trace, _ := ctx.Value(repositoryRPCTraceKey{}).(*repositoryRPCTrace)
	if trace == nil {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	switch stat.(type) {
	case *stats.OutHeader:
		if trace.sent.IsZero() {
			trace.sent = time.Now()
			common.RecordStartupSpan(ctx, trace.name+".transport_admission", trace.started, trace.sent)
		}
	case *stats.End:
		now := time.Now()
		if !trace.sent.IsZero() {
			common.RecordStartupSpan(ctx, trace.name+".response", trace.sent, now)
		}
		common.RecordStartupSpan(ctx, trace.name+".total", trace.started, now)
	}
}

func (repositoryRPCTraceStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (repositoryRPCTraceStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		log.Info().Str("component", "repository_transport").Int64("at_ns", time.Now().UnixNano()).Msg("repository transport opened")
	case *stats.ConnEnd:
		log.Info().Str("component", "repository_transport").Int64("at_ns", time.Now().UnixNano()).Msg("repository transport closed")
	}
}
