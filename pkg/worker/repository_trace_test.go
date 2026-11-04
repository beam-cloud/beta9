package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestRepositoryTraceSeparatesTransportFromResponse(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	entered, release := make(chan struct{}), make(chan struct{})
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///repository-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithStatsHandler(repositoryRPCTraceStats{}))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	trace := common.NewStartupTrace(true)
	ctx = common.ContextWithStartupTrace(ctx, trace)
	done := make(chan error, 1)
	go func() {
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var pending bytes.Buffer
	pendingLogger := zerolog.New(&pending)
	trace.Log(pendingLogger.Info())
	require.Contains(t, pending.String(), "repository.Check.transport_admission")
	require.NotContains(t, pending.String(), "repository.Check.response")
	close(release)
	require.NoError(t, <-done)
	var complete bytes.Buffer
	completeLogger := zerolog.New(&complete)
	trace.Log(completeLogger.Info())
	var event struct {
		Spans []struct {
			Name     string `json:"name"`
			Duration int64  `json:"duration_us"`
		} `json:"startup_spans"`
	}
	require.NoError(t, json.Unmarshal(complete.Bytes(), &event))
	names := map[string]bool{}
	for _, span := range event.Spans {
		names[span.Name] = true
		require.GreaterOrEqual(t, span.Duration, int64(0))
	}
	require.True(t, names["repository.Check.transport_admission"])
	require.True(t, names["repository.Check.response"])
	require.True(t, names["repository.Check.total"])
}
