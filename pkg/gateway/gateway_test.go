package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type blockingMigrator struct {
	calls      atomic.Int32
	concurrent atomic.Int32
	max        atomic.Int32
	firstRun   chan struct{}
	release    chan struct{}
}

func (m *blockingMigrator) MigrateContext(ctx context.Context) error {
	m.calls.Add(1)
	current := m.concurrent.Add(1)
	defer m.concurrent.Add(-1)
	for {
		maximum := m.max.Load()
		if current <= maximum || m.max.CompareAndSwap(maximum, current) {
			break
		}
	}
	if current == 1 && m.calls.Load() == 1 {
		close(m.firstRun)
		select {
		case <-m.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestPostgresMigrationsAreSerializedAcrossReplicas(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	migrator := &blockingMigrator{firstRun: make(chan struct{}), release: make(chan struct{})}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- (&Gateway{ctx: ctx, RedisClient: rdb}).migratePostgres(migrator)
		}()
	}
	select {
	case <-migrator.firstRun:
	case <-time.After(time.Second):
		t.Fatal("first migration did not start")
	}
	time.Sleep(100 * time.Millisecond)
	if got := migrator.calls.Load(); got != 1 {
		t.Fatalf("concurrent replicas started %d migrations", got)
	}
	close(migrator.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if migrator.max.Load() != 1 || migrator.calls.Load() != 2 {
		t.Fatalf("migration concurrency=%d calls=%d", migrator.max.Load(), migrator.calls.Load())
	}
}

func gatewayHealthStatus(t *testing.T, g *Gateway, service string) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	response, err := g.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatal(err)
	}
	return response.Status
}

func TestGatewayHealthSeparatesLivenessFromReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	g := &Gateway{ctx: ctx, computeReady: ready}
	g.newHealthServer()

	if got := gatewayHealthStatus(t, g, gatewayLivenessService); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness = %v, want serving", got)
	}
	if got := gatewayHealthStatus(t, g, gatewayReadinessService); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness before reconciliation = %v, want not serving", got)
	}
	if g.isReady() {
		t.Fatal("HTTP readiness reported ready before reconciliation")
	}

	close(ready)
	deadline := time.Now().Add(time.Second)
	for gatewayHealthStatus(t, g, gatewayReadinessService) != healthpb.HealthCheckResponse_SERVING {
		if time.Now().After(deadline) {
			t.Fatal("readiness did not become serving after reconciliation")
		}
		time.Sleep(time.Millisecond)
	}
	if !g.isReady() {
		t.Fatal("HTTP readiness did not become ready after reconciliation")
	}

	g.startDraining()
	if got := gatewayHealthStatus(t, g, gatewayReadinessService); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness while draining = %v, want not serving", got)
	}
	if got := gatewayHealthStatus(t, g, gatewayLivenessService); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness while draining = %v, want serving", got)
	}
}

func TestDrainMiddlewareSetsConnectionCloseWhileDraining(t *testing.T) {
	g := &Gateway{}
	g.draining.Store(true)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	handler := g.drainMiddleware(func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})
	if err := handler(ctx); err != nil {
		t.Fatal(err)
	}

	if got := rec.Header().Get(echo.HeaderConnection); got != "close" {
		t.Fatalf("Connection header = %q, want close", got)
	}
}

func TestDrainMiddlewareSetsConnectionCloseWhenDrainStartsDuringRequest(t *testing.T) {
	g := &Gateway{}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	handler := g.drainMiddleware(func(c echo.Context) error {
		g.startDraining()
		return c.String(http.StatusOK, "ok")
	})
	if err := handler(ctx); err != nil {
		t.Fatal(err)
	}

	if got := rec.Header().Get(echo.HeaderConnection); got != "close" {
		t.Fatalf("Connection header = %q, want close", got)
	}
}

func TestStartDrainingMarksGatewayNotReady(t *testing.T) {
	g := &Gateway{}

	if !g.isReady() {
		t.Fatal("new gateway should be ready")
	}

	g.startDraining()
	if g.isReady() {
		t.Fatal("draining gateway should not be ready")
	}
}

func TestStartDrainingCancelsDrainContext(t *testing.T) {
	drainCtx, drainCancel := context.WithCancel(context.Background())
	g := &Gateway{drainCtx: drainCtx, drainCancelFunc: drainCancel}

	g.startDraining()

	select {
	case <-drainCtx.Done():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("drain context was not canceled")
	}
}

func TestGRPCGracefulStopTimeout(t *testing.T) {
	if got := grpcGracefulStopTimeout(0); got != gatewayGRPCShutdownMaxWait {
		t.Fatalf("zero grpc shutdown timeout = %s, want %s", got, gatewayGRPCShutdownMaxWait)
	}
	if got := grpcGracefulStopTimeout(5 * time.Second); got != 5*time.Second {
		t.Fatalf("short grpc shutdown timeout = %s, want caller timeout", got)
	}
	if got := grpcGracefulStopTimeout(gatewayGRPCShutdownMaxWait + time.Second); got != gatewayGRPCShutdownMaxWait {
		t.Fatalf("long grpc shutdown timeout = %s, want %s", got, gatewayGRPCShutdownMaxWait)
	}
}
