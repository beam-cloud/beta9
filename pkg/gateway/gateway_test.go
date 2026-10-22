package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	call := m.calls.Add(1)
	current := m.concurrent.Add(1)
	defer m.concurrent.Add(-1)
	for {
		maximum := m.max.Load()
		if current <= maximum || m.max.CompareAndSwap(maximum, current) {
			break
		}
	}
	if call == 1 {
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
	g := &Gateway{ctx: ctx}
	g.newHealthServer()

	if got := gatewayHealthStatus(t, g, gatewayLivenessService); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness = %v, want serving", got)
	}
	if got := gatewayHealthStatus(t, g, gatewayReadinessService); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("readiness = %v, want serving", got)
	}
	if !g.isReady() {
		t.Fatal("HTTP readiness did not become ready")
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

func TestGatewayDrainReconnectsObserversAndFinishesActiveRequests(t *testing.T) {
	drain, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &Gateway{ctx: ctx, cancelFunc: cancel, drainCtx: drain, drainCancelFunc: cancelDrain, grpcServer: grpc.NewServer()}
	g.Config.GatewayService.ShutdownTimeout = 2 * time.Second

	e := echo.New()
	streamDone := make(chan struct{})
	e.GET("/events", func(c echo.Context) error {
		c.Response().WriteHeader(http.StatusOK)
		c.Response().Flush()
		<-c.Request().Context().Done()
		close(streamDone)
		return nil
	}, apiv1.DrainOnShutdown(drain))
	requestStarted := make(chan struct{})
	finishRequest := make(chan struct{})
	e.POST("/request", func(c echo.Context) error {
		close(requestStarted)
		select {
		case <-finishRequest:
			return c.String(http.StatusOK, "finished")
		case <-c.Request().Context().Done():
			return c.Request().Context().Err()
		}
	})
	server := httptest.NewServer(e)
	defer server.Close()
	defer cancelDrain()
	defer func() {
		select {
		case <-finishRequest:
		default:
			close(finishRequest)
		}
	}()
	g.httpServer = server.Config
	response, err := server.Client().Get(server.URL + "/events")
	require.NoError(t, err)
	defer response.Body.Close()

	requestDone := make(chan error, 1)
	go func() {
		response, err := server.Client().Post(server.URL+"/request", "text/plain", nil)
		if err == nil {
			defer response.Body.Close()
			var data []byte
			data, err = io.ReadAll(response.Body)
			if err == nil && (response.StatusCode != http.StatusOK || string(data) != "finished") {
				err = fmt.Errorf("active request returned status %d, body %q", response.StatusCode, data)
			}
		}
		requestDone <- err
	}()
	<-requestStarted
	g.startDraining()
	select {
	case <-streamDone:
	case <-time.After(time.Second):
		t.Fatal("observational stream did not disconnect during drain")
	}
	shutdownDone := make(chan struct{})
	go func() { g.shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
		t.Fatal("gateway stopped before the active request finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(finishRequest)
	require.NoError(t, <-requestDone)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop after the active request finished")
	}
}

func TestGatewayShutdownDeadlineClosesActiveHTTPConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestStarted := make(chan struct{})
	requestClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
		close(requestClosed)
	}))
	defer server.Close()
	g := &Gateway{ctx: ctx, cancelFunc: cancel, httpServer: server.Config, grpcServer: grpc.NewServer()}
	g.Config.GatewayService.ShutdownTimeout = 30 * time.Millisecond
	requestDone := make(chan error, 1)
	go func() {
		response, err := server.Client().Get(server.URL)
		if err == nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	<-requestStarted
	g.shutdown()
	select {
	case <-requestClosed:
	case <-time.After(time.Second):
		t.Fatal("HTTP shutdown deadline left the active connection open")
	}
	require.Error(t, <-requestDone)
}
