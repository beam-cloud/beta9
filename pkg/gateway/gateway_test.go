package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

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
