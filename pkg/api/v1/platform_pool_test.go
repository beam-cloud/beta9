package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type fakePlatformPoolService struct {
	calls int
}

func (s *fakePlatformPoolService) ListPlatformPools(context.Context, *auth.AuthInfo) ([]*model.PlatformPool, error) {
	s.calls++
	return nil, nil
}

func (s *fakePlatformPoolService) CreatePlatformPool(context.Context, *auth.AuthInfo, string, types.WorkerPoolConfig) (*model.PlatformPool, error) {
	s.calls++
	return nil, nil
}

func (s *fakePlatformPoolService) UpdatePlatformPool(context.Context, *auth.AuthInfo, string, types.WorkerPoolConfig) (*model.PlatformPool, error) {
	s.calls++
	return nil, nil
}

func (s *fakePlatformPoolService) DeletePlatformPool(context.Context, *auth.AuthInfo, string) error {
	s.calls++
	return nil
}

func TestPlatformPoolRouteRejectsUnauthenticatedRequestBeforeService(t *testing.T) {
	e := echo.New()
	service := &fakePlatformPoolService{}
	NewPlatformPoolGroup(e.Group("/api/v1/platform/pools"), service)

	request := httptest.NewRequest(http.MethodGet, "/api/v1/platform/pools", nil)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if service.calls != 0 {
		t.Fatalf("service calls = %d, want 0", service.calls)
	}
}

func TestPlatformPoolRouteIgnoresForgedCapabilityInPayload(t *testing.T) {
	e := echo.New()
	service := &fakePlatformPoolService{}
	NewPlatformPoolGroup(e.Group("/api/v1/platform/pools"), service)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/platform/pools", strings.NewReader(`{"platform_operator":true,"name":"forged","config":{"mode":"external"}}`))
	request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if service.calls != 0 {
		t.Fatalf("service calls = %d, want 0", service.calls)
	}
}
