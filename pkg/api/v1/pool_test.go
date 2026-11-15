package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type fakePoolService struct {
	createCalled bool
	created      types.WorkerPoolConfig
}

func (*fakePoolService) ListManagedPools(context.Context, *auth.AuthInfo) ([]*model.ManagedPool, error) {
	return nil, nil
}

func (s *fakePoolService) CreateManagedPool(_ context.Context, _ *auth.AuthInfo, _ string, config types.WorkerPoolConfig) (*model.ManagedPool, error) {
	s.createCalled = true
	s.created = config
	return &model.ManagedPool{}, nil
}

func (*fakePoolService) UpdateManagedPool(context.Context, *auth.AuthInfo, string, types.WorkerPoolConfig) (*model.ManagedPool, error) {
	return nil, nil
}

func (*fakePoolService) DeleteManagedPool(context.Context, *auth.AuthInfo, string) error {
	return nil
}

func TestPoolRouteRejectsAuthenticatedNonAdminWithForgedCapability(t *testing.T) {
	e := echo.New()
	service := &fakePoolService{}
	nonAdminAuth := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			return next(&auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{ExternalId: "workspace-1"},
				Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary, Active: true},
			}})
		}
	}
	NewPoolGroup(e.Group("/api/v1/pools", nonAdminAuth), service)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/pools", strings.NewReader(`{"cluster_admin":true,"name":"forged","config":{"mode":"external"}}`))
	request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if service.createCalled {
		t.Fatal("pool service was called for a non-admin request")
	}
}

func postPool(t *testing.T, service PoolService, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	adminAuth := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			return next(&auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{ExternalId: "workspace-1"},
				Token:     &types.Token{TokenType: types.TokenTypeClusterAdmin, Active: true},
			}})
		}
	}
	NewPoolGroup(e.Group("/api/v1/pools", adminAuth), service)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/pools", strings.NewReader(body))
	request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	return recorder
}

func TestPoolRouteReadsDurationsAsText(t *testing.T) {
	service := &fakePoolService{}
	recorder := postPool(t, service, `{"name":"spot-l40s","config":{"gpu_type":"L40S","provisioning":{
		"provider":"verda","spot":true,"max_nodes":2,"idle_timeout":"5m","boot_timeout":"15m"}}}`)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body)
	}
	provisioning := service.created.Provisioning
	if provisioning.IdleAfter() != 5*time.Minute || provisioning.BootWithin() != 15*time.Minute {
		t.Fatalf("idle %s, boot %s", provisioning.IdleAfter(), provisioning.BootWithin())
	}
}

func TestPoolRouteSaysWhatIsWrongWithADuration(t *testing.T) {
	service := &fakePoolService{}
	recorder := postPool(t, service, `{"name":"spot-l40s","config":{"provisioning":{"provider":"verda","max_nodes":2,"idle_timeout":"five minutes"}}}`)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if !strings.Contains(recorder.Body.String(), `\"five minutes\" is not a duration`) {
		t.Fatalf("body = %s", recorder.Body)
	}
	if service.createCalled {
		t.Fatal("pool service was called with a config that could not be read")
	}
}
