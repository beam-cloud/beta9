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

type fakePoolService struct {
	createCalled bool
}

func (*fakePoolService) ListManagedPools(context.Context, *auth.AuthInfo) ([]*model.ManagedPool, error) {
	return nil, nil
}

func (s *fakePoolService) CreateManagedPool(context.Context, *auth.AuthInfo, string, types.WorkerPoolConfig) (*model.ManagedPool, error) {
	s.createCalled = true
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
