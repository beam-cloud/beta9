package bot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type sessionAuthBackendRepo struct {
	repository.BackendRepository
	stub *types.StubWithRelated
}

func (r *sessionAuthBackendRepo) GetStubByExternalId(context.Context, string, ...types.QueryFilter) (*types.StubWithRelated, error) {
	return r.stub, nil
}

func TestBotSessionAuthorizesBeforeWebsocketUpgrade(t *testing.T) {
	for _, tc := range []struct {
		name, workspace, config string
		status                  int
	}{
		{name: "private foreign stub", workspace: "other", config: `{"authorized":true}`, status: http.StatusNotFound},
		{name: "invalid foreign config", workspace: "other", config: `{`, status: http.StatusNotFound},
		{name: "null foreign config", workspace: "other", config: `null`, status: http.StatusNotFound},
		// Allowed calls reach the upgrader, which rejects this ordinary HTTP request.
		{name: "owner", workspace: "owner", config: `{"authorized":true}`, status: http.StatusBadRequest},
		{name: "public foreign stub", workspace: "other", config: `{"authorized":false}`, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := echo.New()
			pbs := &PetriBotService{backendRepo: &sessionAuthBackendRepo{stub: &types.StubWithRelated{
				Stub:      types.Stub{ExternalId: "stub", Config: tc.config},
				Workspace: types.Workspace{ExternalId: "owner"},
			}}}
			registerBotRoutes(e.Group("/bot"), pbs)
			e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
				return func(c echo.Context) error {
					return next(&auth.HttpAuthContext{Context: c, AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: tc.workspace}}})
				}
			})
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bot/id/stub", nil))
			require.Equal(t, tc.status, rec.Code)
		})
	}
}
