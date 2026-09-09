package managedendpoint

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdminRESTEndpointIDWithSlash: vendor/slug IDs reach the endpoint routes
// when the slash is percent-encoded, which is how echo keeps :id to one
// segment.
func TestAdminRESTEndpointIDWithSlash(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	require.Contains(t, endpoint.Spec.ID, "/")

	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return next(&auth.HttpAuthContext{Context: c, AuthInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
				Token:     &types.Token{TokenType: types.TokenTypeClusterAdmin},
			}})
		}
	})
	s.mountAdminRoutes(e.Group("/api/v1/endpoints"))

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	rec := get("/api/v1/endpoints/acme%2Fmodel")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Endpoint struct {
			ID string `json:"id"`
		} `json:"endpoint"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, endpoint.Spec.ID, body.Endpoint.ID)

	rec = get("/api/v1/endpoints/acme%2Fmodel/replicas")
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// An unencoded slash is two segments and matches nothing.
	assert.Equal(t, http.StatusNotFound, get("/api/v1/endpoints/acme/model").Code)
}
