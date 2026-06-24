package apiv1

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestHealthCheckReportsDrainingBeforeDependencyChecks(t *testing.T) {
	e := echo.New()
	group := &HealthGroup{ready: func() bool { return false }}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()

	err := group.HealthCheck(e.NewContext(req, rec))
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
