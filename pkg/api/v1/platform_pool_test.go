package apiv1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

type fakePlatformPoolService struct {
	PlatformPoolService
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
}
