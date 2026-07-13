package apiv1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

type fakePoolService struct {
	PoolService
}

func TestPoolRouteIgnoresForgedCapabilityInPayload(t *testing.T) {
	e := echo.New()
	service := &fakePoolService{}
	NewPoolGroup(e.Group("/api/v1/pools"), service)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/pools", strings.NewReader(`{"cluster_admin":true,"name":"forged","config":{"mode":"external"}}`))
	request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}
