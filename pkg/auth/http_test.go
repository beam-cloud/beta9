package auth

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

func TestAuthMiddleWare(t *testing.T) {
	md := repository.MockBackendWithValidToken()

	e := echo.New()
	e.Use(AuthMiddleware(md.BackendRepo))

	// 1. Test with valid token
	e.GET("/", func(ctx echo.Context) error {
		cc, _ := ctx.(*HttpAuthContext)

		assert.NotNil(t, cc.AuthInfo)
		assert.NotNil(t, cc.AuthInfo.Token)
		assert.NotNil(t, cc.AuthInfo.Workspace)
		assert.NotNil(t, cc.AuthInfo.Workspace.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.ExternalId, md.TokenForTest.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.Key, md.TokenForTest.Key)

		return ctx.String(200, "OK")
	})

	tests := []struct {
		name           string
		tokenKey       string
		prepares       func()
		expectedStatus int
	}{
		{
			name:           "Test with valid token",
			tokenKey:       md.TokenForTest.Key,
			expectedStatus: 200,
			prepares: func() {
				md.AddExpectedDBTokenSelect(md.Mock, *md.TokenForTest.Workspace, md.TokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			expectedStatus: 401,
			prepares: func() {
				md.Mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:           "Test with empty token",
			tokenKey:       "",
			expectedStatus: 401,
			prepares: func() {
				md.Mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/", nil)
			req.Header.Set("Authorization", tt.tokenKey)
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}

}

func TestWithWorkspaceAuth(t *testing.T) {
	md := repository.MockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(md.BackendRepo))

	e.GET("/:workspaceId", WithWorkspaceAuth(func(c echo.Context) error {
		if c.(*HttpAuthContext).AuthInfo.Token.TokenType == types.TokenTypeClusterAdmin {
			return c.String(200, "OK")
		}

		assert.NotNil(t, c.(*HttpAuthContext).AuthInfo.Workspace)
		assert.NotNil(t, c.(*HttpAuthContext).AuthInfo.Workspace.ExternalId)
		assert.NotNil(t, c.(*HttpAuthContext).Param("workspaceId"))
		assert.Equal(t, c.(*HttpAuthContext).AuthInfo.Workspace.ExternalId, c.(*HttpAuthContext).Param("workspaceId"))
		return c.String(200, "OK")
	}))

	tests := []struct {
		name           string
		tokenKey       string
		workspaceId    string
		expectedStatus int
		prepares       func()
	}{
		{
			name:           "Test with valid token and workspace",
			tokenKey:       md.TokenForTest.Key,
			workspaceId:    md.TokenForTest.Workspace.ExternalId,
			expectedStatus: 200,
			prepares: func() {
				md.AddExpectedDBTokenSelect(md.Mock, *md.TokenForTest.Workspace, md.TokenForTest)
			},
		},
		{
			name:           "Test with valid token and correct workspace",
			tokenKey:       md.TokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 401,
			prepares: func() {
				md.AddExpectedDBTokenSelect(md.Mock, *md.TokenForTest.Workspace, md.TokenForTest)
			},
		},
		{
			name:           "Test correct workspace with admin user override",
			tokenKey:       md.TokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 200,
			prepares: func() {
				TokenForTest := md.TokenForTest
				TokenForTest.TokenType = types.TokenTypeClusterAdmin
				md.AddExpectedDBTokenSelect(md.Mock, *md.TokenForTest.Workspace, TokenForTest)
			},
		},
		{
			name:           "Test correct workspace and admin user override",
			tokenKey:       md.TokenForTest.Key,
			workspaceId:    md.TokenForTest.Workspace.ExternalId,
			expectedStatus: 200,
			prepares: func() {
				TokenForTest := md.TokenForTest
				TokenForTest.TokenType = types.TokenTypeClusterAdmin
				md.AddExpectedDBTokenSelect(md.Mock, *md.TokenForTest.Workspace, TokenForTest)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/"+tt.workspaceId, nil)
			req.Header.Set("Authorization", tt.tokenKey)
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}

}
