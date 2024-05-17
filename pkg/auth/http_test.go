package auth

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

type MockDetails struct {
	backendRepo  repository.BackendRepository
	mock         sqlmock.Sqlmock
	tokenForTest types.Token
}

func addTokenRow(
	mock sqlmock.Sqlmock,
	workspace types.Workspace,
	token types.Token,
) {
	mock.ExpectQuery("SELECT (.+) FROM token").
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"id", "external_id", "key", "active", "reusable", "workspace_id", "workspace.external_id", "token_type", "created_at", "updated_at"},
			).AddRow(
				token.Id,
				token.ExternalId,
				token.Key,
				token.Active,
				token.Reusable,
				token.WorkspaceId,
				workspace.ExternalId,
				token.TokenType,
				token.CreatedAt,
				token.UpdatedAt,
			),
		)
}

func mockBackendWithValidToken() MockDetails {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()
	workspaceForTest := types.Workspace{
		Id:         1,
		ExternalId: "test",
		Name:       "test",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	tokenForTest := types.Token{
		Id:          1,
		ExternalId:  "test",
		Key:         "test",
		Active:      true,
		Reusable:    true,
		WorkspaceId: &workspaceForTest.Id,
		Workspace:   &workspaceForTest,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	return MockDetails{
		backendRepo:  backendRepo,
		mock:         mock,
		tokenForTest: tokenForTest,
	}
}

func TestAuthMiddleWare(t *testing.T) {
	mockDetails := mockBackendWithValidToken()

	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo))

	// 1. Test with valid token
	e.GET("/", func(ctx echo.Context) error {
		cc, _ := ctx.(*HttpAuthContext)

		assert.NotNil(t, cc.AuthInfo)
		assert.NotNil(t, cc.AuthInfo.Token)
		assert.NotNil(t, cc.AuthInfo.Workspace)
		assert.NotNil(t, cc.AuthInfo.Workspace.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.ExternalId, mockDetails.tokenForTest.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.Key, mockDetails.tokenForTest.Key)

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
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 200,
			prepares: func() {
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:           "Test with empty token",
			tokenKey:       "",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
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
	mockDetails := mockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo))

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
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    mockDetails.tokenForTest.Workspace.ExternalId,
			expectedStatus: 200,
			prepares: func() {
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with valid token and correct workspace",
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 401,
			prepares: func() {
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test correct workspace with admin user override",
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 200,
			prepares: func() {
				tokenForTest := mockDetails.tokenForTest
				tokenForTest.TokenType = types.TokenTypeClusterAdmin
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, tokenForTest)
			},
		},
		{
			name:           "Test correct workspace and admin user override",
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    mockDetails.tokenForTest.Workspace.ExternalId,
			expectedStatus: 200,
			prepares: func() {
				tokenForTest := mockDetails.tokenForTest
				tokenForTest.TokenType = types.TokenTypeClusterAdmin
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, tokenForTest)
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

func TestWithClusterAdminAuth(t *testing.T) {
	mockDetails := mockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo))

	e.GET("/", WithClusterAdminAuth(func(c echo.Context) error {
		assert.NotNil(t, c.(*HttpAuthContext).AuthInfo.Token)
		assert.Equal(t, c.(*HttpAuthContext).AuthInfo.Token.TokenType, types.TokenTypeClusterAdmin)
		return c.String(200, "OK")
	}))

	tests := []struct {
		name           string
		tokenKey       string
		expectedStatus int
		prepares       func()
	}{
		{
			name:           "Test with valid token but not admin user",
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 401,
			prepares: func() {
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with valid token and admin user",
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 200,
			prepares: func() {
				tokenForTest := mockDetails.tokenForTest
				tokenForTest.TokenType = types.TokenTypeClusterAdmin
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, tokenForTest)
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
