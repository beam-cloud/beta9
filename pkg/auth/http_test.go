package auth

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

type MockDetails struct {
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
	mock          sqlmock.Sqlmock
	tokenForTest  types.Token
	mockRedis     *common.RedisClient
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

	mockRedis, err := repository.NewRedisClientForTest()
	if err != nil {
		panic(err)
	}
	workspaceRepo := repository.NewWorkspaceRedisRepositoryForTest(mockRedis)

	return MockDetails{
		backendRepo:   backendRepo,
		workspaceRepo: workspaceRepo,
		mock:          mock,
		mockRedis:     mockRedis,
		tokenForTest:  tokenForTest,
	}
}

func TestAuthMiddleWare(t *testing.T) {
	mockDetails := mockBackendWithValidToken()

	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo, mockDetails.workspaceRepo))

	// 1. Test with valid token
	e.GET("/", func(ctx echo.Context) error {
		cc, ok := ctx.(*HttpAuthContext)
		if !ok {
			// All requests will by default allow pass through
			// Thats why we need route specific auth wrappers
			// Auth middleware functions as an auth parser
			return ctx.String(200, "OK")
		}

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
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:           "Test with empty token. Should pass through",
			tokenKey:       "",
			expectedStatus: 200,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/", nil)

			if tt.tokenKey != "" {
				req.Header.Set("Authorization", tt.tokenKey)
			}
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}

}

func TestWithAuth(t *testing.T) {
	mockDetails := mockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo, mockDetails.workspaceRepo))

	e.GET("/", WithAuth(func(c echo.Context) error {
		cc, ok := c.(*HttpAuthContext)
		if !ok {
			return c.String(200, "OK")
		}

		assert.NotNil(t, cc.AuthInfo)
		assert.NotNil(t, cc.AuthInfo.Token)
		assert.NotNil(t, cc.AuthInfo.Workspace)
		assert.NotNil(t, cc.AuthInfo.Workspace.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.ExternalId, mockDetails.tokenForTest.ExternalId)
		assert.Equal(t, cc.AuthInfo.Token.Key, mockDetails.tokenForTest.Key)

		return c.String(200, "OK")
	}))

	tests := []struct {
		name           string
		tokenKey       string
		expectedStatus int
		prepares       func()
	}{
		{
			name:           "Test with valid token",
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 200,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:           "Test with empty token. Should fail to authenticate",
			tokenKey:       "",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/", nil)

			if tt.tokenKey != "" {
				req.Header.Set("Authorization", tt.tokenKey)
			}
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}
}

func TestWithWorkspaceAuth(t *testing.T) {
	mockDetails := mockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo, mockDetails.workspaceRepo))

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
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with valid token and correct workspace",
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test correct workspace with admin user override",
			tokenKey:       mockDetails.tokenForTest.Key,
			workspaceId:    "invalid",
			expectedStatus: 200,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
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
				mockDetails.mockRedis.FlushAll(context.Background())
				tokenForTest := mockDetails.tokenForTest
				tokenForTest.TokenType = types.TokenTypeClusterAdmin
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, tokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			workspaceId:    mockDetails.tokenForTest.Workspace.ExternalId,
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:        "Test no authorization header",
			workspaceId: mockDetails.tokenForTest.Workspace.ExternalId,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
			expectedStatus: 401,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/"+tt.workspaceId, nil)

			if tt.tokenKey != "" {
				req.Header.Set("Authorization", tt.tokenKey)
			}
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}

}

func TestWithClusterAdminAuth(t *testing.T) {
	mockDetails := mockBackendWithValidToken()
	e := echo.New()
	e.Use(AuthMiddleware(mockDetails.backendRepo, mockDetails.workspaceRepo))

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
			name:           "Test with valid token",
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
		},
		{
			name:           "Test with admin token",
			tokenKey:       mockDetails.tokenForTest.Key,
			expectedStatus: 200,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				tokenForTest := mockDetails.tokenForTest
				tokenForTest.TokenType = types.TokenTypeClusterAdmin
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, tokenForTest)
			},
		},
		{
			name:           "Test with invalid token",
			tokenKey:       "invalid",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
		{
			name:           "Test no authorization header",
			expectedStatus: 401,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				mockDetails.mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(echo.GET, "/", nil)

			if tt.tokenKey != "" {
				req.Header.Set("Authorization", tt.tokenKey)
			}
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}
}
