package apiv1

import (
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/tj/assert"
)

type MockTokenTestDetails struct {
	backendRepo *repository.BackendRepository
	server      *echo.Echo
	httpDetails *repository.MockHttpDetails
}

func setupTokenEchoGroup() MockTokenTestDetails {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatalf("Failed to create config manager: %v", err)
	}

	config := configManager.GetConfig()
	md := repository.MockBackendWithValidToken()

	e := echo.New()
	e.Use(auth.AuthMiddleware(md.BackendRepo))
	NewTokenGroup(e.Group("/token"), md.BackendRepo, config)

	return MockTokenTestDetails{
		backendRepo: &md.BackendRepo,
		server:      e,
		httpDetails: &md,
	}
}

func TestListWorkspaceTokens(t *testing.T) {
	testDetails := setupTokenEchoGroup()

	tests := []struct {
		name           string
		prepares       func()
		expectedStatus int
	}{
		{
			name:           "Test list",
			expectedStatus: 200,
			prepares: func() {
				// First query is for token select on the auth middleware
				testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
				// Second query is for the workspace check inside the token list method
				addWorkspaceRowToQuery(testDetails.httpDetails)
				// Third query is for the token list backend
				testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
			},
		},
		{
			name: "Test error occurs in db",
			prepares: func() {
				testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
				addWorkspaceRowToQuery(testDetails.httpDetails)
				testDetails.httpDetails.Mock.ExpectQuery("SELECT (.+) FROM token").
					WillReturnError(errors.New("invalid token"))
			},
			expectedStatus: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(http.MethodGet, "/token/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId, nil)
			req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
			rec := httptest.NewRecorder()

			testDetails.server.ServeHTTP(rec, req)
			if tt.expectedStatus != 0 {
				assert.Equal(t, tt.expectedStatus, rec.Code)
			}
		})
	}
}

func TestRevokeToken(t *testing.T) {
	testDetails := setupTokenEchoGroup()

	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
	req := httptest.NewRequest(http.MethodDelete, "/token/"+testDetails.httpDetails.TokenForTest.ExternalId, nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)

	rec := httptest.NewRecorder()
	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateWorkspaceToken(t *testing.T) {
	testDetails := setupTokenEchoGroup()

	tests := []struct {
		name           string
		key            string
		prepares       func()
		expectedStatus int
	}{
		{
			name: "Test create",
			key:  "new-key",
			prepares: func() {
				testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
				addWorkspaceRowToQuery(testDetails.httpDetails)
				testDetails.httpDetails.Mock.ExpectQuery("INSERT INTO token").WillReturnRows(
					sqlmock.NewRows([]string{"id", "key"}).AddRow(2, "new-key"),
				)
			},
			expectedStatus: 200,
		},
		{
			name: "Test error occurs in db",
			key:  "",
			prepares: func() {
				testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
				addWorkspaceRowToQuery(testDetails.httpDetails)
				testDetails.httpDetails.Mock.ExpectExec("INSERT INTO token").
					WillReturnError(errors.New("invalid token"))
			},
			expectedStatus: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()

			req := httptest.NewRequest(http.MethodPost, "/token/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId, nil)
			req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
			rec := httptest.NewRecorder()

			testDetails.server.ServeHTTP(rec, req)
			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.expectedStatus == 200 {
				assert.Contains(t, rec.Body.String(), tt.key)
			}
		})
	}
}
