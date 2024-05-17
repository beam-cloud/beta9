package apiv1

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	repoCommon "github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/tj/assert"
)

type MockDeploymentTestDetails struct {
	backendRepo *repository.BackendRepository
	server      *echo.Echo
	httpDetails *repository.MockHttpDetails
}

func setupDeploymentEchoGroup() MockDeploymentTestDetails {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatalf("Failed to create config manager: %v", err)
	}

	config := configManager.GetConfig()
	md := repository.MockBackendWithValidToken()

	e := echo.New()
	e.Use(auth.AuthMiddleware(md.BackendRepo))
	NewDeploymentGroup(e.Group("/deployment"), md.BackendRepo, config)

	return MockDeploymentTestDetails{
		backendRepo: &md.BackendRepo,
		server:      e,
		httpDetails: &md,
	}
}

func addDeploymentRowSelectQuery(mockHttpDetails *repository.MockHttpDetails) {
	mockHttpDetails.Mock.ExpectQuery("SELECT (.+) FROM deployment").WillReturnRows(
		sqlmock.NewRows(
			[]string{
				"id",
				"external_id",
				"workspace_id",
			},
		).AddRow(
			1,
			mockHttpDetails.TokenForTest.Workspace.ExternalId,
			mockHttpDetails.TokenForTest.Workspace.Id,
		),
	)
}

func TestListDeployments(t *testing.T) {
	testDetails := setupDeploymentEchoGroup()

	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	// mock add expected db workspace select
	addWorkspaceRowSelectQuery(testDetails.httpDetails)
	// mock add expected db deployment select
	addDeploymentRowSelectQuery(testDetails.httpDetails)

	req := httptest.NewRequest(http.MethodGet, "/deployment/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId, nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec := httptest.NewRecorder()

	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 2. Test with invalid workspaceId
	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	req = httptest.NewRequest(http.MethodGet, "/deployment/invalid", nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec = httptest.NewRecorder()

	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// 3. Test with pagination

	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	// mock add expected db workspace select
	addWorkspaceRowSelectQuery(testDetails.httpDetails)
	// mock add expected db deployment select
	addDeploymentRowSelectQuery(testDetails.httpDetails)

	req = httptest.NewRequest(http.MethodGet, "/deployment/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId+"?pagination=true", nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec = httptest.NewRecorder()

	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// marshal response into common.CursorPaginationInfo[types.DeploymentWithRelated]

	var paginationResult repoCommon.CursorPaginationInfo[types.DeploymentWithRelated]

	err := json.Unmarshal(rec.Body.Bytes(), &paginationResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	assert.Equal(t, 1, len(paginationResult.Data))
	assert.Equal(t, "", paginationResult.Next)
}

func TestRetrieveDeployment(t *testing.T) {
	testDetails := setupDeploymentEchoGroup()

	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	// mock add expected db workspace select
	addWorkspaceRowSelectQuery(testDetails.httpDetails)
	// mock add expected db deployment select
	addDeploymentRowSelectQuery(testDetails.httpDetails)

	req := httptest.NewRequest(http.MethodGet, "/deployment/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId+"/1", nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec := httptest.NewRecorder()

	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 1. Test with invalid workspaceId
	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	req = httptest.NewRequest(http.MethodGet, "/deployment/invalid/1", nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec = httptest.NewRecorder()

	testDetails.server.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
