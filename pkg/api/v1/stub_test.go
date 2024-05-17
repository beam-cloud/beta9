package apiv1

import (
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

type MockStubTestDetails struct {
	backendRepo *repository.BackendRepository
	server      *echo.Echo
	httpDetails *repository.MockHttpDetails
}

func setupStubEchoGroup() MockStubTestDetails {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatalf("Failed to create config manager: %v", err)
	}

	config := configManager.GetConfig()
	md := repository.MockBackendWithValidToken()

	e := echo.New()
	e.Use(auth.AuthMiddleware(md.BackendRepo))
	NewStubGroup(e.Group("/stub"), md.BackendRepo, config)

	return MockStubTestDetails{
		backendRepo: &md.BackendRepo,
		server:      e,
		httpDetails: &md,
	}
}

func addStubRowToQuery(mockHttpDetails *repository.MockHttpDetails) {
	mockHttpDetails.Mock.ExpectQuery("SELECT (.+) FROM stub").WillReturnRows(
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

func TestListStubs(t *testing.T) {
	testDetails := setupStubEchoGroup()

	// 1. Test with valid workspaceId
	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	addStubRowToQuery(testDetails.httpDetails)

	req := httptest.NewRequest(http.MethodGet, "/stub/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId, nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec := httptest.NewRecorder()
	testDetails.server.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}
