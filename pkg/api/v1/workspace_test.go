package apiv1

import (
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/tj/assert"
)

type MockWorkspaceTestDetails struct {
	backendRepo *repository.BackendRepository
	server      *echo.Echo
	httpDetails *repository.MockHttpDetails
}

func setupWorkspaceEchoGroup() MockWorkspaceTestDetails {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatalf("Failed to create config manager: %v", err)
	}

	config := configManager.GetConfig()
	md := repository.MockBackendWithValidToken()

	e := echo.New()
	e.Use(auth.AuthMiddleware(md.BackendRepo))
	NewWorkspaceGroup(e.Group("/workspace"), md.BackendRepo, config)

	return MockWorkspaceTestDetails{
		backendRepo: &md.BackendRepo,
		server:      e,
		httpDetails: &md,
	}
}

func TestCreateWorkspace(t *testing.T) {
	testDetails := setupWorkspaceEchoGroup()

	// Try to create a workspace with a non-admin token
	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)
	req := httptest.NewRequest(http.MethodPost, "/workspace", nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)

	rec := httptest.NewRecorder()
	testDetails.server.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)

	// Try to create a workspace with an admin token
	tokenForTestAsAdmin := testDetails.httpDetails.TokenForTest
	tokenForTestAsAdmin.TokenType = types.TokenTypeClusterAdmin

	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, tokenForTestAsAdmin)
	testDetails.httpDetails.Mock.ExpectQuery("INSERT INTO workspace").WillReturnRows(
		testDetails.httpDetails.Mock.NewRows([]string{"id"}).AddRow(1),
	)

	req = httptest.NewRequest(http.MethodPost, "/workspace", nil)
	req.Header.Set("Authorization", "Bearer "+tokenForTestAsAdmin.Key)

	rec = httptest.NewRecorder()
	testDetails.server.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}
