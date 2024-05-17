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

type MockContainerTestDetails struct {
	backendRepo   *repository.BackendRepository
	containerRepo *repository.ContainerRepository
	server        *echo.Echo
	httpDetails   *repository.MockHttpDetails
}

func setupContainerEchoGroup() MockContainerTestDetails {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatalf("Failed to create config manager: %v", err)
	}

	config := configManager.GetConfig()
	md := repository.MockBackendWithValidToken()

	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		log.Fatalf("Failed to create redis client: %v", err)
	}

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)

	e := echo.New()
	e.Use(auth.AuthMiddleware(md.BackendRepo))
	NewContainerGroup(e.Group("/container"), md.BackendRepo, containerRepo, config)

	return MockContainerTestDetails{
		backendRepo:   &md.BackendRepo,
		containerRepo: &containerRepo,
		server:        e,
		httpDetails:   &md,
	}
}

func TestListContainersByWorkspaceId(t *testing.T) {
	testDetails := setupContainerEchoGroup()

	// 1. Test with valid workspaceId
	testDetails.httpDetails.AddExpectedDBTokenSelect(testDetails.httpDetails.Mock, *testDetails.httpDetails.TokenForTest.Workspace, testDetails.httpDetails.TokenForTest)

	req := httptest.NewRequest(http.MethodGet, "/container/"+testDetails.httpDetails.TokenForTest.Workspace.ExternalId, nil)
	req.Header.Set("Authorization", "Bearer "+testDetails.httpDetails.TokenForTest.Key)
	rec := httptest.NewRecorder()
	testDetails.server.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "[]\n", rec.Body.String())
}
