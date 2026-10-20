package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

type downloadPackageRepository struct {
	repository.BackendRepository
}

func (*downloadPackageRepository) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	return types.Workspace{Id: 1, Name: "workspace"}, nil
}

func (*downloadPackageRepository) GetWorkspace(context.Context, uint) (*types.Workspace, error) {
	return &types.Workspace{Id: 1, Name: "workspace", Storage: &types.WorkspaceStorage{
		BucketName: ptr.To("workspace-bucket"), Region: ptr.To("us-east-1"),
		AccessKey: ptr.To("test-access-key"), SecretKey: ptr.To("test-secret-key"),
		EndpointUrl: ptr.To("https://storage.example"),
	}}, nil
}

func (*downloadPackageRepository) GetObjectByExternalStubId(context.Context, string, uint) (types.Object, error) {
	return types.Object{ExternalId: "source-object"}, nil
}

func TestDownloadDeploymentPackageLoadsWorkspaceStorage(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	ctx := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
	ctx.SetParamNames("workspaceId", "stubId")
	ctx.SetParamValues("workspace", "stub")
	group := &DeploymentGroup{backendRepo: &downloadPackageRepository{}}
	group.config.Storage.WorkspaceStorage.DefaultEndpointUrl = "https://storage.example"
	group.config.Storage.WorkspaceStorage.DefaultPresignedEndpointUrl = "https://public-storage.example"

	require.NoError(t, group.DownloadDeploymentPackage(ctx))
	require.Equal(t, http.StatusTemporaryRedirect, rec.Code)
	location, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "public-storage.example", location.Host)
	require.Equal(t, "/workspace-bucket/objects/source-object", location.Path)
	require.NotEmpty(t, location.Query().Get("X-Amz-Signature"))
}
