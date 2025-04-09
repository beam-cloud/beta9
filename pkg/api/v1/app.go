package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/repository/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type AppGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewAppGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *AppGroup {
	group := &AppGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.GET("/:workspaceId/latest", auth.WithWorkspaceAuth(group.ListAppWithLatestActivity))

	return group
}

type AppWithLatestStubOrDeployment struct {
	types.App
	Stub       *types.StubWithRelated       `json:"stub"`
	Deployment *types.DeploymentWithRelated `json:"deployment"`
}

func (a *AppGroup) ListAppWithLatestActivity(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceID := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceID && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPNotFound()
	}

	workspace, err := a.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return HTTPBadRequest("Failed to retrieve workspace")
	}

	var filters types.AppFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	apps, err := a.backendRepo.ListAppsPaginated(ctx.Request().Context(), workspace.Id, filters)
	if err != nil {
		return err
	}

	appsWithLatest := common.CursorPaginationInfo[AppWithLatestStubOrDeployment]{
		Data: make([]AppWithLatestStubOrDeployment, len(apps.Data)),
		Next: apps.Next,
	}

	for i := range apps.Data {
		appsWithLatest.Data[i].App = apps.Data[i]

		deployments, err := a.backendRepo.ListDeploymentsWithRelated(ctx.Request().Context(), types.DeploymentFilter{AppId: apps.Data[i].ExternalId})
		if err != nil {
			return HTTPBadRequest("Failed to get apps")
		}

		if deployments != nil && len(deployments) > 0 {

			appsWithLatest.Data[i].Deployment = &deployments[0]
			continue
		}

		// If the app doesn't have a deployment, we get the latest stub
		stubs, err := a.backendRepo.ListStubs(ctx.Request().Context(), types.StubFilter{AppId: apps.Data[i].ExternalId})
		if err != nil {
			return HTTPBadRequest("Failed to get apps")
		}

		if stubs == nil || len(stubs) == 0 {
			return HTTPBadRequest("No stubs or deployments found for app")
		}

		appsWithLatest.Data[i].Stub = &stubs[0]
		appsWithLatest.Data[i].Stub.SanitizeConfig()
	}

	return ctx.JSON(
		http.StatusOK,
		appsWithLatest,
	)
}
