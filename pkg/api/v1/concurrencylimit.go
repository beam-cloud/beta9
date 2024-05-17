package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type ConcurrencyLimitGroup struct {
	routerGroup *echo.Group
	backendRepo repository.BackendRepository
}

func NewConcurrencyLimitGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
) *ConcurrencyLimitGroup {
	group := &ConcurrencyLimitGroup{routerGroup: g,
		backendRepo: backendRepo,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.GetConcurrencyLimitByWorkspaceId))
	g.POST("/:workspaceId", auth.WithClusterAdminAuth(group.CreateOrUpdateConcurrencyLimit))

	return group
}

func (c *ConcurrencyLimitGroup) GetConcurrencyLimitByWorkspaceId(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspace := cc.AuthInfo.Workspace

	concurrencyLimit, err := c.backendRepo.GetConcurrencyLimitByWorkspaceId(ctx.Request().Context(), *workspace.ConcurrencyLimitId)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get concurrency limit")
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}

func (c *ConcurrencyLimitGroup) CreateOrUpdateConcurrencyLimit(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspace := cc.AuthInfo.Workspace

	data := new(types.ConcurrencyLimit)
	if err := ctx.Bind(data); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode request body")
	}

	concurrencyLimit, err := c.backendRepo.GetConcurrencyLimitByWorkspaceId(ctx.Request().Context(), *workspace.ConcurrencyLimitId)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get concurrency limit")
	}

	if concurrencyLimit != nil {
		concurrencyLimit, err = c.backendRepo.UpdateConcurrencyLimit(ctx.Request().Context(), workspace.Id, data.GPULimit, data.CPULimit)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to update concurrency limit")
		}

		return ctx.JSON(http.StatusOK, concurrencyLimit)
	}

	concurrencyLimit, err = c.backendRepo.CreateConcurrencyLimit(ctx.Request().Context(), *workspace.ConcurrencyLimitId, data.GPULimit, data.CPULimit)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create concurrency limit")
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}
