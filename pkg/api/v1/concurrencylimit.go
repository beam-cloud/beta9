package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type ConcurrencyLimitGroup struct {
	routerGroup   *echo.Group
	backendRepo   repository.BackendRepository
	workspaceRepo repository.WorkspaceRepository
}

func NewConcurrencyLimitGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
	workspaceRepo repository.WorkspaceRepository,
) *ConcurrencyLimitGroup {
	group := &ConcurrencyLimitGroup{routerGroup: g,
		backendRepo:   backendRepo,
		workspaceRepo: workspaceRepo,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListConcurrencyLimitsByWorkspaceId))
	g.GET("/:workspaceId/current", auth.WithWorkspaceAuth(group.GetConcurrencyLimitByWorkspaceId))
	g.POST("/:workspaceId", auth.WithClusterAdminAuth(group.CreateConcurrencyLimit))
	g.POST("/:workspaceId/revert/:concurrencyLimitId", auth.WithClusterAdminAuth(group.RevertConcurrencyLimit))
	g.DELETE("/:workspaceId", auth.WithClusterAdminAuth(group.DeleteConcurrencyLimit))

	return group
}

func (c *ConcurrencyLimitGroup) GetConcurrencyLimitByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	if workspace.ConcurrencyLimitId == nil {
		return HTTPNotFound()
	}

	concurrencyLimit, err := c.backendRepo.GetConcurrencyLimit(ctx.Request().Context(), *workspace.ConcurrencyLimitId)
	if err != nil {
		return HTTPInternalServerError("Failed to get concurrency limit")
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}

func (c *ConcurrencyLimitGroup) CreateConcurrencyLimit(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	data := new(types.ConcurrencyLimit)
	if err := ctx.Bind(data); err != nil {
		return HTTPBadRequest("Invalid request")
	}

	// Always create a new concurrency limit (no longer update existing ones)
	concurrencyLimit, err := c.backendRepo.CreateConcurrencyLimit(ctx.Request().Context(), workspace.Id, data.GPULimit, data.CPUMillicoreLimit)
	if err != nil {
		return HTTPInternalServerError("Failed to create concurrency limit")
	}

	err = c.workspaceRepo.SetConcurrencyLimitByWorkspaceId(workspaceId, concurrencyLimit)
	if err != nil {
		return HTTPInternalServerError("Failed to recache concurrency limit")
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}

func (c *ConcurrencyLimitGroup) DeleteConcurrencyLimit(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	if workspace.ConcurrencyLimitId == nil {
		return ctx.NoContent(http.StatusNoContent)
	}

	err = c.backendRepo.DeleteConcurrencyLimit(ctx.Request().Context(), workspace)
	if err != nil {
		return HTTPInternalServerError("Failed to delete concurrency limit")
	}

	return ctx.NoContent(http.StatusOK)
}

func (c *ConcurrencyLimitGroup) ListConcurrencyLimitsByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	limits, err := c.backendRepo.ListConcurrencyLimitsByWorkspaceId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPInternalServerError("Failed to get concurrency limits")
	}

	return ctx.JSON(http.StatusOK, limits)
}

func (c *ConcurrencyLimitGroup) RevertConcurrencyLimit(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	concurrencyLimitId := ctx.Param("concurrencyLimitId")

	concurrencyLimit, err := c.backendRepo.RevertConcurrencyLimit(ctx.Request().Context(), workspaceId, concurrencyLimitId)
	if err != nil {
		return HTTPInternalServerError("Failed to revert concurrency limit")
	}

	// Update cache
	err = c.workspaceRepo.SetConcurrencyLimitByWorkspaceId(workspaceId, concurrencyLimit)
	if err != nil {
		return HTTPInternalServerError("Failed to recache concurrency limit")
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}
