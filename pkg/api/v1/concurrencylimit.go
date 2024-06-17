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

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.GetConcurrencyLimitByWorkspaceId))
	g.POST("/:workspaceId", auth.WithClusterAdminAuth(group.CreateOrUpdateConcurrencyLimit))
	g.DELETE("/:workspaceId", auth.WithClusterAdminAuth(group.DeleteConcurrencyLimit))

	return group
}

func (c *ConcurrencyLimitGroup) GetConcurrencyLimitByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	if workspace.ConcurrencyLimitId == nil {
		return ctx.JSON(http.StatusNotFound, map[string]interface{}{
			"error": "concurrency limit not found",
		})
	}

	concurrencyLimit, err := c.backendRepo.GetConcurrencyLimit(ctx.Request().Context(), *workspace.ConcurrencyLimitId)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to get concurrency limit",
		})
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}

func (c *ConcurrencyLimitGroup) CreateOrUpdateConcurrencyLimit(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	data := new(types.ConcurrencyLimit)
	if err := ctx.Bind(data); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request",
		})
	}

	if workspace.ConcurrencyLimitId != nil {
		concurrencyLimit, err := c.backendRepo.UpdateConcurrencyLimit(ctx.Request().Context(), *workspace.ConcurrencyLimitId, data.GPULimit, data.CPUMillicoreLimit)
		if err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"error": "failed to update concurrency limit",
			})
		}

		err = c.workspaceRepo.SetConcurrencyLimitByWorkspaceId(workspaceId, concurrencyLimit)
		if err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"error": "failed to recache concurrency limit",
			})
		}

		return ctx.JSON(http.StatusOK, concurrencyLimit)
	}

	concurrencyLimit, err := c.backendRepo.CreateConcurrencyLimit(ctx.Request().Context(), workspace.Id, data.GPULimit, data.CPUMillicoreLimit)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to create concurrency limit",
		})
	}

	err = c.workspaceRepo.SetConcurrencyLimitByWorkspaceId(workspaceId, concurrencyLimit)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to recache concurrency limit",
		})
	}

	return ctx.JSON(http.StatusOK, concurrencyLimit)
}

func (c *ConcurrencyLimitGroup) DeleteConcurrencyLimit(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid workspace ID",
		})
	}

	if workspace.ConcurrencyLimitId == nil {
		return ctx.NoContent(http.StatusNoContent)
	}

	err = c.backendRepo.DeleteConcurrencyLimit(ctx.Request().Context(), workspace)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to delete concurrency limit",
		})
	}

	return ctx.NoContent(http.StatusOK)
}
