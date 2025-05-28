package taskqueue

import (
	"net/http"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/labstack/echo/v4"
)

type taskQueueGroup struct {
	routeGroup *echo.Group
	tq         *RedisTaskQueue
	cache      *expirable.LRU[string, string]
}

func registerTaskQueueRoutes(g *echo.Group, tq *RedisTaskQueue) *taskQueueGroup {
	group := &taskQueueGroup{routeGroup: g, tq: tq, cache: abstractions.NewDeploymentStubCache()}

	g.POST("/id/:stubId", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.TaskQueuePut))
	g.POST("/public/:stubId", auth.WithAssumedStubAuth(group.TaskQueuePut, group.tq.isPublic))

	g.POST("/id/:stubId/warmup", auth.WithAuth(group.TaskQueueWarmUp))
	g.POST("/:deploymentName/warmup", auth.WithAuth(group.TaskQueueWarmUp))
	g.POST("/:deploymentName/latest/warmup", auth.WithAuth(group.TaskQueueWarmUp))
	g.POST("/:deploymentName/v:version/warmup", auth.WithAuth(group.TaskQueueWarmUp))

	return group
}

func (g *taskQueueGroup) TaskQueuePut(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		g.cache,
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		types.StubTypeTaskQueueDeployment,
		g.tq.backendRepo,
	)
	if err != nil {
		return err
	}

	payload, err := task.SerializeHttpPayload(ctx)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": err.Error(),
		})
	}

	taskId, err := g.tq.put(ctx.Request().Context(), cc.AuthInfo, stubId, payload)
	if err != nil {
		if _, ok := err.(*types.ErrExceededTaskLimit); ok {
			return ctx.JSON(http.StatusTooManyRequests, map[string]interface{}{
				"error": err.Error(),
			})
		}

		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"task_id": taskId,
	})
}

func (g *taskQueueGroup) TaskQueueWarmUp(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		g.cache,
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		types.StubTypeTaskQueueDeployment,
		g.tq.backendRepo,
	)
	if err != nil {
		return err
	}

	err = g.tq.controller.Warmup(
		ctx.Request().Context(),
		stubId,
	)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
	}

	return ctx.NoContent(http.StatusOK)
}
