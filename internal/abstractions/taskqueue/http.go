package taskqueue

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/types"

	"github.com/labstack/echo/v4"
)

type taskQueueGroup struct {
	routerGroup *echo.Group
	tq          *RedisTaskQueue
}

func registerTaskQueueRoutes(g *echo.Group, tq *RedisTaskQueue) *taskQueueGroup {
	group := &taskQueueGroup{routerGroup: g, tq: tq}

	g.POST("/id/:stubId", group.TaskQueuePut)
	g.POST("/:deploymentName/v:version", group.TaskQueuePut)

	return group
}

func (g *taskQueueGroup) TaskQueuePut(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	if deploymentName != "" && version != "" {
		version, err := strconv.Atoi(version)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": "invalid deployment version",
			})
		}

		deployment, err := g.tq.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeTaskQueueDeployment)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": "invalid deployment",
			})
		}

		stubId = deployment.Stub.ExternalId
	}

	var payload common.FunctionPayload
	payload, err := common.UnmarshalFunctionPayload(ctx)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request payload",
		})
	}

	taskId, err := g.tq.put(ctx.Request().Context(), cc.AuthInfo, stubId, &payload)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"task_id": taskId,
	})
}
