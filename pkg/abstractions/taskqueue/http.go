package taskqueue

import (
	"net/http"
	"strconv"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/labstack/echo/v4"
)

type taskQueueGroup struct {
	routeGroup *echo.Group
	tq         *RedisTaskQueue
}

func registerTaskQueueRoutes(g *echo.Group, tq *RedisTaskQueue) *taskQueueGroup {
	group := &taskQueueGroup{routeGroup: g, tq: tq}

	g.POST("/id/:stubId", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.TaskQueuePut))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.TaskQueuePut))

	return group
}

func (g *taskQueueGroup) TaskQueuePut(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.tq.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, types.StubTypeTaskQueueDeployment, true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.tq.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeTaskQueueDeployment)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		}

		if !deployment.Active {
			return apiv1.HTTPBadRequest("Deployment is not active")
		}

		stubId = deployment.Stub.ExternalId
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
