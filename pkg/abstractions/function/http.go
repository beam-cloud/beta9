package function

import (
	"net/http"
	"strconv"
	"strings"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type functionGroup struct {
	routerGroup *echo.Group
	fs          *RunCFunctionService
}

func registerFunctionRoutes(g *echo.Group, fs *RunCFunctionService) *functionGroup {
	group := &functionGroup{routerGroup: g, fs: fs}

	g.POST("/id/:stubId", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.FunctionInvoke))

	return group
}

func (g *functionGroup) FunctionInvoke(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	stubType := types.StubTypeFunctionDeployment
	if strings.HasPrefix(ctx.Path(), scheduleRoutePrefix) {
		stubType = types.StubTypeScheduledJobDeployment
	}

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.fs.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, stubType, true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.fs.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), stubType)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		}

		if deployment == nil {
			return apiv1.HTTPBadRequest("Invalid deployment")
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

	task, err := g.fs.invoke(ctx.Request().Context(), cc.AuthInfo, stubId, payload)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"task_id": task.Metadata().TaskId,
	})
}
