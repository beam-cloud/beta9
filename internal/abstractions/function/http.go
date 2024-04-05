package function

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/task"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type functionGroup struct {
	routerGroup *echo.Group
	fs          *RunCFunctionService
}

type functionPayload struct {
	Args   []interface{} `json:"args"`
	Kwargs []interface{} `json:"kwargs"`
}

func registerFunctionRoutes(g *echo.Group, fs *RunCFunctionService) *functionGroup {
	group := &functionGroup{routerGroup: g, fs: fs}

	g.POST("/id/:stubId", group.FunctionInvoke)
	g.POST("/:deploymentName/v:version", group.FunctionInvoke)

	return group
}

func (g *functionGroup) FunctionInvoke(ctx echo.Context) error {
	/*
		 TODO: support three different unmarshalling strategies
		 	- explicit args/kwargs (nested under {"args", "kwargs"})
			- just kwargs (key/value)
			- just args (in list)
	*/
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

		deployment, err := g.fs.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeFunctionDeployment)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": "invalid deployment",
			})
		}

		stubId = deployment.Stub.ExternalId
	}

	payload, err := task.SerializeHttpPayload(ctx)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request payload",
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
