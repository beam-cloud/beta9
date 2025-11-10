package function

import (
	"net/http"
	"strings"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/labstack/echo/v4"
)

type functionGroup struct {
	routerGroup *echo.Group
	fs          *ContainerFunctionService
	cache       *expirable.LRU[string, string]
}

func registerFunctionRoutes(g *echo.Group, fs *ContainerFunctionService) *functionGroup {
	group := &functionGroup{routerGroup: g, fs: fs, cache: abstractions.NewDeploymentStubCache()}

	g.POST("/id/:stubId", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.FunctionInvoke))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.FunctionInvoke))

	return group
}

func (g *functionGroup) FunctionInvoke(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubType := types.StubTypeFunctionDeployment
	if strings.HasPrefix(ctx.Path(), scheduleRoutePrefix) {
		stubType = types.StubTypeScheduledJobDeployment
	}

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		g.cache,
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		stubType,
		g.fs.backendRepo,
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
