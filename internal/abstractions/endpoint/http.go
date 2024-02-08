package endpoint

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type endpointGroup struct {
	routerGroup *echo.Group
	es          *RingBufferEndpointService
}

func registerEndpointRoutes(g *echo.Group, es *RingBufferEndpointService) *endpointGroup {
	group := &endpointGroup{routerGroup: g, es: es}

	g.GET("/id/:stubId/", group.endpointRequest)
	g.POST("/:deploymentName/v:version", group.endpointRequest)

	return group
}

func (g *endpointGroup) endpointRequest(ctx echo.Context) error {
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

		deployment, err := g.es.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeEndpointDeployment)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"error": "invalid deployment",
			})
		}

		stubId = deployment.Stub.ExternalId
	}

	var payload interface{}

	if err := ctx.Bind(&payload); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request payload",
		})
	}

	resp, err := g.es.forwardRequest(ctx.Request().Context(), stubId, ctx.Request().Method, ctx.Request().Header, ctx.Request().Body)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.Blob(http.StatusOK, "application/json", resp)
}
