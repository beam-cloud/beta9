package endpoint

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type endpointGroup struct {
	routeGroup *echo.Group
	es         *HttpEndpointService
}

func registerEndpointRoutes(g *echo.Group, es *HttpEndpointService) *endpointGroup {
	group := &endpointGroup{routeGroup: g, es: es}

	g.POST("/id/:stubId", group.endpointRequest)
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

	return g.es.forwardRequest(ctx, cc.AuthInfo, stubId)
}
