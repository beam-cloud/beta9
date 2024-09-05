package endpoint

import (
	"strconv"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type endpointGroup struct {
	routeGroup *echo.Group
	es         *HttpEndpointService
}

func registerEndpointRoutes(g *echo.Group, es *HttpEndpointService) *endpointGroup {
	group := &endpointGroup{routeGroup: g, es: es}

	g.POST("/id/:stubId", auth.WithAuth(group.endpointRequest))
	g.POST("/:deploymentName", auth.WithAuth(group.endpointRequest))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.endpointRequest))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.endpointRequest))
	g.POST("/public/:stubId", auth.WithAssumedStubAuth(group.endpointRequest, group.es.isPublic))
	g.GET("/id/:stubId", auth.WithAuth(group.endpointRequest))

	g.GET("/:deploymentName", auth.WithAuth(group.endpointRequest))
	g.GET("/:deploymentName/latest", auth.WithAuth(group.endpointRequest))
	g.GET("/:deploymentName/v:version", auth.WithAuth(group.endpointRequest))
	g.GET("/public/:stubId", auth.WithAssumedStubAuth(group.endpointRequest, group.es.isPublic))

	return group
}

func registerASGIRoutes(g *echo.Group, es *HttpEndpointService) *endpointGroup {
	group := &endpointGroup{routeGroup: g, es: es}

	g.POST("/id/:stubId", auth.WithAuth(group.ASGIRequest))
	g.POST("/id/:stubId/:subPath", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName/:subPath", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName/latest/:subPath", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.ASGIRequest))
	g.POST("/:deploymentName/v:version/:subPath", auth.WithAuth(group.ASGIRequest))
	g.POST("/public/:stubId", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.isPublic))
	g.POST("/public/:stubId/:subPath", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.isPublic))

	g.GET("/id/:stubId", auth.WithAuth(group.ASGIRequest))
	g.GET("/id/:stubId/:subPath", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName/:subPath", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName/latest", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName/latest/:subPath", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName/v:version", auth.WithAuth(group.ASGIRequest))
	g.GET("/:deploymentName/v:version/:subPath", auth.WithAuth(group.ASGIRequest))
	g.GET("/public/:stubId", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.isPublic))
	g.GET("/public/:stubId/:subPath", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.isPublic))

	return group
}

func (g *endpointGroup) endpointRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.es.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, types.StubTypeEndpointDeployment, true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.es.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeEndpointDeployment)
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

	return g.es.forwardRequest(ctx, cc.AuthInfo, stubId)
}

func (g *endpointGroup) ASGIRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.es.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, types.StubTypeASGIDeployment, true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.es.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), types.StubTypeASGIDeployment)
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

	return g.es.forwardRequest(ctx, cc.AuthInfo, stubId)
}
