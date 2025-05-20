package endpoint

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
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

	g.POST("/id/:stubId", auth.WithAuth(group.EndpointRequest))
	g.POST("/:deploymentName", auth.WithAuth(group.EndpointRequest))
	g.POST("/:deploymentName/latest", auth.WithAuth(group.EndpointRequest))
	g.POST("/:deploymentName/v:version", auth.WithAuth(group.EndpointRequest))
	g.POST("/public/:stubId", auth.WithAssumedStubAuth(group.EndpointRequest, group.es.IsPublic))
	g.POST("/metered/:stubId", auth.WithMeteredAuth(group.EndpointRequest, group.es.IsMetered))

	g.GET("/id/:stubId", auth.WithAuth(group.EndpointRequest))
	g.GET("/:deploymentName", auth.WithAuth(group.EndpointRequest))
	g.GET("/:deploymentName/latest", auth.WithAuth(group.EndpointRequest))
	g.GET("/:deploymentName/v:version", auth.WithAuth(group.EndpointRequest))
	g.GET("/public/:stubId", auth.WithAssumedStubAuth(group.EndpointRequest, group.es.IsPublic))
	g.GET("/metered/:stubId", auth.WithMeteredAuth(group.EndpointRequest, group.es.IsMetered))

	g.POST("/id/:stubId/warmup", auth.WithAuth(group.WarmUpEndpoint))
	g.POST("/:deploymentName/warmup", auth.WithAuth(group.WarmUpEndpoint))
	g.POST("/:deploymentName/latest/warmup", auth.WithAuth(group.WarmUpEndpoint))
	g.POST("/:deploymentName/v:version/warmup", auth.WithAuth(group.WarmUpEndpoint))

	return group
}

func registerASGIRoutes(g *echo.Group, es *HttpEndpointService) *endpointGroup {
	group := &endpointGroup{routeGroup: g, es: es}

	g.Any("/id/:stubId", auth.WithAuth(group.ASGIRequest))
	g.Any("/id/:stubId/:subPath", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName/:subPath", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName/latest", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName/latest/:subPath", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName/v:version", auth.WithAuth(group.ASGIRequest))
	g.Any("/:deploymentName/v:version/:subPath", auth.WithAuth(group.ASGIRequest))
	g.Any("/public/:stubId", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.IsPublic))
	g.Any("/public/:stubId/:subPath", auth.WithAssumedStubAuth(group.ASGIRequest, group.es.IsPublic))

	g.POST("/id/:stubId/warmup", auth.WithAuth(group.WarmupASGI))
	g.POST("/:deploymentName/warmup", auth.WithAuth(group.WarmupASGI))
	g.POST("/:deploymentName/latest/warmup", auth.WithAuth(group.WarmupASGI))
	g.POST("/:deploymentName/v:version/warmup", auth.WithAuth(group.WarmupASGI))

	return group
}

func (g *endpointGroup) EndpointRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		types.StubTypeEndpointDeployment,
		g.es.backendRepo,
	)
	if err != nil {
		return err
	}

	return g.es.forwardRequest(ctx, cc.AuthInfo, stubId)
}

func (g *endpointGroup) ASGIRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		types.StubTypeASGIDeployment,
		g.es.backendRepo,
	)
	if err != nil {
		return err
	}

	return g.es.forwardRequest(ctx, cc.AuthInfo, stubId)
}

func (g *endpointGroup) WarmUpEndpoint(ctx echo.Context) error {
	return g.warmup(ctx, types.StubTypeEndpointDeployment)
}

func (g *endpointGroup) WarmupASGI(ctx echo.Context) error {
	return g.warmup(ctx, types.StubTypeASGIDeployment)
}

func (g *endpointGroup) warmup(
	ctx echo.Context,
	deploymentType string,
) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		deploymentType,
		g.es.backendRepo,
	)
	if err != nil {
		return err
	}

	return g.es.controller.Warmup(
		ctx.Request().Context(),
		stubId,
	)
}
