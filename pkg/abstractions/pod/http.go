package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type podGroup struct {
	routeGroup *echo.Group
	ps         *GenericPodService
}

func registerPodGroup(g *echo.Group, ps *GenericPodService) *podGroup {
	group := &podGroup{routeGroup: g, ps: ps}

	g.Any("/id/:stubId/:port", auth.WithAuth(group.PodRequest))
	g.Any("/id/:stubId/:port/:subPath", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/:port", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/:port/:subPath", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/latest/:port", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/latest/:port/:subPath", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/v:version/:port", auth.WithAuth(group.PodRequest))
	g.Any("/:deploymentName/v:version/:port/:subPath", auth.WithAuth(group.PodRequest))
	g.Any("/public/:stubId/:port", auth.WithAssumedStubAuth(group.PodRequest, group.ps.IsPublic))
	g.Any("/public/:stubId/:port/:subPath", auth.WithAssumedStubAuth(group.PodRequest, group.ps.IsPublic))

	return group
}

func (g *podGroup) PodRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		cc.AuthInfo,
		ctx.Param("stubId"),
		ctx.Param("deploymentName"),
		ctx.Param("version"),
		types.StubTypePodDeployment,
		g.ps.backendRepo,
	)
	if err != nil {
		return err
	}

	return g.ps.forwardRequest(ctx, stubId)
}
