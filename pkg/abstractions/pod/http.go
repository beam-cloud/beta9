package pod

import (
	"net/http"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/labstack/echo/v4"
)

type podGroup struct {
	routeGroup *echo.Group
	ps         *GenericPodService
	cache      *expirable.LRU[string, string]
}

func registerPodGroup(g *echo.Group, ps *GenericPodService) *podGroup {
	group := &podGroup{routeGroup: g, ps: ps, cache: abstractions.NewDeploymentStubCache()}

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
	g.Any("/container/:stubId/:containerId/:port", auth.WithAssumedStubAuth(group.SandboxContainerRequest, group.ps.IsPublic))
	g.Any("/container/:stubId/:containerId/:port/:subPath", auth.WithAssumedStubAuth(group.SandboxContainerRequest, group.ps.IsPublic))
	g.POST("/run/:stubId", auth.WithAuth(group.PodRun))

	return group
}

func (g *podGroup) PodRequest(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId, err := abstractions.ParseAndValidateDeploymentStubId(
		ctx.Request().Context(),
		g.cache,
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

func (g *podGroup) SandboxContainerRequest(ctx echo.Context) error {
	stubId := ctx.Param("stubId")
	containerId := ctx.Param("containerId")

	containerStubId, err := common.ExtractStubIdFromContainerId(containerId)
	if err != nil || containerStubId != stubId {
		return apiv1.HTTPBadRequest("Invalid sandbox container")
	}

	return g.ps.forwardContainerRequest(ctx, stubId, containerId)
}

func (g *podGroup) PodRun(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	stubId := ctx.Param("stubId")

	stub, err := g.ps.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubId)
	if err != nil {
		return apiv1.HTTPInternalServerError("Failed to get stub")
	}

	if stub.WorkspaceId != cc.AuthInfo.Workspace.Id {
		return apiv1.HTTPNotFound()
	}

	if g.ps.trackRunAsTask(stub) {
		podTask, err := g.ps.taskDispatcher.SendAndExecute(ctx.Request().Context(), string(types.ExecutorContainer), cc.AuthInfo, stub.ExternalId, &types.TaskPayload{}, podRunTaskPolicy(), cc.AuthInfo, stub, runOptions{})
		if err != nil {
			return err
		}

		metadata := podTask.Metadata()
		return ctx.JSON(http.StatusOK, map[string]string{
			"container_id": metadata.ContainerId,
			"task_id":      metadata.TaskId,
		})
	}

	containerId, err := g.ps.run(
		ctx.Request().Context(),
		cc.AuthInfo,
		stub,
		runOptions{},
	)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, map[string]string{
		"container_id": containerId,
	})
}
