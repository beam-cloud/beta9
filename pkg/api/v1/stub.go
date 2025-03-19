package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type StubGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewStubGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *StubGroup {
	group := &StubGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListStubsByWorkspaceId))           // Allows workspace admins to list stubs specific to their workspace
	g.GET("/:workspaceId/:stubId", auth.WithWorkspaceAuth(group.RetrieveStub))             // Allows workspace admins to retrieve a specific stub
	g.GET("", auth.WithClusterAdminAuth(group.ListStubs))                                  // Allows cluster admins to list all stubs
	g.GET("/:workspaceId/:stubId/url", auth.WithWorkspaceAuth(group.GetURL))               // Allows workspace admins to get the URL of a stub
	g.GET("/:workspaceId/:stubId/url/:deploymentId", auth.WithWorkspaceAuth(group.GetURL)) // Allows workspace admins to get the URL of a stub by deployment Id
	g.POST("/:stubId/clone", auth.WithAuth(group.CloneStubPublic))                         // Allows users to clone a public stub

	return group
}

func (g *StubGroup) ListStubsByWorkspaceId(ctx echo.Context) error {
	workspaceID := ctx.Param("workspaceId")

	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspaceID

	if filters.Pagination {
		if stubs, err := g.backendRepo.ListStubsPaginated(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			return ctx.JSON(http.StatusOK, stubs)
		}
	} else {
		if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			return ctx.JSON(http.StatusOK, stubs)
		}
	}
}

func (g *StubGroup) ListStubs(ctx echo.Context) error {
	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	if filters.Pagination {
		if stubs, err := g.backendRepo.ListStubsPaginated(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			return ctx.JSON(http.StatusOK, stubs)
		}
	} else {
		if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			return ctx.JSON(http.StatusOK, stubs)
		}
	}
}

func (g *StubGroup) RetrieveStub(ctx echo.Context) error {
	stubID := ctx.Param("stubId")
	workspaceID := ctx.Param("workspaceId")

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID, types.QueryFilter{
		Field: "workspace_id",
		Value: workspaceID,
	})
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return HTTPNotFound()
	}

	return ctx.JSON(http.StatusOK, stub)

}

func (g *StubGroup) GetURL(ctx echo.Context) error {
	filter := &types.StubGetURLFilter{}
	if err := ctx.Bind(filter); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	if filter.URLType == "" {
		filter.URLType = g.config.GatewayService.InvokeURLType
	}

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), filter.StubId)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup stub")
	}
	if stub == nil {
		return HTTPBadRequest("Invalid stub ID")
	}

	// Get URL for Serves
	if stub.Type.IsServe() || stub.Type.Kind() == types.StubTypeShell {
		invokeUrl := common.BuildStubURL(g.config.GatewayService.HTTP.GetExternalURL(), filter.URLType, stub)
		return ctx.JSON(http.StatusOK, map[string]string{"url": invokeUrl})
	} else if stub.Type.Kind() == types.StubTypePod {
		stubConfig := &types.StubConfigV1{}
		if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
			return HTTPInternalServerError("Failed to decode stub config")
		}

		invokeUrl := common.BuildPodURL(g.config.GatewayService.HTTP.GetExternalURL(), filter.URLType, stub, stubConfig)
		return ctx.JSON(http.StatusOK, map[string]string{"url": invokeUrl})
	}

	// Get URL for Deployments
	if filter.DeploymentId == "" {
		return HTTPBadRequest("Deployment ID is required")
	}

	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), filter.WorkspaceId)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup workspace")
	}

	deployment, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), workspace.Id, filter.DeploymentId)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup deployment")
	}

	invokeUrl := common.BuildDeploymentURL(g.config.GatewayService.HTTP.GetExternalURL(), filter.URLType, stub, &deployment.Deployment)
	return ctx.JSON(http.StatusOK, map[string]string{"url": invokeUrl})
}

func (g *StubGroup) CloneStubPublic(ctx echo.Context) error {
	stubID := ctx.Param("stubId")
	cc, _ := ctx.(*auth.HttpAuthContext)

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup stub")
	}

	if stub == nil || (!stub.Public && cc.AuthInfo.Workspace.Id != stub.WorkspaceId) {
		return HTTPBadRequest("Invalid stub ID")
	}

	newStub, err := g.cloneStub(ctx.Request().Context(), cc.AuthInfo.Workspace, stub)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, newStub)
}

func (g StubGroup) configureVolumes(ctx context.Context, volumes []*pb.Volume, workspace *types.Workspace) error {
	for i, volume := range volumes {
		if volume.Config != nil {
			// De-reference secrets
			accessKey, err := g.backendRepo.GetSecretByName(ctx, workspace, volume.Config.AccessKey)
			if err != nil {
				return fmt.Errorf("Failed to get secret: %s", volume.Config.AccessKey)
			}
			volumes[i].Config.AccessKey = accessKey.Value

			secretKey, err := g.backendRepo.GetSecretByName(ctx, workspace, volume.Config.SecretKey)
			if err != nil {
				return fmt.Errorf("Failed to get secret: %s", volume.Config.SecretKey)
			}
			volumes[i].Config.SecretKey = secretKey.Value
		}
	}

	return nil
}

func (g *StubGroup) copyObjectContents(ctx context.Context, workspace *types.Workspace, stub *types.StubWithRelated) (uint, error) {
	parentObject, err := g.backendRepo.GetObjectByExternalStubId(ctx, stub.ExternalId, stub.WorkspaceId)
	if err != nil {
		return 0, err
	}

	parentObjectPath := path.Join(types.DefaultObjectPath, stub.Workspace.Name)
	parentObjectFilePath := path.Join(parentObjectPath, parentObject.ExternalId)

	if existingObject, err := g.backendRepo.GetObjectByHash(ctx, parentObject.Hash, workspace.Id); err == nil {
		return existingObject.Id, nil
	}

	newObject, err := g.backendRepo.CreateObject(ctx, parentObject.Hash, parentObject.Size, workspace.Id)
	if err != nil {
		return 0, err
	}

	newObjectPath := path.Join(types.DefaultObjectPath, workspace.Name)
	newObjectFilePath := path.Join(newObjectPath, newObject.ExternalId)

	input, err := os.ReadFile(parentObjectFilePath)
	if err != nil {
		g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return 0, err
	}

	err = os.WriteFile(newObjectFilePath, input, 0644)
	if err != nil {
		g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return 0, err
	}

	return newObject.Id, nil
}

func (g *StubGroup) cloneStub(ctx context.Context, workspace *types.Workspace, stub *types.StubWithRelated) (*types.Stub, error) {
	objectId, err := g.copyObjectContents(ctx, workspace, stub)
	if err != nil {
		return nil, HTTPBadRequest("Failed to clone object")
	}

	stubConfig := &types.StubConfigV1{}
	if err = json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return nil, HTTPInternalServerError("Failed to decode stub config")
	}

	parentSecrets := stubConfig.Secrets
	stubConfig.Secrets = []types.Secret{}

	if stubConfig.RequiresGPU() {
		concurrencyLimit, err := g.backendRepo.GetConcurrencyLimitByWorkspaceId(ctx, workspace.ExternalId)
		if err != nil && concurrencyLimit != nil && concurrencyLimit.GPULimit <= 0 {
			return nil, HTTPBadRequest("GPU concurrency limit is 0.")
		}
	}

	for _, secret := range parentSecrets {
		secret, err := g.backendRepo.GetSecretByName(ctx, workspace, secret.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}

			return nil, HTTPInternalServerError("Failed to lookup secret")
		}

		stubConfig.Secrets = append(stubConfig.Secrets, types.Secret{
			Name:      secret.Name,
			Value:     secret.Value,
			CreatedAt: secret.CreatedAt,
			UpdatedAt: secret.UpdatedAt,
		})
	}

	err = g.configureVolumes(ctx, stubConfig.Volumes, workspace)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to configure volumes")
	}

	newStub, err := g.backendRepo.GetOrCreateStub(ctx, stub.Name, string(stub.Type), *stubConfig, objectId, workspace.Id, true)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to clone stub")
	}

	return &newStub, nil
}
