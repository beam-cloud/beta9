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
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type StubGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
	eventRepo   repository.EventRepository
}

func NewStubGroup(g *echo.Group, backendRepo repository.BackendRepository, eventRepo repository.EventRepository, config types.AppConfig) *StubGroup {
	group := &StubGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
		eventRepo:   eventRepo,
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
			serializedStub, err := serializer.Serialize(stubs)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize stubs")
			}

			return ctx.JSON(http.StatusOK, serializedStub)
		}
	} else {
		if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			serializedStub, err := serializer.Serialize(stubs)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize stubs")
			}

			return ctx.JSON(http.StatusOK, serializedStub)
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
			serializedStub, err := serializer.Serialize(stubs)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize stubs")
			}

			return ctx.JSON(http.StatusOK, serializedStub)
		}
	} else {
		if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
			return HTTPInternalServerError("Failed to list stubs")
		} else {
			serializedStub, err := serializer.Serialize(stubs)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize stubs")
			}

			return ctx.JSON(http.StatusOK, serializedStub)
		}
	}
}

func (g *StubGroup) RetrieveStub(ctx echo.Context) error {
	stubID := ctx.Param("stubId")

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return HTTPNotFound()
	}

	if !stub.Public {
		cc, _ := ctx.(*auth.HttpAuthContext)
		if cc.AuthInfo.Workspace.Id != stub.WorkspaceId {
			return HTTPNotFound()
		}
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

type OverrideStubConfig struct {
	Cpu      *int64  `json:"cpu"`
	Memory   *int64  `json:"memory"`
	Gpu      *string `json:"gpu"`
	GpuCount *uint32 `json:"gpu_count"`
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

	var overrideConfig OverrideStubConfig
	if err := ctx.Bind(&overrideConfig); err != nil {
		return HTTPBadRequest("Failed to process overrides")
	}

	if err := g.processStubOverrides(overrideConfig, stub); err != nil {
		return err
	}

	newStub, err := g.cloneStub(ctx.Request().Context(), cc.AuthInfo.Workspace, stub)
	if err != nil {
		return err
	}

	serializedStub, err := serializer.Serialize(newStub)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, serializedStub)
}

func (g StubGroup) processStubOverrides(overrideConfig OverrideStubConfig, stub *types.StubWithRelated) error {
	var stubConfig types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return HTTPBadRequest("Failed to process overrides")
	}

	if overrideConfig.Cpu != nil {
		stubConfig.Runtime.Cpu = int64(*overrideConfig.Cpu)
	}

	if overrideConfig.Memory != nil {
		if *overrideConfig.Memory > int64(g.config.GatewayService.StubLimits.Memory) {
			return HTTPBadRequest(fmt.Sprintf("Memory must be %dGiB or less.", g.config.GatewayService.StubLimits.Memory/1024))
		}
		stubConfig.Runtime.Memory = int64(*overrideConfig.Memory)
	}

	if overrideConfig.Gpu != nil {
		if _, ok := types.GPUTypesToMap(types.AllGPUTypes())[*overrideConfig.Gpu]; ok {
			stubConfig.Runtime.Gpus = []types.GpuType{types.GpuType(*overrideConfig.Gpu)}
		} else {

			return HTTPBadRequest("Invalid GPU type")
		}
	}

	if overrideConfig.GpuCount != nil {
		if *overrideConfig.GpuCount > g.config.GatewayService.StubLimits.MaxGpuCount {
			return HTTPBadRequest(fmt.Sprintf("GPU count must be %d or less.", g.config.GatewayService.StubLimits.MaxGpuCount))
		}
		stubConfig.Runtime.GpuCount = uint32(*overrideConfig.GpuCount)
	}

	stubConfigBytes, err := json.Marshal(stubConfig)
	if err != nil {
		return HTTPBadRequest("Failed to process overrides")
	}

	stub.Config = string(stubConfigBytes)
	return nil
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

func (g *StubGroup) copyObjectContents(ctx context.Context, childWorkspace *types.Workspace, stub *types.StubWithRelated) (uint, error) {
	parentWorkspace, err := g.backendRepo.GetWorkspaceWithRelated(ctx, stub.Workspace.Id)
	if err != nil {
		return 0, err
	}

	parentObject, err := g.backendRepo.GetObjectByExternalStubId(ctx, stub.ExternalId, stub.WorkspaceId)
	if err != nil {
		return 0, err
	}

	parentObjectVolumePath := path.Join(types.DefaultObjectPath, stub.Workspace.Name)
	parentObjectVolumeFilePath := path.Join(parentObjectVolumePath, parentObject.ExternalId)
	parentObjectStorageFilePath := path.Join(types.DefaultObjectPrefix, parentObject.ExternalId)

	// Check if the object already exists in the child workspace
	if existingObject, err := g.backendRepo.GetObjectByHash(ctx, parentObject.Hash, childWorkspace.Id); err == nil {
		return existingObject.Id, nil
	}

	success := false
	newObject, err := g.backendRepo.CreateObject(ctx, parentObject.Hash, parentObject.Size, childWorkspace.Id)
	if err != nil {
		return 0, err
	}

	defer func() {
		if !success {
			g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		}
	}()

	newObjectVolumePath := path.Join(types.DefaultObjectPath, childWorkspace.Name)
	newObjectVolumeFilePath := path.Join(newObjectVolumePath, newObject.ExternalId)
	newObjectStorageFilePath := path.Join(types.DefaultObjectPrefix, newObject.ExternalId)

	// If both workspaces have the storage client available, copy the object with the storage client
	if parentWorkspace.StorageAvailable() && childWorkspace.StorageAvailable() {
		storageClient, err := clients.NewDefaultStorageClient(ctx, g.config)
		if err != nil {
			return 0, err
		}

		err = storageClient.CopyObject(ctx, clients.CopyObjectInput{
			SourceKey:             parentObjectStorageFilePath,
			SourceBucketName:      *parentWorkspace.Storage.BucketName,
			DestinationKey:        newObjectStorageFilePath,
			DestinationBucketName: *childWorkspace.Storage.BucketName,
		})
		if err != nil {
			return 0, err
		}

		success = true
		return newObject.Id, nil
	}

	var input []byte
	// If the parent workspace has the storage client available, download the object with the storage client
	if parentWorkspace.StorageAvailable() {
		parentStorageClient, err := clients.NewWorkspaceStorageClient(ctx, parentWorkspace.Name, parentWorkspace.Storage)
		if err != nil {
			return 0, err
		}

		input, err = parentStorageClient.Download(ctx, parentObjectStorageFilePath)
		if err != nil {
			return 0, err
		}
	} else {
		// If the parent workspace does not have the storage client available, read the object from the volume mount
		input, err = os.ReadFile(parentObjectVolumeFilePath)
		if err != nil {
			g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return 0, err
		}
	}

	// If the child workspace has the storage client available, upload the object with the storage client
	if childWorkspace.StorageAvailable() {
		childStorageClient, err := clients.NewWorkspaceStorageClient(ctx, childWorkspace.Name, childWorkspace.Storage)
		if err != nil {
			return 0, err
		}

		err = childStorageClient.Upload(ctx, newObjectStorageFilePath, input)
		if err != nil {
			return 0, err
		}

		success = true
		return newObject.Id, nil
	}

	if _, err := os.Stat(newObjectVolumePath); os.IsNotExist(err) {
		if err := os.MkdirAll(newObjectVolumePath, 0755); err != nil {
			return 0, err
		}
	}

	// If the child workspace does not have the storage client available, write the object to the volume mount
	err = os.WriteFile(newObjectVolumeFilePath, input, 0644)
	if err != nil {
		g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return 0, err
	}

	success = true
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

	if stubConfig.Runtime.GpuCount > 1 && !workspace.MultiGpuEnabled {
		return nil, HTTPBadRequest("Multi-GPU containers are not enabled for this workspace.")
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

	app, err := g.backendRepo.GetOrCreateApp(ctx, workspace.Id, stub.Name)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to create app")
	}

	newStub, err := g.backendRepo.GetOrCreateStub(ctx, stub.Name, string(stub.Type), *stubConfig, objectId, workspace.Id, true, app.Id)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to clone stub")
	}

	go g.eventRepo.PushCloneStubEvent(workspace.ExternalId, &newStub, &stub.Stub)

	return &newStub, nil
}
