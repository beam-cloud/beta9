package apiv1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	g.GET("/:stubId/url", auth.WithAuth(group.GetURL))                                     // Allows users to get the URL of a stub

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

	serializedStub, err := serializer.Serialize(stub)
	if err != nil {
		return HTTPInternalServerError("Failed to serialize stub")
	}

	return ctx.JSON(http.StatusOK, serializedStub)
}

func (g *StubGroup) GetURL(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := cc.AuthInfo

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

	stubConfig := &types.StubConfigV1{}
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to decode stub config")
	}

	// Allow public stubs to be accessed by any workspace
	workspaceId := stub.WorkspaceId
	if stubConfig.Pricing != nil {
		filter.WorkspaceId = stub.Workspace.ExternalId
		workspaceId = stub.Workspace.Id
	} else if stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return HTTPNotFound()
	}

	// Get URL for Serves, Pods, and public stubs
	if stub.Type.IsServe() || stub.Type.Kind() == types.StubTypeShell || stubConfig.Pricing != nil {
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

	deployment, err := g.backendRepo.GetDeploymentByStubExternalId(ctx.Request().Context(), workspaceId, stub.ExternalId)
	if err != nil {
		return HTTPInternalServerError("Failed to lookup deployment")
	}

	if deployment == nil {
		return HTTPNotFound()
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
		stubConfig.Runtime.Memory = int64(*overrideConfig.Memory)
	}

	valid, errorMsg := types.ValidateCpuAndMemory(stubConfig.Runtime.Cpu, stubConfig.Runtime.Memory, g.config.GatewayService.StubLimits)
	if !valid {
		return HTTPBadRequest(errorMsg)
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

func (g *StubGroup) copyObjectContents(ctx context.Context, destinationWorkspace *types.Workspace, stub *types.StubWithRelated) (uint, error) {
	sourceWorkspace, err := g.backendRepo.GetWorkspace(ctx, stub.Workspace.Id)
	if err != nil {
		return 0, err
	}

	sourceObject, err := g.backendRepo.GetObjectByExternalStubId(ctx, stub.ExternalId, stub.WorkspaceId)
	if err != nil {
		return 0, err
	}

	sourceObjectVolumePath := path.Join(types.DefaultObjectPath, stub.Workspace.Name)
	sourceObjectVolumeFilePath := path.Join(sourceObjectVolumePath, sourceObject.ExternalId)
	sourceObjectStorageFilePath := path.Join(types.DefaultObjectPrefix, sourceObject.ExternalId)

	// Check if the object already exists in the child workspace
	if existingObject, err := g.backendRepo.GetObjectByHash(ctx, sourceObject.Hash, destinationWorkspace.Id); err == nil {
		return existingObject.Id, nil
	}

	success := false
	newObject, err := g.backendRepo.CreateObject(ctx, sourceObject.Hash, sourceObject.Size, destinationWorkspace.Id)
	if err != nil {
		return 0, err
	}

	defer func() {
		if !success {
			g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		}
	}()

	newObjectVolumePath := path.Join(types.DefaultObjectPath, destinationWorkspace.Name)
	newObjectVolumeFilePath := path.Join(newObjectVolumePath, newObject.ExternalId)
	newObjectStorageFilePath := path.Join(types.DefaultObjectPrefix, newObject.ExternalId)

	// If both workspaces have the storage client available and both are pointed to same storage provider, copy the object with the storage client
	if sourceWorkspace.StorageAvailable() && destinationWorkspace.StorageAvailable() && sourceWorkspace.Storage.EndpointUrl == destinationWorkspace.Storage.EndpointUrl {
		storageClient, err := clients.NewDefaultStorageClient(ctx, g.config)
		if err != nil {
			return 0, err
		}

		err = storageClient.CopyObject(ctx, clients.CopyObjectInput{
			SourceKey:             sourceObjectStorageFilePath,
			SourceBucketName:      *sourceWorkspace.Storage.BucketName,
			DestinationKey:        newObjectStorageFilePath,
			DestinationBucketName: *destinationWorkspace.Storage.BucketName,
		})
		if err != nil {
			return 0, err
		}

		success = true
		return newObject.Id, nil
	}

	var input io.Reader
	// If the parent workspace has the storage client available, download the object with the storage client
	if sourceWorkspace.StorageAvailable() {
		sourceStorageClient, err := clients.NewWorkspaceStorageClient(ctx, sourceWorkspace.Name, sourceWorkspace.Storage)
		if err != nil {
			return 0, err
		}

		_input, err := sourceStorageClient.DownloadWithReader(ctx, sourceObjectStorageFilePath)
		if err != nil {
			return 0, err
		}

		defer _input.Close()
		input = _input
	} else {
		// If the parent workspace does not have the storage client available, read the object from the volume mount
		_input, err := os.ReadFile(sourceObjectVolumeFilePath)
		if err != nil {
			g.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return 0, err
		}

		input = bytes.NewReader(_input)
	}

	// If the child workspace has the storage client available, upload the object with the storage client
	if destinationWorkspace.StorageAvailable() {
		destinationStorageClient, err := clients.NewWorkspaceStorageClient(ctx, destinationWorkspace.Name, destinationWorkspace.Storage)
		if err != nil {
			return 0, err
		}

		err = destinationStorageClient.UploadWithReader(ctx, newObjectStorageFilePath, input)
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

	var file *os.File
	if _, err := os.Stat(newObjectVolumeFilePath); err == nil {
		file, err = os.OpenFile(newObjectVolumeFilePath, os.O_WRONLY, 0644)
		if err != nil {
			return 0, err
		}
	} else if os.IsNotExist(err) {
		file, err = os.Create(newObjectVolumeFilePath)
		if err != nil {
			return 0, err
		}
	} else {
		return 0, err
	}

	defer file.Close()

	// If the child workspace does not have the storage client available, write the object to the volume mount
	_, err = io.Copy(file, input)
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
