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
	"reflect"
	"sort"
	"strings"
	"time"

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
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	eventRepo     repository.EventRepository
}

func NewStubGroup(g *echo.Group, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, eventRepo repository.EventRepository, config types.AppConfig) *StubGroup {
	group := &StubGroup{routerGroup: g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		config:        config,
		eventRepo:     eventRepo,
	}

	g.GET("/:workspaceId/sandboxes", auth.WithWorkspaceAuth(group.ListSandboxes))             // Lists sandboxes (enriched with live container state) for a workspace
	g.GET("/:workspaceId/sandboxes/stats", auth.WithWorkspaceAuth(group.GetSandboxStats))     // Aggregated sandbox stats for the sandboxes dashboard
	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListStubsByWorkspaceId))              // Allows workspace admins to list stubs specific to their workspace
	g.GET("/:workspaceId/:stubId", auth.WithWorkspaceAuth(group.RetrieveStub))                // Allows workspace admins to retrieve a specific stub
	g.GET("", auth.WithClusterAdminAuth(group.ListStubs))                                     // Allows cluster admins to list all stubs
	g.GET("/:workspaceId/:stubId/url", auth.WithWorkspaceAuth(group.GetURL))                  // Allows workspace admins to get the URL of a stub
	g.GET("/:workspaceId/:stubId/url/:deploymentId", auth.WithWorkspaceAuth(group.GetURL))    // Allows workspace admins to get the URL of a stub by deployment Id
	g.PATCH("/:workspaceId/:stubId/config", auth.WithStrictWorkspaceAuth(group.UpdateConfig)) // Allows workspace admins to update the config of a stub
	g.POST("/:stubId/clone", auth.WithAuth(group.CloneStubPublic))                            // Allows users to clone a public stub
	g.GET("/:stubId/url", auth.WithAuth(group.GetURL))                                        // Allows users to get the URL of a stub
	g.GET("/:stubId/config", group.GetConfig)                                                 // Allows users to get the config of a stub

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
	} else if stub.Type.Kind() == types.StubTypePod || stub.Type.Kind() == types.StubTypeSandbox {
		stubConfig := &types.StubConfigV1{}
		if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
			return HTTPInternalServerError("Failed to decode stub config")
		}

		externalUrl := g.config.GatewayService.HTTP.GetExternalURL()

		if stubConfig.TCP {
			externalUrl = g.config.Abstractions.Pod.TCP.GetExternalURL()
			filter.URLType = common.InvokeUrlTypeHost
		}

		invokeUrl := common.BuildPodURL(externalUrl, filter.URLType, stub, stubConfig)
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
		gpu := types.NormalizeGPUType(*overrideConfig.Gpu)
		if _, ok := types.GPUTypesToMap(types.AllGPUTypes())[string(gpu)]; ok {
			stubConfig.Runtime.Gpus = []types.GpuType{gpu}
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

	for _, volumeConfig := range stubConfig.Volumes {
		parentVolume, err := g.backendRepo.GetVolumeByExternalId(ctx, stub.WorkspaceId, volumeConfig.Id)
		if err != nil {
			return nil, err
		}

		childVolume, err := g.backendRepo.GetOrCreateVolume(ctx, workspace.Id, parentVolume.Name)
		if err != nil {
			return nil, err
		}

		volumeConfig.Id = childVolume.ExternalId
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

func (g *StubGroup) GetConfig(ctx echo.Context) error {
	stubID := ctx.Param("stubId")
	cc, _ := ctx.(*auth.HttpAuthContext)

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return HTTPNotFound()
	}

	err = stub.Stub.SanitizeConfig()
	if err != nil {
		return HTTPInternalServerError("Failed to sanitize stub config")
	}

	stubConfig := &types.StubConfigV1{}
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to decode stub config")
	}

	// If there is no pricing policy, only allow access to the config if the user is the owner of the stub
	if stubConfig.Pricing == nil && cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace.Id != stub.WorkspaceId {
		return HTTPNotFound()
	}

	limitedConfig := &types.StubConfigLimitedValues{
		Pricing:       stubConfig.Pricing,
		Inputs:        stubConfig.Inputs,
		Outputs:       stubConfig.Outputs,
		TaskPolicy:    stubConfig.TaskPolicy,
		PythonVersion: stubConfig.PythonVersion,
		Runtime:       stubConfig.Runtime,
	}

	return ctx.JSON(http.StatusOK, limitedConfig)
}

type UpdateConfigRequest struct {
	Fields map[string]interface{} `json:"fields"` // Map of field paths to values (e.g., {"runtime.cpu": 4, "runtime.memory": 8192})
}

func (g *StubGroup) UpdateConfig(ctx echo.Context) error {
	stubID := ctx.Param("stubId")

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return HTTPNotFound()
	}

	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Workspace.Id != stub.WorkspaceId {
		return HTTPNotFound()
	}

	var updateReq UpdateConfigRequest
	if err := ctx.Bind(&updateReq); err != nil {
		return HTTPBadRequest("Failed to decode request body")
	}

	if len(updateReq.Fields) == 0 {
		return HTTPBadRequest("At least one field must be provided")
	}

	var stubConfig types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to decode stub config")
	}

	updatedFields := make([]string, 0, len(updateReq.Fields))
	for fieldPath, value := range updateReq.Fields {
		if fieldPath == "" {
			return HTTPBadRequest("Field path cannot be empty")
		}

		if err := g.updateConfigField(&stubConfig, fieldPath, value); err != nil {
			return HTTPBadRequest(fmt.Sprintf("Failed to update field '%s': %v", fieldPath, err))
		}
		updatedFields = append(updatedFields, fieldPath)
	}

	valid, errorMsg := types.ValidateCpuAndMemory(stubConfig.Runtime.Cpu, stubConfig.Runtime.Memory, g.config.GatewayService.StubLimits)
	if !valid {
		return HTTPBadRequest(errorMsg)
	}

	if err := g.backendRepo.UpdateStubConfig(ctx.Request().Context(), stub.Id, &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to update stub config")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"message":        fmt.Sprintf("Stub config updated successfully. Updated fields: %v", updatedFields),
		"updated_fields": updatedFields,
	})
}

func (g *StubGroup) updateConfigField(config *types.StubConfigV1, fieldPath string, value interface{}) error {
	fields := strings.Split(fieldPath, ".")
	if len(fields) == 0 {
		return fmt.Errorf("empty field path")
	}

	current := reflect.ValueOf(config).Elem()

	for i, field := range fields {
		if field == "" {
			return fmt.Errorf("empty field name at position %d", i)
		}

		fieldValue, found := common.FindField(current, field)
		if !found {
			return fmt.Errorf("field '%s' not found at path '%s'", field, strings.Join(fields[:i+1], "."))
		}

		if i == len(fields)-1 {
			convertedValue, err := common.ConvertValue(fieldValue.Type(), value)
			if err != nil {
				return fmt.Errorf("failed to convert value for field '%s': %v", field, err)
			}

			fieldValue.Set(convertedValue)
			return nil
		}

		if fieldValue.Kind() == reflect.Ptr {
			if fieldValue.IsNil() {
				newValue := reflect.New(fieldValue.Type().Elem())
				fieldValue.Set(newValue)
			}
			current = fieldValue.Elem()
		} else {
			current = fieldValue
		}
	}

	return nil
}

// Sandbox status values surfaced to the dashboard. The live states reuse the
// canonical container status enum (types.ContainerStatus) so the dashboard
// filters line up with actual container states; the terminal states cover
// sandboxes that no longer have an active container.
const (
	SandboxStatusPending  = string(types.ContainerStatusPending)  // "PENDING"
	SandboxStatusRunning  = string(types.ContainerStatusRunning)  // "RUNNING"
	SandboxStatusStopping = string(types.ContainerStatusStopping) // "STOPPING"
	SandboxStatusStopped  = "STOPPED"
	SandboxStatusFailed   = "FAILED"
)

type SandboxRow struct {
	Id              string    `json:"id"`
	Name            string    `json:"name"`
	CreatedAt       time.Time `json:"created_at"`
	Status          string    `json:"status"`
	Gpu             string    `json:"gpu,omitempty"`
	ContainerId     string    `json:"container_id,omitempty"`
	TimeToStartedMs *int64    `json:"time_to_started_ms,omitempty"`
	LifetimeMs      *int64    `json:"lifetime_ms,omitempty"`
}

type SandboxListResponse struct {
	Data []SandboxRow `json:"data"`
	Next string       `json:"next"`
}

type SandboxCreatedBucket struct {
	Timestamp time.Time `json:"timestamp"`
	Count     int       `json:"count"`
}

type SandboxStatsResponse struct {
	Concurrent     int                    `json:"concurrent"`
	TotalCreated   int                    `json:"total_created"`
	RatePerSecond  float64                `json:"rate_per_second"`
	StatusCounts   map[string]int         `json:"status_counts"`
	CreatedBuckets []SandboxCreatedBucket `json:"created_buckets"`
}

// ListSandboxes returns a cursor-paginated list of sandboxes enriched with live
// status, GPU, time-to-started, and lifetime. It is scoped to the app when an
// app_id is provided, otherwise it lists all sandboxes in the workspace.
func (g *StubGroup) ListSandboxes(ctx echo.Context) error {
	workspaceID := ctx.Param("workspaceId")

	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspaceID
	filters.StubTypes = types.StringSlice{types.StubTypeSandbox}
	filters.Pagination = true

	page, err := g.backendRepo.ListStubsPaginated(ctx.Request().Context(), filters)
	if err != nil {
		return HTTPInternalServerError("Failed to list sandboxes")
	}

	containersByStub := g.activeContainersByStub(workspaceID)

	rows := make([]SandboxRow, 0, len(page.Data))
	for i := range page.Data {
		rows = append(rows, g.buildSandboxRow(ctx.Request().Context(), workspaceID, &page.Data[i], containersByStub[page.Data[i].ExternalId]))
	}

	return ctx.JSON(http.StatusOK, SandboxListResponse{Data: rows, Next: page.Next})
}

// GetSandboxStats returns aggregate stats for the sandboxes dashboard. The
// status counts and concurrency are computed from live container state; the
// totals and creation histogram come from the sandbox stubs themselves.
func (g *StubGroup) GetSandboxStats(ctx echo.Context) error {
	workspaceID := ctx.Param("workspaceId")

	stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), types.StubFilter{
		WorkspaceID: workspaceID,
		StubTypes:   types.StringSlice{types.StubTypeSandbox},
		AppId:       ctx.QueryParam("app_id"),
	})
	if err != nil {
		return HTTPInternalServerError("Failed to list sandboxes")
	}

	containersByStub := g.activeContainersByStub(workspaceID)

	statusCounts := map[string]int{
		SandboxStatusRunning:  0,
		SandboxStatusPending:  0,
		SandboxStatusStopping: 0,
		SandboxStatusStopped:  0,
		SandboxStatusFailed:   0,
	}

	concurrent := 0
	var earliest, latest time.Time
	for i := range stubs {
		stub := &stubs[i]
		createdAt := stub.CreatedAt.Time
		if earliest.IsZero() || createdAt.Before(earliest) {
			earliest = createdAt
		}
		if latest.IsZero() || createdAt.After(latest) {
			latest = createdAt
		}

		// Live states come straight from the container status enum. Sandboxes
		// without an active container are grouped as stopped here; the list
		// endpoint refines a subset of those into failed per-row.
		status := liveSandboxStatus(containersByStub[stub.ExternalId])
		if status == "" {
			status = SandboxStatusStopped
		} else {
			concurrent++
		}
		statusCounts[status]++
	}

	total := len(stubs)
	ratePerSecond := 0.0
	if total > 1 && !earliest.IsZero() && latest.After(earliest) {
		ratePerSecond = float64(total) / latest.Sub(earliest).Seconds()
	}

	return ctx.JSON(http.StatusOK, SandboxStatsResponse{
		Concurrent:     concurrent,
		TotalCreated:   total,
		RatePerSecond:  ratePerSecond,
		StatusCounts:   statusCounts,
		CreatedBuckets: buildCreatedBuckets(stubs),
	})
}

func (g *StubGroup) activeContainersByStub(workspaceID string) map[string][]types.ContainerState {
	byStub := map[string][]types.ContainerState{}
	if g.containerRepo == nil {
		return byStub
	}

	containers, err := g.containerRepo.GetActiveContainersByWorkspaceId(workspaceID)
	if err != nil {
		return byStub
	}

	for _, container := range containers {
		if !strings.HasPrefix(container.ContainerId, types.StubTypeSandbox+"-") {
			continue
		}
		byStub[container.StubId] = append(byStub[container.StubId], container)
	}

	return byStub
}

func (g *StubGroup) buildSandboxRow(ctx context.Context, workspaceID string, stub *types.StubWithRelated, containers []types.ContainerState) SandboxRow {
	row := SandboxRow{
		Id:        stub.ExternalId,
		Name:      stub.Name,
		CreatedAt: stub.CreatedAt.Time,
	}

	if active := mostRelevantContainer(containers); active != nil {
		row.ContainerId = active.ContainerId
		row.Gpu = active.Gpu
		row.Status = string(active.Status)

		if active.StartedAt > 0 && active.ScheduledAt > 0 && active.StartedAt >= active.ScheduledAt {
			tts := (active.StartedAt - active.ScheduledAt) * 1000
			row.TimeToStartedMs = &tts
		}

		if active.Status == types.ContainerStatusRunning && active.StartedAt > 0 {
			lifetime := (time.Now().Unix() - active.StartedAt) * 1000
			if lifetime >= 0 {
				row.LifetimeMs = &lifetime
			}
		}

		return row
	}

	status, timeToStarted, containerID := g.deriveTerminalSandbox(ctx, workspaceID, stub.ExternalId)
	row.Status = status
	row.TimeToStartedMs = timeToStarted
	row.ContainerId = containerID
	return row
}

// deriveTerminalSandbox performs a best-effort lookup against the event store to
// classify a sandbox that no longer has an active container. It never fails the
// request: on any error it falls back to TERMINATED.
func (g *StubGroup) deriveTerminalSandbox(ctx context.Context, workspaceID, stubID string) (string, *int64, string) {
	if g.eventRepo == nil {
		return SandboxStatusStopped, nil, ""
	}

	history, err := g.eventRepo.GetEventHistory(ctx, types.EventQuery{
		WorkspaceID: workspaceID,
		StubID:      stubID,
		Limit:       200,
		EventTypes:  []string{types.EventContainerLifecycle, types.EventContainerEvent},
	})
	if err != nil || history == nil || len(history.Events) == 0 {
		return SandboxStatusStopped, nil, ""
	}

	containerID := ""
	for i := len(history.Events) - 1; i >= 0; i-- {
		if history.Events[i].ContainerID != "" {
			containerID = history.Events[i].ContainerID
			break
		}
	}
	if containerID == "" {
		return SandboxStatusStopped, nil, ""
	}

	resp, err := g.eventRepo.GetContainerEvents(ctx, containerID, types.EventQuery{
		WorkspaceID: workspaceID,
		StubID:      stubID,
	})
	if err != nil || resp == nil {
		return SandboxStatusStopped, nil, containerID
	}

	var timeToStarted *int64
	if v, ok := resp.Summary["container_request_to_running_ms"]; ok && v > 0 {
		value := v
		timeToStarted = &value
	}

	return terminalStatusFromContainerEvents(resp), timeToStarted, containerID
}

func terminalStatusFromContainerEvents(resp *types.ContainerEventsResponse) string {
	signal := strings.ToLower(strings.Join([]string{resp.RootCauseEvent, resp.StopReason, resp.Status}, " "))
	if strings.Contains(signal, "oom") || strings.Contains(signal, "fail") || strings.Contains(signal, "error") {
		return SandboxStatusFailed
	}
	return SandboxStatusStopped
}

// liveSandboxStatus returns the canonical container status string for the
// sandbox's active container, or "" when no active container exists.
func liveSandboxStatus(containers []types.ContainerState) string {
	active := mostRelevantContainer(containers)
	if active == nil {
		return ""
	}
	return string(active.Status)
}

// mostRelevantContainer picks the container that best represents the sandbox's
// current state, preferring running over pending over stopping.
func mostRelevantContainer(containers []types.ContainerState) *types.ContainerState {
	rank := func(status types.ContainerStatus) int {
		switch status {
		case types.ContainerStatusRunning:
			return 3
		case types.ContainerStatusPending:
			return 2
		case types.ContainerStatusStopping:
			return 1
		default:
			return 0
		}
	}

	var best *types.ContainerState
	for i := range containers {
		if best == nil || rank(containers[i].Status) > rank(best.Status) {
			best = &containers[i]
		}
	}
	return best
}

// buildCreatedBuckets groups sandbox creation timestamps into evenly spaced
// buckets spanning the observed time range, suitable for the "Sandboxes created"
// chart.
func buildCreatedBuckets(stubs []types.StubWithRelated) []SandboxCreatedBucket {
	if len(stubs) == 0 {
		return []SandboxCreatedBucket{}
	}

	times := make([]time.Time, 0, len(stubs))
	var earliest, latest time.Time
	for i := range stubs {
		t := stubs[i].CreatedAt.Time
		if t.IsZero() {
			continue
		}
		times = append(times, t)
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
		if latest.IsZero() || t.After(latest) {
			latest = t
		}
	}

	if len(times) == 0 {
		return []SandboxCreatedBucket{}
	}

	const bucketCount = 48
	span := latest.Sub(earliest)
	if span <= 0 {
		return []SandboxCreatedBucket{{Timestamp: earliest, Count: len(times)}}
	}

	bucketSize := span / time.Duration(bucketCount)
	if bucketSize <= 0 {
		bucketSize = time.Minute
	}

	buckets := make([]SandboxCreatedBucket, bucketCount)
	for i := range buckets {
		buckets[i] = SandboxCreatedBucket{Timestamp: earliest.Add(time.Duration(i) * bucketSize)}
	}

	for _, t := range times {
		idx := int(t.Sub(earliest) / bucketSize)
		if idx < 0 {
			idx = 0
		}
		if idx >= bucketCount {
			idx = bucketCount - 1
		}
		buckets[idx].Count++
	}

	sort.SliceStable(buckets, func(i, j int) bool { return buckets[i].Timestamp.Before(buckets[j].Timestamp) })
	return buckets
}
