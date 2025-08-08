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
	"strings"

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
	g.PATCH("/:workspaceId/:stubId/config", auth.WithWorkspaceAuth(group.UpdateConfig))    // Allows workspace admins to update the config of a stub
	g.POST("/:stubId/clone", auth.WithAuth(group.CloneStubPublic))                         // Allows users to clone a public stub
	g.GET("/:stubId/url", auth.WithAuth(group.GetURL))                                     // Allows users to get the URL of a stub
	g.GET("/:stubId/config", group.GetConfig)                                              // Allows users to get the config of a stub

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

	// Parse the request body
	var updateReq UpdateConfigRequest
	if err := ctx.Bind(&updateReq); err != nil {
		return HTTPBadRequest("Failed to decode request body")
	}

	if len(updateReq.Fields) == 0 {
		return HTTPBadRequest("At least one field must be provided")
	}

	// Parse the current config
	var stubConfig types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to decode stub config")
	}

	// Update each specified field
	updatedFields := make([]string, 0, len(updateReq.Fields))
	for fieldPath, value := range updateReq.Fields {
		if fieldPath == "" {
			return HTTPBadRequest("Field path cannot be empty")
		}

		// Update the specified field using reflection
		if err := g.updateConfigField(&stubConfig, fieldPath, value); err != nil {
			return HTTPBadRequest(fmt.Sprintf("Failed to update field '%s': %v", fieldPath, err))
		}
		updatedFields = append(updatedFields, fieldPath)
	}

	// Update the stub config in the database
	if err := g.backendRepo.UpdateStubConfig(ctx.Request().Context(), stub.Id, &stubConfig); err != nil {
		return HTTPInternalServerError("Failed to update stub config")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"message":        fmt.Sprintf("Stub config updated successfully. Updated fields: %v", updatedFields),
		"updated_fields": updatedFields,
	})
}

func (g *StubGroup) updateConfigField(config *types.StubConfigV1, fieldPath string, value interface{}) error {
	// Split the field path by dots to handle nested fields
	fields := strings.Split(fieldPath, ".")
	if len(fields) == 0 {
		return fmt.Errorf("empty field path")
	}

	// Start with the config struct
	current := reflect.ValueOf(config).Elem()

	// Navigate through the field path
	for i, field := range fields {
		if field == "" {
			return fmt.Errorf("empty field name at position %d", i)
		}

		// Find the field by JSON tag or field name
		fieldValue, found := g.findField(current, field)
		if !found {
			return fmt.Errorf("field '%s' not found at path '%s'", field, strings.Join(fields[:i+1], "."))
		}

		// If this is the last field, set the value
		if i == len(fields)-1 {
			// Convert the value to the correct type
			convertedValue, err := g.convertValue(fieldValue.Type(), value)
			if err != nil {
				return fmt.Errorf("failed to convert value for field '%s': %v", field, err)
			}

			fieldValue.Set(convertedValue)
			return nil
		}

		// If not the last field, continue navigating
		if fieldValue.Kind() == reflect.Ptr {
			if fieldValue.IsNil() {
				// Create a new instance if the pointer is nil
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

// findField finds a field by JSON tag in a struct
func (g *StubGroup) findField(v reflect.Value, fieldName string) (reflect.Value, bool) {
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}

	// Find by JSON tag
	for i := 0; i < v.NumField(); i++ {
		fieldType := v.Type().Field(i)
		jsonTag := fieldType.Tag.Get("json")

		// Remove any options after comma
		if commaIndex := strings.Index(jsonTag, ","); commaIndex != -1 {
			jsonTag = jsonTag[:commaIndex]
		}

		if jsonTag == fieldName {
			return v.Field(i), true
		}
	}

	return reflect.Value{}, false
}

func (g *StubGroup) convertValue(targetType reflect.Type, value interface{}) (reflect.Value, error) {
	valueType := reflect.TypeOf(value)

	if valueType == targetType {
		return reflect.ValueOf(value), nil
	}

	// Handle nil values
	if value == nil {
		return reflect.Zero(targetType), nil
	}

	// Handle GpuType specifically
	if targetType == reflect.TypeOf(types.GpuType("")) {
		switch v := value.(type) {
		case string:
			return reflect.ValueOf(types.GpuType(v)), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to GpuType", value, value)
		}
	}

	switch targetType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case float64:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case int:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case int64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case string:
			// Try to parse as integer
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(i).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case float64:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative float %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case int:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative int %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case int64:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative int64 %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case string:
			// Try to parse as unsigned integer
			var u uint64
			if _, err := fmt.Sscanf(v, "%d", &u); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(u).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case int:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case int64:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case float64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case string:
			// Try to parse as float
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(f).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			return reflect.ValueOf(v), nil
		case int, int64, uint, uint64, float64, bool:
			return reflect.ValueOf(fmt.Sprintf("%v", v)), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to string", value, value)
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			return reflect.ValueOf(v), nil
		case string:
			switch strings.ToLower(v) {
			case "true", "1", "yes", "on":
				return reflect.ValueOf(true), nil
			case "false", "0", "no", "off":
				return reflect.ValueOf(false), nil
			default:
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to bool", v)
			}
		case int, int64:
			return reflect.ValueOf(v != 0), nil
		case uint, uint64:
			return reflect.ValueOf(v != 0), nil
		case float64:
			return reflect.ValueOf(v != 0), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to bool", value, value)
		}
	case reflect.Slice:
		// Handle slice conversions (e.g., []string, []uint32)
		if valueType.Kind() == reflect.Slice {
			srcSlice := reflect.ValueOf(value)
			dstSlice := reflect.MakeSlice(targetType, srcSlice.Len(), srcSlice.Len())

			for i := 0; i < srcSlice.Len(); i++ {
				converted, err := g.convertValue(targetType.Elem(), srcSlice.Index(i).Interface())
				if err != nil {
					return reflect.Value{}, fmt.Errorf("failed to convert slice element %d: %v", i, err)
				}
				dstSlice.Index(i).Set(converted)
			}
			return dstSlice, nil
		}
		return reflect.Value{}, fmt.Errorf("cannot convert %v to slice", valueType)
	case reflect.Ptr:
		// Handle pointer types
		if value == nil {
			return reflect.Zero(targetType), nil
		}
		// Dereference the pointer and convert the value
		elemValue, err := g.convertValue(targetType.Elem(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		ptrValue := reflect.New(targetType.Elem())
		ptrValue.Elem().Set(elemValue)
		return ptrValue, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported type conversion to %v", targetType)
	}
}
