package apiv1

import (
	"net/http"

	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type WorkspaceGroup struct {
	routerGroup          *echo.Group
	config               types.AppConfig
	backendRepo          repository.BackendRepository
	workspaceRepo        repository.WorkspaceRepository
	defaultStorageClient *clients.StorageClient
}

func NewWorkspaceGroup(g *echo.Group, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, defaultStorageClient *clients.StorageClient, config types.AppConfig) *WorkspaceGroup {
	group := &WorkspaceGroup{routerGroup: g,
		backendRepo:          backendRepo,
		workspaceRepo:        workspaceRepo,
		config:               config,
		defaultStorageClient: defaultStorageClient,
	}

	g.POST("", group.CreateWorkspace)
	g.GET("/current", auth.WithAuth(group.CurrentWorkspace))
	g.GET("/:workspaceId/export", auth.WithWorkspaceAuth(group.ExportWorkspaceConfig))
	g.POST("/:workspaceId/set-external-storage", auth.WithWorkspaceAuth(group.SetExternalWorkspaceStorage))
	g.POST("/:workspaceId/create-storage", auth.WithWorkspaceAuth(group.CreateWorkspaceStorage))

	return group
}

type CreateWorkspaceRequest struct {
}

func (g *WorkspaceGroup) CreateWorkspace(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPUnauthorized("Invalid token")
	}

	var request CreateWorkspaceRequest
	if err := ctx.Bind(&request); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	workspace, err := g.backendRepo.CreateWorkspace(ctx.Request().Context())
	if err != nil {
		return HTTPInternalServerError("Unable to create workspace")
	}

	bucketName := types.WorkspaceBucketName(workspace.ExternalId)
	_, err = g.setupDefaultWorkspaceStorage(ctx, bucketName, workspace.Id)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to setup workspace storage for workspace %d", workspace.Id)
		return HTTPInternalServerError("Unable to setup workspace storage")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"workspace_id": workspace.ExternalId,
	})
}

func (g *WorkspaceGroup) CurrentWorkspace(ctx echo.Context) error {
	authContext, _ := ctx.(*auth.HttpAuthContext)

	serializedWorkspace, err := serializer.Serialize(authContext.AuthInfo.Workspace)
	if err != nil {
		return HTTPInternalServerError("Unable to serialize workspace")
	}

	return ctx.JSON(http.StatusOK, serializedWorkspace)
}

type WorkspaceConfigExport struct {
	GatewayHTTPHost string `json:"gateway_http_host"`
	GatewayHTTPPort int    `json:"gateway_http_port"`
	GatewayHTTPTLS  bool   `json:"gateway_http_tls"`
	GatewayGRPCHost string `json:"gateway_grpc_host"`
	GatewayGRPCPort int    `json:"gateway_grpc_port"`
	GatewayGRPCTLS  bool   `json:"gateway_grpc_tls"`
	WorkspaceID     string `json:"workspace_id"`
	Token           string `json:"token"`
}

func (g *WorkspaceGroup) ExportWorkspaceConfig(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	config := WorkspaceConfigExport{
		GatewayHTTPHost: g.config.GatewayService.HTTP.ExternalHost,
		GatewayHTTPPort: g.config.GatewayService.HTTP.ExternalPort,
		GatewayHTTPTLS:  g.config.GatewayService.HTTP.TLS,
		GatewayGRPCHost: g.config.GatewayService.GRPC.ExternalHost,
		GatewayGRPCPort: g.config.GatewayService.GRPC.ExternalPort,
		GatewayGRPCTLS:  g.config.GatewayService.GRPC.TLS,
		WorkspaceID:     workspaceId,
	}

	return ctx.JSON(http.StatusOK, config)
}

type CreateWorkspaceStorageRequest struct {
	BucketName  string `json:"bucket_name" validate:"required"`
	AccessKey   string `json:"access_key" validate:"required"`
	SecretKey   string `json:"secret_key" validate:"required"`
	EndpointUrl string `json:"endpoint_url" validate:"required"`
	Region      string `json:"region" validate:"required"`
}

// SetExternalWorkspaceStorage takes in the details for accessing an external s3 compatible storage bucket.
// This includes:
// - Bucket name
// - Access key
// - Secret key
// - Endpoint URL
// - Region
// It then creates a new workspace storage object for that bucket and sets it as the storage bucket for the workspace.
func (g *WorkspaceGroup) SetExternalWorkspaceStorage(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")

	workspace, err := g.validateWorkspaceForStorageCreation(ctx, workspaceId)
	if err != nil {
		return err
	}

	var request CreateWorkspaceStorageRequest
	if err := ctx.Bind(&request); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	v := validator.New()
	if err := v.Struct(request); err != nil {
		var missingFields []string
		for _, err := range err.(validator.ValidationErrors) {
			missingFields = append(missingFields, err.Field())
		}

		return HTTPBadRequest("Missing required fields: " + strings.Join(missingFields, ", "))
	}

	storage := &types.WorkspaceStorage{
		BucketName:  &request.BucketName,
		AccessKey:   &request.AccessKey,
		SecretKey:   &request.SecretKey,
		EndpointUrl: &request.EndpointUrl,
		Region:      &request.Region,
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx.Request().Context(), workspace.Name, storage)
	if err != nil {
		return HTTPInternalServerError("Unable to create workspace storage")
	}

	err = storageClient.ValidateBucketAccess(ctx.Request().Context())
	if err != nil {
		return HTTPInternalServerError("Unable to access bucket: " + err.Error())
	}

	createdStorage, err := g.backendRepo.CreateWorkspaceStorage(ctx.Request().Context(), workspace.Id, *storage)
	if err != nil {
		return HTTPInternalServerError("Unable to create workspace storage")
	}

	if err := g.revokeTokenIfPresent(ctx); err != nil {
		return HTTPInternalServerError("Unable to revoke token cache")
	}

	return ctx.JSON(http.StatusCreated, createdStorage)
}

// CreateWorkspaceStorage creates a new bucket in the configured default storage provider.
// It then creates a new workspace storage object for that bucket and sets it as the storage bucket for the workspace.
func (g *WorkspaceGroup) CreateWorkspaceStorage(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.validateWorkspaceForStorageCreation(ctx, workspaceId)
	if err != nil {
		return err
	}

	bucketName := types.WorkspaceBucketName(workspace.ExternalId)

	createdStorage, err := g.setupDefaultWorkspaceStorage(ctx, bucketName, workspace.Id)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to create workspace storage for workspace %d", workspace.Id)
		return HTTPInternalServerError("Unable to create workspace storage")
	}

	if err := g.revokeTokenIfPresent(ctx); err != nil {
		return HTTPInternalServerError("Unable to revoke token cache")
	}

	return ctx.JSON(http.StatusCreated, createdStorage)
}

// setupDefaultWorkspaceStorage creates a new bucket in the configured default storage provider.
func (g *WorkspaceGroup) setupDefaultWorkspaceStorage(ctx echo.Context, bucketName string, workspaceId uint) (*types.WorkspaceStorage, error) {
	err := g.defaultStorageClient.CreateBucket(ctx.Request().Context(), bucketName)
	if err != nil {
		return nil, err
	}

	err = g.defaultStorageClient.ValidateBucketAccess(ctx.Request().Context(), bucketName)
	if err != nil {
		return nil, err
	}

	// Register the newly created bucket for this workspace in the database
	createdStorage, err := g.backendRepo.CreateWorkspaceStorage(ctx.Request().Context(), workspaceId, types.WorkspaceStorage{
		BucketName:  &bucketName,
		AccessKey:   &g.config.Storage.WorkspaceStorage.DefaultAccessKey,
		SecretKey:   &g.config.Storage.WorkspaceStorage.DefaultSecretKey,
		EndpointUrl: &g.config.Storage.WorkspaceStorage.DefaultEndpointUrl,
		Region:      &g.config.Storage.WorkspaceStorage.DefaultRegion,
	})
	return createdStorage, err
}

// revokeTokenIfPresent revokes the token found in the Authorization header, if present.
func (g *WorkspaceGroup) revokeTokenIfPresent(ctx echo.Context) error {
	authHeader := ctx.Request().Header.Get("Authorization")
	tokenKey := strings.TrimPrefix(authHeader, "Bearer ")
	if tokenKey != "" {
		err := g.workspaceRepo.RevokeToken(tokenKey)
		if err != nil {
			ctx.Logger().Errorf("Failed to revoke token %s after storage update: %v", tokenKey, err)
			return err
		}
	}
	return nil
}

// validateWorkspaceForStorageCreation checks if the request is authorized for the given workspace ID
// and if storage can be created for this workspace.
func (g *WorkspaceGroup) validateWorkspaceForStorageCreation(ctx echo.Context, workspaceId string) (*types.Workspace, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspace := cc.AuthInfo.Workspace
	if workspace.ExternalId != workspaceId {
		return nil, HTTPUnauthorized("Invalid token for workspace")
	}

	if workspace.StorageAvailable() {
		return nil, HTTPBadRequest("Workspace storage already exists")
	}

	return workspace, nil
}
