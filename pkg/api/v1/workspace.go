package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type WorkspaceGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewWorkspaceGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *WorkspaceGroup {
	group := &WorkspaceGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.POST("", group.CreateWorkspace)
	g.GET("/current", auth.WithAuth(group.CurrentWorkspace))
	g.GET("/:workspaceId/export", auth.WithWorkspaceAuth(group.ExportWorkspaceConfig))

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

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"workspace_id": workspace.ExternalId,
	})
}

func (g *WorkspaceGroup) CurrentWorkspace(ctx echo.Context) error {
	authContext, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, authContext.AuthInfo.Workspace)
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
