package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type ConfigGroup struct {
	backendRepo repository.BackendRepository
	routerGroup *echo.Group
	config      types.AppConfig
}

func NewConfigGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *ConfigGroup {
	group := &ConfigGroup{routerGroup: g, backendRepo: backendRepo, config: config}
	g.GET("/", group.GetConfig)
	return group
}

func (g *ConfigGroup) GetConfig(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": cc.AuthInfo.Token.ExternalId,
	})
}
