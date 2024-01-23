package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/labstack/echo/v4"
)

type DeployGroup struct {
	backendRepo repository.BackendRepository
	routerGroup *echo.Group
}

func NewDeployGroup(g *echo.Group, backendRepo repository.BackendRepository) *DeployGroup {
	group := &DeployGroup{routerGroup: g, backendRepo: backendRepo}
	g.GET("/", group.ListDeploys)
	return group
}

func (g *DeployGroup) ListDeploys(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": cc.AuthInfo.Token.ExternalId,
	})
}
