package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	"github.com/labstack/echo/v4"
)

type DeployGroup struct {
	redisClient *common.RedisClient
	routerGroup *echo.Group
}

func NewDeployGroup(g *echo.Group, rdb *common.RedisClient) *DeployGroup {
	group := &DeployGroup{routerGroup: g, redisClient: rdb}
	g.GET("/", group.ListDeploys)
	return group
}

func (g *DeployGroup) ListDeploys(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": cc.AuthInfo.Token.ExternalId,
	})
}
