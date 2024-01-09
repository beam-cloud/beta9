package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/labstack/echo/v4"
)

type DeployGroup struct {
	redisClient *common.RedisClient
	routerGroup *echo.Group
}

func NewDeployGroup(g *echo.Group, rdb *common.RedisClient) *HealthGroup {
	group := &HealthGroup{routerGroup: g, redisClient: rdb}
	g.GET("/", group.ListDeploys)
	return group
}

func (h *HealthGroup) ListDeploys(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}
