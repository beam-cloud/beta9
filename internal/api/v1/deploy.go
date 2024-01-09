package apiv1

import (
	"log"
	"net/http"

	"github.com/beam-cloud/beam/internal/auth"
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
	cc, _ := ctx.(*auth.HttpAuthContext)

	log.Println("Token value: ", cc.AuthInfo.Token.ExternalId)
	log.Println("Workspace name: ", cc.AuthInfo.Workspace.Name)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}
