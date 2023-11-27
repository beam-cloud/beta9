package apiv1

import (
	"github.com/beam-cloud/beam/internal/common"
	"github.com/gin-gonic/gin"
)

type AppGroup struct {
	redisClient *common.RedisClient
	routerGroup *gin.RouterGroup
}

func NewAppGroup(rg *gin.RouterGroup, rdb *common.RedisClient) *AppGroup {
	group := &AppGroup{routerGroup: rg, redisClient: rdb}

	rg.GET("", group.ListApps)

	return group
}

/*
Endpoints we need
  - creating an app
  - deleting an app
  - listing apps
  - retrieving an app
*/
func (g *AppGroup) ListApps(ctx *gin.Context) {

}
