package apiv1

import (
	"github.com/beam-cloud/beam/internal/common"
	"github.com/gin-gonic/gin"
)

type TaskGroup struct {
	redisClient *common.RedisClient
	routerGroup *gin.RouterGroup
}

func NewTaskGroup(rg *gin.RouterGroup, rdb *common.RedisClient) *TaskGroup {
	group := &TaskGroup{routerGroup: rg, redisClient: rdb}

	rg.GET("", group.ListTasks)

	return group
}

func (g *TaskGroup) ListTasks(ctx *gin.Context) {

}
