package apiv1

import (
	"context"
	"log"
	"net/http"

	"github.com/beam-cloud/beam/pkg/common"
	"github.com/gin-gonic/gin"
)

type HealthGroup struct {
	redisClient *common.RedisClient
	routerGroup *gin.RouterGroup
}

func NewHealthGroup(rg *gin.RouterGroup, rdb *common.RedisClient) *HealthGroup {
	group := &HealthGroup{routerGroup: rg, redisClient: rdb}

	rg.GET("", group.HealthCheck)

	return group
}

func (h *HealthGroup) HealthCheck(ctx *gin.Context) {
	err := h.redisClient.Ping(context.Background()).Err()

	if err != nil {
		log.Printf("health check failed: %v\n", err)

		ctx.JSON(http.StatusInternalServerError, gin.H{
			"status": "not ok",
			"error":  err.Error(),
		})

		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}
