package apiv1

import (
	"context"
	"log"
	"net/http"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/labstack/echo/v4"
)

type HealthGroup struct {
	redisClient *common.RedisClient
	routerGroup *echo.Group
}

func NewHealthGroup(g *echo.Group, rdb *common.RedisClient) *HealthGroup {
	group := &HealthGroup{routerGroup: g, redisClient: rdb}

	g.GET("", group.HealthCheck)
	g.GET("/", group.HealthCheck)

	return group
}

func (h *HealthGroup) HealthCheck(ctx echo.Context) error {
	err := h.redisClient.Ping(context.Background()).Err()

	if err != nil {
		log.Printf("health check failed: %v\n", err)
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status": "not ok",
			"error":  err.Error(),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}
