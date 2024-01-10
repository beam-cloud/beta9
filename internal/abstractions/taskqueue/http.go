package taskqueue

import (
	"net/http"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/labstack/echo/v4"
)

type taskQueueGroup struct {
	routerGroup *echo.Group
	tq          *RedisTaskQueue
}

func registerTaskQueueRoutes(g *echo.Group, tq *RedisTaskQueue) *taskQueueGroup {
	group := &taskQueueGroup{routerGroup: g, tq: tq}
	g.POST("/:stubId", group.TaskQueuePut)
	return group
}

func (g *taskQueueGroup) TaskQueuePut(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	var payload TaskPayload

	if err := ctx.Bind(&payload); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request payload",
		})
	}

	taskId, err := g.tq.put(ctx.Request().Context(), cc.AuthInfo, stubId, &payload)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"task_id": taskId,
	})
}
