package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type HealthGroup struct {
	redisClient *common.RedisClient
	backendRepo repository.BackendRepository
	routerGroup *echo.Group
	ready       func() bool
}

func NewHealthGroup(g *echo.Group, rdb *common.RedisClient, backendRepo repository.BackendRepository, ready func() bool) *HealthGroup {
	group := &HealthGroup{routerGroup: g, redisClient: rdb, backendRepo: backendRepo, ready: ready}

	g.GET("", group.HealthCheck)
	g.GET("/live", group.Live)

	return group
}

func (h *HealthGroup) Live(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (h *HealthGroup) HealthCheck(c echo.Context) error {
	if h.ready != nil && !h.ready() {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"status": "draining",
		})
	}

	g, ctx := errgroup.WithContext(c.Request().Context())

	g.Go(func() error {
		return h.redisClient.Ping(ctx).Err()
	})

	g.Go(func() error {
		return h.backendRepo.Ping()
	})

	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("health check failed")
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"status": "not ok",
			"error":  err.Error(),
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}
