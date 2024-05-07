package apiv1

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/storage"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type MachineGroup struct {
	providerRepo repository.ProviderRepository
	tailscale    *network.Tailscale
	routerGroup  *echo.Group
	config       types.AppConfig
}

func NewMachineGroup(g *echo.Group, providerRepo repository.ProviderRepository, tailscale *network.Tailscale, config types.AppConfig) *MachineGroup {
	group := &MachineGroup{routerGroup: g,
		providerRepo: providerRepo,
		tailscale:    tailscale,
		config:       config,
	}

	g.POST("/register", group.RegisterMachine)
	return group
}

type RegisterMachineRequest struct {
	Token        string `json:"token"`
	MachineID    string `json:"machine_id"`
	ProviderName string `json:"provider_name"`
	PoolName     string `json:"pool_name"`
}

func (g *MachineGroup) RegisterMachine(ctx echo.Context) error {
	_, _ = ctx.(*auth.HttpAuthContext)

	var request RegisterMachineRequest
	if err := ctx.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid payload")
	}

	// Overwrite certain config fields with tailscale hostnames
	// TODO: figure out a more elegant to override these fields without hardcoding service names
	// possibly, use proxy config values
	remoteConfig := g.config

	redisHostname, err := g.tailscale.GetHostnameForService("redis")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Unable to lookup service: redis")
	}

	if g.config.Storage.Mode == storage.StorageModeJuiceFS {
		juiceFsRedisHostname, err := g.tailscale.GetHostnameForService("juicefs-redis")
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Unable to lookup service: juicefs-redis")
		}
		remoteConfig.Storage.JuiceFS.RedisURI = fmt.Sprintf("redis://%s/0", juiceFsRedisHostname)
	}

	gatewayGrpcHostname, err := g.tailscale.GetHostnameForService("gateway-grpc")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Unable to lookup service: gateway-grpc")
	}

	remoteConfig.Database.Redis.Addrs[0] = redisHostname
	remoteConfig.GatewayService.Host = strings.Split(gatewayGrpcHostname, ":")[0]

	hostName := fmt.Sprintf("%s.%s.%s", request.MachineID, g.config.Tailscale.User, g.config.Tailscale.HostName)
	err = g.providerRepo.RegisterMachine(request.ProviderName, request.PoolName, request.MachineID, &types.ProviderMachineState{
		MachineId: request.MachineID,
		Token:     request.Token,
		HostName:  hostName,
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to register machine")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": remoteConfig,
	})
}
