package apiv1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
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
	Cpu          string `json:"cpu"`
	Memory       string `json:"memory"`
}

func (g *MachineGroup) RegisterMachine(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if (cc.AuthInfo.Token.TokenType != types.TokenTypeMachine) && (cc.AuthInfo.Token.TokenType != types.TokenTypeWorker) {
		return echo.NewHTTPError(http.StatusForbidden, "Invalid token")
	}

	var request RegisterMachineRequest
	if err := ctx.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid payload")
	}

	configByes, err := json.Marshal(g.config)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Unable to serialize config")
	}

	// Overwrite certain config fields with tailscale hostnames
	// TODO: figure out a more elegant to override these fields without hardcoding service names
	// possibly, use proxy config values
	remoteConfig := types.AppConfig{}
	if err = json.Unmarshal(configByes, &remoteConfig); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Unable to deserialize config")
	}

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

	cpu, err := scheduler.ParseCPU(request.Cpu)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Invalid machine CPU value")
	}

	memory, err := scheduler.ParseMemory(request.Memory)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Invalid machine memory value")
	}

	hostName := fmt.Sprintf("%s.%s.%s", request.MachineID, g.config.Tailscale.User, g.config.Tailscale.HostName)
	err = g.providerRepo.RegisterMachine(request.ProviderName, request.PoolName, request.MachineID, &types.ProviderMachineState{
		MachineId: request.MachineID,
		Token:     request.Token,
		HostName:  hostName,
		Cpu:       cpu,
		Memory:    memory,
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to register machine")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": remoteConfig,
	})
}
