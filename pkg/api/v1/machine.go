package apiv1

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
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
	GpuCount     string `json:"gpu_count"`
}

func (g *MachineGroup) RegisterMachine(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if (cc.AuthInfo.Token.TokenType != types.TokenTypeMachine) && (cc.AuthInfo.Token.TokenType != types.TokenTypeWorker) {
		return echo.NewHTTPError(http.StatusForbidden, "Invalid token")
	}

	var request RegisterMachineRequest
	if err := ctx.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid payload")
	}

	remoteConfig, err := providers.GetRemoteConfig(g.config, g.tailscale)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "unable to create remote config")
	}

	cpu, err := scheduler.ParseCPU(request.Cpu)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid machine cpu value")
	}

	memory, err := scheduler.ParseMemory(request.Memory)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid machine memory value")
	}

	gpuCount, err := strconv.ParseUint(request.GpuCount, 10, 32)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid gpu count")
	}

	hostName := fmt.Sprintf("%s.%s", request.MachineID, g.config.Tailscale.HostName)

	// If user is != "", add it into hostname (for self-managed control servers like headscale)
	if g.config.Tailscale.User != "" {
		hostName = fmt.Sprintf("%s.%s.%s", request.MachineID, g.config.Tailscale.User, g.config.Tailscale.HostName)
	}

	err = g.providerRepo.RegisterMachine(request.ProviderName, request.PoolName, request.MachineID, &types.ProviderMachineState{
		MachineId: request.MachineID,
		Token:     request.Token,
		HostName:  hostName,
		Cpu:       cpu,
		Memory:    memory,
		GpuCount:  uint32(gpuCount),
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to register machine")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": remoteConfig,
	})
}
