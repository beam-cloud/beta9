package apiv1

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

type RegisterMachineRequest struct {
	Token        string `json:"token"`
	MachineID    string `json:"machine_id"`
	HostName     string `json:"hostname"`
	ProviderName string `json:"provider_name"`
	PoolName     string `json:"pool_name"`
	Cpu          string `json:"cpu"`
	Memory       string `json:"memory"`
	GpuCount     string `json:"gpu_count"`
	PrivateIP    string `json:"private_ip"`
}

type MachineGroup struct {
	providerRepo repository.ProviderRepository
	tailscale    *network.Tailscale
	routerGroup  *echo.Group
	config       types.AppConfig
	workerRepo   repository.WorkerRepository
}

func NewMachineGroup(g *echo.Group, providerRepo repository.ProviderRepository, tailscale *network.Tailscale, config types.AppConfig, workerRepo repository.WorkerRepository) *MachineGroup {
	group := &MachineGroup{routerGroup: g,
		providerRepo: providerRepo,
		tailscale:    tailscale,
		config:       config,
		workerRepo:   workerRepo,
	}

	g.GET("/:workspaceId/gpus", auth.WithWorkspaceAuth(group.GPUCounts))
	g.POST("/register", group.RegisterMachine)
	g.GET("/remote-config", group.GetRemoteConfig)
	g.GET("/list", group.ListPoolMachines)
	return group
}

func (g *MachineGroup) ListPoolMachines(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if (cc.AuthInfo.Token.TokenType != types.TokenTypeMachine) && (cc.AuthInfo.Token.TokenType != types.TokenTypeWorker) {
		return HTTPForbidden("Invalid token")
	}

	poolName := ctx.QueryParam("pool_name")
	providerName := ctx.QueryParam("provider_name")

	machines, err := g.providerRepo.ListAllMachines(providerName, poolName, false)
	if err != nil {
		return HTTPInternalServerError("Failed to get neighbors")
	}

	availableMachines := make([]*types.ProviderMachine, 0)
	for _, machine := range machines {
		if machine.State.Status != types.MachineStatusReady {
			continue
		}

		availableMachines = append(availableMachines, machine)
	}

	return ctx.JSON(http.StatusOK, availableMachines)
}

func (g *MachineGroup) GetRemoteConfig(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if (cc.AuthInfo.Token.TokenType != types.TokenTypeMachine) && (cc.AuthInfo.Token.TokenType != types.TokenTypeWorker) {
		return HTTPForbidden("Invalid token")
	}

	remoteConfig, err := providers.GetRemoteConfig(g.config, g.tailscale)
	if err != nil {
		return HTTPInternalServerError("Unable to create remote config")
	}

	return ctx.JSON(http.StatusOK, remoteConfig)
}

func (g *MachineGroup) RegisterMachine(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if (cc.AuthInfo.Token.TokenType != types.TokenTypeMachine) && (cc.AuthInfo.Token.TokenType != types.TokenTypeWorker) {
		return HTTPForbidden("Invalid token")
	}

	var request RegisterMachineRequest
	if err := ctx.Bind(&request); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	remoteConfig, err := providers.GetRemoteConfig(g.config, g.tailscale)
	if err != nil {
		return HTTPInternalServerError("Unable to create remote config")
	}

	cpu, err := scheduler.ParseCPU(request.Cpu)
	if err != nil {
		return HTTPInternalServerError("Invalid machine cpu value")
	}

	memory, err := scheduler.ParseMemory(request.Memory)
	if err != nil {
		return HTTPInternalServerError("Invalid machine memory value")
	}

	gpuCount, err := strconv.ParseUint(request.GpuCount, 10, 32)
	if err != nil {
		return HTTPInternalServerError("Invalid gpu count")
	}

	hostName := fmt.Sprintf("%s.%s", request.HostName, g.config.Tailscale.HostName)

	// If user is != "", add it into hostname (for self-managed control servers like headscale)
	if g.config.Tailscale.User != "" {
		hostName = fmt.Sprintf("%s.%s.%s", request.HostName, g.config.Tailscale.User, g.config.Tailscale.HostName)
	}

	poolConfig, ok := g.config.Worker.Pools[request.PoolName]
	if !ok {
		return HTTPInternalServerError("Invalid pool name")
	}

	err = g.providerRepo.RegisterMachine(request.ProviderName, request.PoolName, request.MachineID, &types.ProviderMachineState{
		MachineId: request.MachineID,
		Token:     request.Token,
		HostName:  hostName,
		Cpu:       cpu,
		Memory:    memory,
		GpuCount:  uint32(gpuCount),
		PrivateIP: request.PrivateIP,
	}, &poolConfig)
	if err != nil {
		return HTTPInternalServerError("Failed to register machine")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": remoteConfig,
	})
}

func (g *MachineGroup) GPUCounts(ctx echo.Context) error {
	gpuCounts, err := g.workerRepo.GetGpuCounts()
	if err != nil {
		return HTTPInternalServerError("Unable to list GPUs")
	}

	return ctx.JSON(http.StatusOK, gpuCounts)
}
