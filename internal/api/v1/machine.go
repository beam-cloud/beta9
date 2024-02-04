package apiv1

import (
	"fmt"
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type MachineGroup struct {
	providerRepo repository.ProviderRepository
	routerGroup  *echo.Group
	config       types.AppConfig
}

func NewMachineGroup(g *echo.Group, providerRepo repository.ProviderRepository, config types.AppConfig) *MachineGroup {
	group := &MachineGroup{routerGroup: g, providerRepo: providerRepo, config: config}
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

	hostName := fmt.Sprintf("%s.%s.%s", request.MachineID, g.config.Tailscale.User, g.config.Tailscale.HostName)
	err := g.providerRepo.RegisterMachine(request.ProviderName, request.PoolName, request.MachineID, &types.ProviderMachineState{
		MachineId: request.MachineID,
		Token:     request.Token,
		HostName:  hostName,
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to register machine")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": g.config,
	})
}
