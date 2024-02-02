package apiv1

import (
	"log"
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type MachineGroup struct {
	backendRepo repository.BackendRepository
	routerGroup *echo.Group
	config      types.AppConfig
}

func NewMachineGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *MachineGroup {
	group := &MachineGroup{routerGroup: g, backendRepo: backendRepo, config: config}
	g.POST("/register", group.RegisterMachine)
	return group
}

type RegisterMachineRequest struct {
	Token     string `json:"token"`
	MachineID string `json:"machine_id"`
}

func (g *MachineGroup) RegisterMachine(ctx echo.Context) error {
	_, _ = ctx.(*auth.HttpAuthContext)

	var request RegisterMachineRequest
	if err := ctx.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid payload")
	}

	log.Println("k8s service token:", request.Token)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"config": g.config,
	})
}
