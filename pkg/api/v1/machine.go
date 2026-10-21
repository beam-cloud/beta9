package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/labstack/echo/v4"
)

type MachineGroup struct {
	workerRepo repository.WorkerRepository
}

func NewMachineGroup(g *echo.Group, workerRepo repository.WorkerRepository) *MachineGroup {
	group := &MachineGroup{workerRepo: workerRepo}
	g.GET("/:workspaceId/gpus", auth.WithWorkspaceAuth(group.GPUCounts))
	return group
}

func (g *MachineGroup) GPUCounts(ctx echo.Context) error {
	gpuCounts, err := g.workerRepo.GetGpuCounts()
	if err != nil {
		return HTTPInternalServerError("Unable to list GPUs")
	}

	return ctx.JSON(http.StatusOK, gpuCounts)
}
