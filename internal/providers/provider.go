package providers

import (
	"context"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
)

type Provider interface {
	ListMachines(ctx context.Context, poolName string) ([]string, error)
	ProvisionMachine(ctx context.Context, poolName string, compute types.ProviderComputeRequest) (string, error)
	TerminateMachine(ctx context.Context, poolName, machineId string) error
	Reconcile(ctx context.Context, poolName string)
}

func MachineId() string {
	return uuid.New().String()[:8]
}
