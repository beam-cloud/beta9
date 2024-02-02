package providers

import (
	"context"

	"github.com/google/uuid"
)

type ComputeRequest struct {
	Cpu    int64
	Memory int64
	Gpu    string
}

type Provider interface {
	ListMachines(ctx context.Context, poolName string) ([]string, error)
	ProvisionMachine(ctx context.Context, poolName string, compute ComputeRequest) (string, error)
	TerminateMachine(ctx context.Context, poolName, machineId string) error
	Reconcile(ctx context.Context, poolName string)
}

func MachineId() string {
	return uuid.New().String()[:8]
}
